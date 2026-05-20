"""Binance spot combined ``@trade`` stream for multiple symbols (one websocket)."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger("kngtop")
TRADE_HISTORY_RETENTION_SEC = 180.0

try:
    import websocket
except ImportError as exc:  # pragma: no cover
    websocket = None  # type: ignore[assignment]
    _IMPORT_ERR = exc
else:
    _IMPORT_ERR = None


@dataclass(frozen=True, slots=True)
class TradeSample:
    wall_ts: float
    price: float
    qty: float


def _prune_trade_samples(samples: deque[TradeSample], *, now_ts: float, retention_sec: float = TRADE_HISTORY_RETENTION_SEC) -> None:
    cutoff = float(now_ts) - float(retention_sec)
    while samples and float(samples[0].wall_ts) < cutoff:
        samples.popleft()


def _price_then_now_from_samples(
    samples: deque[TradeSample],
    *,
    now_ts: float,
    lookback_sec: int,
) -> tuple[float, float] | None:
    if not samples:
        return None
    current = float(samples[-1].price)
    cutoff = float(now_ts) - float(max(0, int(lookback_sec)))
    past: float | None = None
    for sample in samples:
        sample_ts = float(sample.wall_ts)
        if sample_ts <= cutoff:
            past = float(sample.price)
            continue
        if past is None:
            past = float(sample.price)
        break
    if past is None:
        past = float(samples[0].price)
    return current, past


def _volume_ratio_from_samples(
    samples: deque[TradeSample],
    *,
    now_ts: float,
    lookback_sec: int,
) -> float:
    if not samples:
        return 0.0
    lb = max(1, int(lookback_sec))
    buckets: dict[int, float] = {}
    for sample in samples:
        sec = int(sample.wall_ts)
        buckets[sec] = buckets.get(sec, 0.0) + max(0.0, float(sample.qty))
    now_sec = int(now_ts)
    current = buckets.get(now_sec, 0.0)
    prev_total = 0.0
    for sec in range(now_sec - lb, now_sec):
        prev_total += buckets.get(sec, 0.0)
    prev_mean = prev_total / float(lb)
    if current <= 0.0 and prev_mean <= 0.0:
        return 0.0
    if prev_mean <= 0.0:
        return float("inf")
    return current / prev_mean


class BinanceCombinedTradeFeed:
    """Maintains latest trade price per ``BASEUSDT`` symbol on one multiplex connection."""

    def __init__(
        self,
        symbols: list[str],
        *,
        on_trade: Callable[[str], None] | None = None,
        on_ws_reconnect: Callable[[float], None] | None = None,
    ) -> None:
        if websocket is None:
            raise RuntimeError(f"websocket-client required: {_IMPORT_ERR}")
        self._symbols_norm = sorted(
            {s.strip().upper().replace("/", "") for s in symbols if s.strip()},
            key=lambda x: x,
        )
        if not self._symbols_norm:
            raise ValueError("BinanceCombinedTradeFeed: empty symbols")
        streams = "/".join(f"{s.lower()}@trade" for s in self._symbols_norm)
        self._url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self._on_trade = on_trade
        self._on_ws_reconnect = on_ws_reconnect
        self._disconnect_at: float | None = None
        self._lock = threading.Lock()
        self._px: dict[str, tuple[float, float]] = {}  # SYMBOL -> (price, wall_ts)
        self._trades: dict[str, deque[TradeSample]] = {}  # SYMBOL -> recent trades
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws_app: Any = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="binance-combo-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:
                pass
            self._ws_app = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(self._symbols_norm)

    def apply_trade_price(self, symbol: str, price: float) -> None:
        """Update cache (e.g. from REST fallback). Same path as WS trades."""
        sym_u = symbol.strip().upper().replace("/", "")
        self._bump(sym_u, float(price), 0.0)

    def last_price(self, symbol: str, *, max_age_sec: float = 6.0) -> float | None:
        sym = symbol.strip().upper().replace("/", "")
        with self._lock:
            row = self._px.get(sym)
            if row is None:
                return None
            p, ts = row
            if p <= 0 or time.time() - ts > max_age_sec:
                return None
            return float(p)

    def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0) -> tuple[float, float] | None:
        sym = symbol.strip().upper().replace("/", "")
        now_ts = time.time()
        with self._lock:
            row = self._px.get(sym)
            samples = self._trades.get(sym)
            if row is None or samples is None:
                return None
            if float(row[0]) <= 0 or now_ts - float(row[1]) > float(max_age_sec):
                return None
            _prune_trade_samples(samples, now_ts=now_ts)
            return _price_then_now_from_samples(samples, now_ts=now_ts, lookback_sec=lookback_sec)

    def current_volume_ratio(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0) -> float | None:
        sym = symbol.strip().upper().replace("/", "")
        now_ts = time.time()
        with self._lock:
            row = self._px.get(sym)
            samples = self._trades.get(sym)
            if row is None or samples is None:
                return None
            if float(row[0]) <= 0 or now_ts - float(row[1]) > float(max_age_sec):
                return None
            _prune_trade_samples(samples, now_ts=now_ts)
            return _volume_ratio_from_samples(samples, now_ts=now_ts, lookback_sec=lookback_sec)

    def _bump(self, sym_u: str, price: float, qty: float = 0.0) -> None:
        if price <= 0:
            return
        now_ts = time.time()
        with self._lock:
            self._px[sym_u] = (price, now_ts)
            samples = self._trades.setdefault(sym_u, deque())
            samples.append(TradeSample(wall_ts=now_ts, price=float(price), qty=max(0.0, float(qty))))
            _prune_trade_samples(samples, now_ts=now_ts)
        cb = self._on_trade
        if cb is not None:
            try:
                cb(sym_u)
            except Exception:  # noqa: BLE001
                LOGGER.debug("on_trade callback failed", exc_info=True)

    def _dispatch_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        payloads: list[dict[str, Any]] = []

        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            payloads.append(data["data"])
        elif isinstance(data, dict) and data.get("e") == "trade":
            payloads.append(data)

        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            if payload.get("e") != "trade":
                continue
            p_raw = payload.get("p")
            q_raw = payload.get("q")
            s_raw = payload.get("s")
            if p_raw is None or not s_raw:
                continue
            try:
                p = float(p_raw)
                q = float(q_raw or 0.0)
                sym_u = str(s_raw).strip().upper()
            except (TypeError, ValueError):
                continue
            self._bump(sym_u, p, q)

    def _on_ws_open(self, _ws: Any) -> None:
        now = time.time()
        downtime: float | None = None
        if self._disconnect_at is not None:
            downtime = now - self._disconnect_at
            self._disconnect_at = None
        LOGGER.debug(
            "Binance combo WS connected (%d streams)",
            len(self._symbols_norm),
        )
        if downtime is not None and self._on_ws_reconnect is not None:
            try:
                self._on_ws_reconnect(float(downtime))
            except Exception:  # noqa: BLE001
                LOGGER.debug("on_ws_reconnect failed", exc_info=True)

    def _on_ws_close(self, *_a: Any) -> None:
        self._disconnect_at = time.time()
        LOGGER.debug("Binance combo WS closed")

    def _run_loop(self) -> None:
        backoff = 1.5
        while not self._stop.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    self._url,
                    on_message=lambda _ws, msg: self._dispatch_message(msg),
                    on_error=lambda _w, e: LOGGER.warning("Binance combo WS error: %s", e),
                    on_close=self._on_ws_close,
                    on_open=self._on_ws_open,
                )
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
                backoff = 1.5
            except Exception as exc:
                LOGGER.warning("Binance combo WS session error: %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 45.0)
