"""Binance spot combined ``@trade`` stream for multiple symbols (one websocket)."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

LOGGER = logging.getLogger("kngtop")

try:
    import websocket
except ImportError as exc:  # pragma: no cover
    websocket = None  # type: ignore[assignment]
    _IMPORT_ERR = exc
else:
    _IMPORT_ERR = None


class BinanceCombinedTradeFeed:
    """Maintains latest trade price per ``BASEUSDT`` symbol on one multiplex connection."""

    def __init__(
        self,
        symbols: list[str],
        *,
        on_trade: Callable[[str], None] | None = None,
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
        self._lock = threading.Lock()
        self._px: dict[str, tuple[float, float]] = {}  # SYMBOL -> (price, wall_ts)
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

    def _bump(self, sym_u: str, price: float) -> None:
        if price <= 0:
            return
        with self._lock:
            self._px[sym_u] = (price, time.time())
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
            s_raw = payload.get("s")
            if p_raw is None or not s_raw:
                continue
            try:
                p = float(p_raw)
                sym_u = str(s_raw).strip().upper()
            except (TypeError, ValueError):
                continue
            self._bump(sym_u, p)

    def _run_loop(self) -> None:
        backoff = 1.5
        while not self._stop.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    self._url,
                    on_message=lambda _ws, msg: self._dispatch_message(msg),
                    on_error=lambda _w, e: LOGGER.warning("Binance combo WS error: %s", e),
                    on_close=lambda *_a: LOGGER.debug("Binance combo WS closed"),
                    on_open=lambda _ws: LOGGER.debug(
                        "Binance combo WS connected (%d streams)", len(self._symbols_norm)
                    ),
                )
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
                backoff = 1.5
            except Exception as exc:
                LOGGER.warning("Binance combo WS session error: %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 45.0)
