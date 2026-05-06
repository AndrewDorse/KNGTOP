"""Polymarket CLOB market WebSocket — best bid/ask per outcome token (KNG3 ``polymarket_ws``)."""

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

DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class MarketWsFeed:
    """Background thread: latest bid/ask per asset_id from the market channel."""

    def __init__(
        self,
        url: str = DEFAULT_WS_URL,
        on_quote_update: Callable[[], None] | None = None,
        on_ws_reconnect: Callable[[float], None] | None = None,
    ) -> None:
        if websocket is None:
            raise RuntimeError(
                "websocket-client is required for MarketWsFeed "
                f"(pip install websocket-client): {_IMPORT_ERR}"
            )
        self._url = url
        self._on_quote_update = on_quote_update
        self._on_ws_reconnect = on_ws_reconnect
        self._disconnect_at: float | None = None
        self._skip_next_disconnect_mark = False
        self._lock = threading.Lock()
        self._quotes: dict[str, dict[str, float]] = {}
        self._subscribed: tuple[str, ...] = ()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws_app: Any = None
        self._ping_stop = threading.Event()
        self._reconnect_sleep_sec = 1.5

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._ping_stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="poly-ws-market", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._ping_stop.set()
        self._close_ws()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None

    def set_assets(self, asset_ids: list[str]) -> None:
        t = tuple(asset_ids)
        with self._lock:
            if t == self._subscribed:
                return
            self._subscribed = t
        LOGGER.debug("WS market: asset set changed (%d ids); reconnect", len(t))
        self._close_ws(record_disconnect=False)

    def subscribed_asset_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._subscribed)

    def apply_quote(self, asset_id: str, bid: float, ask: float) -> None:
        """Seed or refresh from REST (same as a WS book event)."""
        self._set_quote(asset_id, bid, ask)

    def mid_for(self, asset_id: str, max_age_sec: float = 4.0) -> float | None:
        with self._lock:
            q = self._quotes.get(asset_id)
            if not q:
                return None
            if time.time() - q["ts"] > max_age_sec:
                return None
            return float(q["mid"])

    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 4.0) -> tuple[float, float] | None:
        with self._lock:
            q = self._quotes.get(asset_id)
            if not q:
                return None
            if time.time() - q["ts"] > max_age_sec:
                return None
            return float(q["bid"]), float(q["ask"])

    def _close_ws(self, *, record_disconnect: bool = True) -> None:
        if not record_disconnect:
            self._skip_next_disconnect_mark = True
        app = self._ws_app
        self._ws_app = None
        if app is not None:
            try:
                app.close()
            except Exception as exc:
                LOGGER.debug("WS close: %s", exc)

    def _set_quote(self, asset_id: str, bid: float, ask: float) -> None:
        if bid <= 0 or ask <= 0:
            return
        with self._lock:
            self._quotes[asset_id] = {
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) / 2.0,
                "ts": time.time(),
            }
        cb = self._on_quote_update
        if cb is not None:
            try:
                cb()
            except Exception:  # noqa: BLE001
                LOGGER.debug("on_quote_update failed", exc_info=True)

    def _on_message(self, _ws: Any, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._handle_event(item)
        elif isinstance(data, dict):
            self._handle_event(data)

    def _handle_event(self, msg: dict[str, Any]) -> None:
        et = msg.get("event_type")
        if et == "best_bid_ask":
            aid = str(msg.get("asset_id") or "")
            bb = _to_float(msg.get("best_bid"))
            ba = _to_float(msg.get("best_ask"))
            if aid and bb > 0 and ba > 0:
                self._set_quote(aid, bb, ba)
        elif et == "book":
            aid = str(msg.get("asset_id") or "")
            bids = msg.get("bids") or []
            asks = msg.get("asks") or []
            bb = _book_best(bids, ask_side=False)
            ba = _book_best(asks, ask_side=True)
            if aid and bb > 0 and ba > 0:
                self._set_quote(aid, bb, ba)
        elif et == "price_change":
            for ch in msg.get("price_changes") or []:
                if not isinstance(ch, dict):
                    continue
                aid = str(ch.get("asset_id") or "")
                bb = _to_float(ch.get("best_bid"))
                ba = _to_float(ch.get("best_ask"))
                if aid and bb > 0 and ba > 0:
                    self._set_quote(aid, bb, ba)

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            assets = list(self._subscribed)
            if len(assets) < 1:
                time.sleep(0.3)
                self._reconnect_sleep_sec = 1.5
                continue
            self._connect_session(assets)
            time.sleep(self._reconnect_sleep_sec)
            self._reconnect_sleep_sec = min(self._reconnect_sleep_sec * 2.0, 45.0)

    def _connect_session(self, assets: list[str]) -> None:
        ready = threading.Event()

        def on_open(ws: Any) -> None:
            self._reconnect_sleep_sec = 1.5
            sub = {"assets_ids": assets, "type": "market", "custom_feature_enabled": True}
            ws.send(json.dumps(sub))
            ready.set()
            now = time.time()
            downtime: float | None = None
            if self._disconnect_at is not None:
                downtime = now - self._disconnect_at
                self._disconnect_at = None
            LOGGER.debug("WS market: connected + subscribed")
            if downtime is not None and self._on_ws_reconnect is not None:
                try:
                    self._on_ws_reconnect(float(downtime))
                except Exception:  # noqa: BLE001
                    LOGGER.debug("on_ws_reconnect failed", exc_info=True)

        def on_close(*_a: Any) -> None:
            if self._skip_next_disconnect_mark:
                self._skip_next_disconnect_mark = False
                LOGGER.debug("WS market closed (resubscribe)")
                return
            self._disconnect_at = time.time()
            LOGGER.debug("WS market closed")

        self._ws_app = websocket.WebSocketApp(
            self._url,
            on_open=on_open,
            on_message=self._on_message,
            on_error=lambda _ws, e: LOGGER.debug("WS error: %s", e),
            on_close=on_close,
        )

        def ping_worker() -> None:
            while not self._ping_stop.is_set() and not self._stop.is_set():
                time.sleep(10.0)
                w = self._ws_app
                if w is None:
                    break
                try:
                    w.send("PING")
                except Exception:
                    break

        ping_thread = threading.Thread(target=ping_worker, name="poly-ws-ping", daemon=True)
        ping_thread.start()
        try:
            self._ws_app.run_forever(ping_interval=None)
        finally:
            self._ping_stop.set()


def _to_float(x: Any) -> float:
    try:
        if x is None or x == "":
            return 0.0
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _book_best(levels: list[Any], *, ask_side: bool) -> float:
    if not levels:
        return 0.0
    first = levels[0]
    if isinstance(first, dict):
        return _to_float(first.get("price"))
    return _to_float(getattr(first, "price", 0))
