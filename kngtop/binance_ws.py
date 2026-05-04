"""Binance spot BTCUSDT price via WebSocket (last trade price)."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

LOGGER = logging.getLogger("kngtop")

try:
    import websocket
except ImportError as exc:  # pragma: no cover
    websocket = None  # type: ignore[assignment]
    _IMPORT_ERR = exc
else:
    _IMPORT_ERR = None


def _stream_path(symbol_lower: str) -> str:
    return f"{symbol_lower}@trade"


class BinanceBtcWsFeed:
    """Maintains latest trade price for ``symbol`` (e.g. btcusdt)."""

    def __init__(self, symbol: str = "btcusdt") -> None:
        if websocket is None:
            raise RuntimeError(f"websocket-client required: {_IMPORT_ERR}")
        self._sym = symbol.lower().replace("/", "")
        self._url = f"wss://stream.binance.com:9443/ws/{_stream_path(self._sym)}"
        self._lock = threading.Lock()
        self._price: float = 0.0
        self._ts: float = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws_app: Any = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="binance-ws", daemon=True)
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
            self._thread.join(timeout=3.0)
            self._thread = None

    def last_price(self, max_age_sec: float = 5.0) -> float | None:
        with self._lock:
            if self._price <= 0 or time.time() - self._ts > max_age_sec:
                return None
            return float(self._price)

    def _set_px(self, p: float) -> None:
        if p <= 0:
            return
        with self._lock:
            self._price = p
            self._ts = time.time()

    def _on_message(self, _ws: Any, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        if isinstance(data, dict) and data.get("p"):
            try:
                self._set_px(float(data["p"]))
            except (TypeError, ValueError):
                return

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    self._url,
                    on_message=self._on_message,
                    on_error=lambda _w, e: LOGGER.debug("Binance WS: %s", e),
                    on_close=lambda *_a: LOGGER.debug("Binance WS closed"),
                )
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                LOGGER.warning("Binance WS session error: %s", exc)
                time.sleep(1.5)
