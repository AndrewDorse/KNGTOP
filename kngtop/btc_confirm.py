"""BTC-only secondary confirmation feed using major exchange spot prices."""

from __future__ import annotations

import logging
import threading
import time
from statistics import median

import requests

LOGGER = logging.getLogger("kngtop")


class BtcConfirmFeed:
    """Background poller for slower BTC confirmation prices."""

    def __init__(self, *, interval_sec: float = 1.0, timeout_sec: float = 1.5) -> None:
        self._interval_sec = max(0.5, float(interval_sec))
        self._timeout_sec = max(0.5, float(timeout_sec))
        self._lock = threading.Lock()
        self._prices: dict[str, tuple[float, float]] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._session = requests.Session()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="btc-confirm", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None
        self._session.close()

    def median_price(self, *, binance_spot: float, max_age_sec: float = 4.0) -> float | None:
        fresh: list[float] = [float(binance_spot)]
        now = time.time()
        with self._lock:
            for px, ts in self._prices.values():
                if now - ts <= max_age_sec:
                    fresh.append(float(px))
        if len(fresh) < 3:
            return None
        return float(median(fresh))

    def side_matches(self, *, start_px: float, binance_spot: float, max_age_sec: float = 4.0) -> bool:
        px = self.median_price(binance_spot=binance_spot, max_age_sec=max_age_sec)
        if px is None:
            return True
        binance_side = 1 if binance_spot > start_px else -1 if binance_spot < start_px else 0
        median_side = 1 if px > start_px else -1 if px < start_px else 0
        return binance_side != 0 and binance_side == median_side

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            started = time.perf_counter()
            for source, fetcher in (
                ("coinbase", self._fetch_coinbase),
                ("bybit", self._fetch_bybit),
                ("okx", self._fetch_okx),
            ):
                try:
                    px = fetcher()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.debug("BTC confirm %s fetch failed: %s", source, exc)
                    continue
                if px is None or px <= 0:
                    continue
                with self._lock:
                    self._prices[source] = (float(px), time.time())
            sleep_for = self._interval_sec - (time.perf_counter() - started)
            if sleep_for > 0:
                self._stop.wait(sleep_for)

    def _fetch_coinbase(self) -> float | None:
        data = self._session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
            timeout=self._timeout_sec,
        ).json()
        return _to_float(data.get("price"))

    def _fetch_bybit(self) -> float | None:
        data = self._session.get(
            "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT",
            timeout=self._timeout_sec,
        ).json()
        result = data.get("result") or {}
        items = result.get("list") or []
        if not items:
            return None
        return _to_float((items[0] or {}).get("lastPrice"))

    def _fetch_okx(self) -> float | None:
        data = self._session.get(
            "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT",
            timeout=self._timeout_sec,
        ).json()
        items = data.get("data") or []
        if not items:
            return None
        return _to_float((items[0] or {}).get("last"))


def _to_float(raw: object) -> float | None:
    try:
        if raw is None or raw == "":
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None
