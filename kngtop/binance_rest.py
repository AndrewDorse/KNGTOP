"""Binance REST — candle open at window start (aligns PM slug epoch). From KNG4 ``clob_shim``."""

from __future__ import annotations

import logging

import requests

LOGGER = logging.getLogger("kngtop")

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


def _interval_for_window(window_minutes: int) -> str:
    wm = int(window_minutes)
    if wm <= 5:
        return "5m"
    if wm <= 15:
        return "15m"
    return "15m"


def fetch_binance_window_open_btc(
    *,
    symbol: str,
    window_start_sec: int,
    window_minutes: int,
    timeout: float,
) -> float | None:
    try:
        start_ms = int(window_start_sec) * 1000
        interval = _interval_for_window(window_minutes)
        r = requests.get(
            _BINANCE_KLINES,
            params={
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": start_ms,
                "limit": 1,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return None
        o = float(rows[0][1])
        return o if o > 0 else None
    except (requests.RequestException, IndexError, KeyError, ValueError, TypeError) as exc:
        LOGGER.debug("Binance kline open: %s", exc)
        return None
