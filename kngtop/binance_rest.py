"""Binance REST — candle open at window start (aligns PM slug epoch). From KNG4 ``clob_shim``."""

from __future__ import annotations

import logging

import requests

LOGGER = logging.getLogger("kngtop")

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
_BINANCE_TICKER_PRICE = "https://api.binance.com/api/v3/ticker/price"


def _interval_for_window(window_minutes: int) -> str:
    wm = int(window_minutes)
    if wm <= 5:
        return "5m"
    if wm <= 15:
        return "15m"
    if wm <= 60:
        return "1h"
    if wm <= 240:
        return "4h"
    return "4h"


def fetch_binance_window_open_px(
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


def fetch_binance_spot_price(*, symbol: str, timeout: float) -> float | None:
    """Last traded price from Binance REST (fallback when WS cache is stale)."""
    sym = symbol.strip().upper().replace("/", "")
    if not sym:
        return None
    try:
        r = requests.get(_BINANCE_TICKER_PRICE, params={"symbol": sym}, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        raw = j.get("price")
        if raw is None:
            return None
        px = float(raw)
        return px if px > 0 else None
    except (requests.RequestException, KeyError, ValueError, TypeError) as exc:
        LOGGER.debug("Binance ticker price %s: %s", sym, exc)
        return None


# Back-compat name
fetch_binance_window_open_btc = fetch_binance_window_open_px
