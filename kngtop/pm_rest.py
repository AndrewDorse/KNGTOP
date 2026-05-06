"""Polymarket CLOB public REST — best bid/ask when WS quotes go stale."""

from __future__ import annotations

import logging
from typing import Any

import requests

LOGGER = logging.getLogger("kngtop")

_CLOB_BOOK = "https://clob.polymarket.com/book"


def _level_price(level: Any) -> float:
    if isinstance(level, dict):
        raw = level.get("price")
    else:
        raw = getattr(level, "price", None)
    try:
        if raw is None or raw == "":
            return 0.0
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def fetch_clob_best_bid_ask(*, token_id: str, timeout: float) -> tuple[float, float] | None:
    """Best bid (max) and best ask (min) from the public order book."""
    tid = (token_id or "").strip()
    if not tid:
        return None
    try:
        r = requests.get(_CLOB_BOOK, params={"token_id": tid}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        bb = 0.0
        for b in bids:
            bb = max(bb, _level_price(b))
        ba = 0.0
        for a in asks:
            p = _level_price(a)
            if p > 0:
                ba = p if ba <= 0 else min(ba, p)
        if bb <= 0 or ba <= 0:
            return None
        return bb, ba
    except (requests.RequestException, TypeError, ValueError) as exc:
        LOGGER.debug("CLOB book %s…: %s", tid[:16], exc)
        return None
