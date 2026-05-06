"""REST fallback when WS-backed caches are stale (quiet tape or disconnected WS)."""

from __future__ import annotations

import logging
import threading

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_spot_price
from kngtop.config import KngtopConfig
from kngtop.pm_rest import fetch_clob_best_bid_ask
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")


def run_ws_rest_fallback_loop(
    stop: threading.Event,
    cfg: KngtopConfig,
    binance: BinanceCombinedTradeFeed,
    poly: MarketWsFeed,
) -> None:
    """Background thread: poll REST at ``ws_rest_poll_interval_sec`` for symbols/tokens with stale cache."""
    bin_age = float(cfg.binance_max_age_sec)
    poly_age = float(cfg.poly_mid_max_age_sec)
    timeout = float(cfg.request_timeout_sec)

    while not stop.wait(cfg.ws_rest_poll_interval_sec):
        if not cfg.ws_rest_poll_enabled:
            continue

        for sym in binance.symbols:
            if binance.last_price(sym, max_age_sec=bin_age) is not None:
                continue
            px = fetch_binance_spot_price(symbol=sym, timeout=timeout)
            if px is None:
                continue
            binance.apply_trade_price(sym, px)

        for aid in poly.subscribed_asset_ids():
            if poly.mid_for(aid, max_age_sec=poly_age) is not None:
                continue
            row = fetch_clob_best_bid_ask(token_id=aid, timeout=timeout)
            if row is None:
                continue
            bb, ba = row
            poly.apply_quote(aid, bb, ba)
