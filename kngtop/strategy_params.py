"""Single-strategy presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


BTC_ENTRY_PRICE_MIN_5M = 0.01
BTC_ENTRY_PRICE_MAX_5M = 0.30
BTC_MARKET_BUY_MAX_PRICE_5M = 0.85
BTC_ENTRY_PRICE_MIN_15M = 0.01
BTC_ENTRY_PRICE_MAX_15M = 0.25
BTC_MARKET_BUY_MAX_PRICE_15M = 0.85

ETH_ENTRY_PRICE_MIN_5M = 0.01
ETH_ENTRY_PRICE_MAX_5M = 0.25
ETH_MARKET_BUY_MAX_PRICE_5M = 0.85
ETH_ENTRY_PRICE_MIN_15M = 0.01
ETH_ENTRY_PRICE_MAX_15M = 0.20
ETH_MARKET_BUY_MAX_PRICE_15M = 0.85

# Backward-compatible aliases used by tests/helpers that operate on the default ETH rules.
ENTRY_PRICE_MIN_5M = ETH_ENTRY_PRICE_MIN_5M
ENTRY_PRICE_MAX_5M = ETH_ENTRY_PRICE_MAX_5M
ENTRY_PRICE_MIN_15M = ETH_ENTRY_PRICE_MIN_15M
ENTRY_PRICE_MAX_15M = ETH_ENTRY_PRICE_MAX_15M
MARKET_BUY_MAX_PRICE = ETH_MARKET_BUY_MAX_PRICE_5M
MIN_ELAPSED_SEC_5M = 30
MIN_ELAPSED_SEC_15M = 180
MAX_ELAPSED_SEC_5M = 300
MAX_ELAPSED_SEC_15M = 900
RECLAIM_LOOKBACK_SEC_5M = 40
RECLAIM_LOOKBACK_SEC_15M = 40
RECLAIM_GAP_MIN = 0.05


RuleKind = Literal["reclaim_up", "reclaim_dn", "cwc_up", "cwc_dn", "cwm_up", "cwm_dn"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    price_min: float
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: RuleKind
    min_elapsed_sec: int = MIN_ELAPSED_SEC_5M
    max_elapsed_sec: int = MAX_ELAPSED_SEC_5M
    lookback_sec: int = RECLAIM_LOOKBACK_SEC_5M
    gap_min: float = RECLAIM_GAP_MIN
    distance_bps_max: float | None = None
    momentum_lookback_sec: int | None = None
    momentum_bps_min: float | None = None
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


CWM_ENTRY_PRICE_MIN_5M = 0.01
CWM_ENTRY_PRICE_MAX_5M = 0.25
CWM_MIN_ELAPSED_SEC_5M = 20
CWM_MAX_ELAPSED_SEC_5M = 300
CWM_MOMENTUM_LOOKBACK_SEC = 5
CWM_MOMENTUM_BPS_MIN = 0.0


BTC_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "cwm_buy_up",
        price_min=CWM_ENTRY_PRICE_MIN_5M,
        cheap_max=CWM_ENTRY_PRICE_MAX_5M,
        side="UP",
        kind="cwm_up",
        min_elapsed_sec=CWM_MIN_ELAPSED_SEC_5M,
        max_elapsed_sec=CWM_MAX_ELAPSED_SEC_5M,
        momentum_lookback_sec=CWM_MOMENTUM_LOOKBACK_SEC,
        momentum_bps_min=CWM_MOMENTUM_BPS_MIN,
        market_buy_max_price=BTC_MARKET_BUY_MAX_PRICE_5M,
    ),
    MispriceRule(
        "cwm_buy_down",
        price_min=CWM_ENTRY_PRICE_MIN_5M,
        cheap_max=CWM_ENTRY_PRICE_MAX_5M,
        side="DOWN",
        kind="cwm_dn",
        min_elapsed_sec=CWM_MIN_ELAPSED_SEC_5M,
        max_elapsed_sec=CWM_MAX_ELAPSED_SEC_5M,
        momentum_lookback_sec=CWM_MOMENTUM_LOOKBACK_SEC,
        momentum_bps_min=CWM_MOMENTUM_BPS_MIN,
        market_buy_max_price=BTC_MARKET_BUY_MAX_PRICE_5M,
    ),
)

BTC_RULES_15M: tuple[MispriceRule, ...] = ()

ETH_RULES_5M: tuple[MispriceRule, ...] = ()

ETH_RULES_15M: tuple[MispriceRule, ...] = ()

# Backward-compatible aliases used by tests/helpers.
RULES_5M: tuple[MispriceRule, ...] = BTC_RULES_5M
RULES_15M: tuple[MispriceRule, ...] = BTC_RULES_15M


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the active rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    if p == "BTC":
        return BTC_RULES_5M if int(window_minutes) <= 5 else BTC_RULES_15M
    return ()


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    gap = abs(mid_up - mid_dn)
    if rule.kind == "reclaim_up":
        return btc > start_btc and rule.price_min <= mid_up <= rule.cheap_max and gap >= rule.gap_min
    if rule.kind == "reclaim_dn":
        return btc < start_btc and rule.price_min <= mid_dn <= rule.cheap_max and gap >= rule.gap_min
    if rule.kind == "cwm_up":
        return btc > start_btc and rule.price_min <= mid_up <= rule.cheap_max
    if rule.kind == "cwm_dn":
        return btc < start_btc and rule.price_min <= mid_dn <= rule.cheap_max
    return False
