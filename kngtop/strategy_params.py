"""Single-strategy presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ENTRY_PRICE_MIN_5M = 0.01
ENTRY_PRICE_MAX_5M = 0.36
ENTRY_PRICE_MIN_15M = 0.01
ENTRY_PRICE_MAX_15M = 0.37
MARKET_BUY_MAX_PRICE = 0.38
MIN_ELAPSED_SEC_5M = 30
MIN_ELAPSED_SEC_15M = 180
MAX_ELAPSED_SEC_5M = 300
MAX_ELAPSED_SEC_15M = 900
RECLAIM_LOOKBACK_SEC_5M = 40
RECLAIM_LOOKBACK_SEC_15M = 60
RECLAIM_GAP_MIN = 0.05


RuleKind = Literal["reclaim_up", "reclaim_dn"]


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
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "reclaim_buy_up",
        price_min=ENTRY_PRICE_MIN_5M,
        cheap_max=ENTRY_PRICE_MAX_5M,
        side="UP",
        kind="reclaim_up",
    ),
    MispriceRule(
        "reclaim_buy_down",
        price_min=ENTRY_PRICE_MIN_5M,
        cheap_max=ENTRY_PRICE_MAX_5M,
        side="DOWN",
        kind="reclaim_dn",
    ),
)

RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "reclaim_buy_up",
        price_min=ENTRY_PRICE_MIN_15M,
        cheap_max=ENTRY_PRICE_MAX_15M,
        side="UP",
        kind="reclaim_up",
        min_elapsed_sec=MIN_ELAPSED_SEC_15M,
        max_elapsed_sec=MAX_ELAPSED_SEC_15M,
        lookback_sec=RECLAIM_LOOKBACK_SEC_15M,
    ),
    MispriceRule(
        "reclaim_buy_down",
        price_min=ENTRY_PRICE_MIN_15M,
        cheap_max=ENTRY_PRICE_MAX_15M,
        side="DOWN",
        kind="reclaim_dn",
        min_elapsed_sec=MIN_ELAPSED_SEC_15M,
        max_elapsed_sec=MAX_ELAPSED_SEC_15M,
        lookback_sec=RECLAIM_LOOKBACK_SEC_15M,
    ),
)


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the active rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    if p != "BTC":
        return ()
    return RULES_5M if int(window_minutes) <= 5 else RULES_15M


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    gap = abs(mid_up - mid_dn)
    if rule.kind == "reclaim_up":
        return btc > start_btc and rule.price_min <= mid_up <= rule.cheap_max and gap >= rule.gap_min
    if rule.kind == "reclaim_dn":
        return btc < start_btc and rule.price_min <= mid_dn <= rule.cheap_max and gap >= rule.gap_min
    return False
