"""Single-strategy presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ENTRY_PRICE_MIN = 0.28
ENTRY_PRICE_MAX = 0.45
MARKET_BUY_MAX_PRICE = 0.45
MIN_ELAPSED_SEC = 30
MAX_ELAPSED_SEC = 300
RECLAIM_LOOKBACK_SEC = 40


RuleKind = Literal["reclaim_up", "reclaim_dn"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    price_min: float
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: RuleKind
    min_elapsed_sec: int = MIN_ELAPSED_SEC
    max_elapsed_sec: int = MAX_ELAPSED_SEC
    lookback_sec: int = RECLAIM_LOOKBACK_SEC
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "reclaim_buy_up",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="UP",
        kind="reclaim_up",
    ),
    MispriceRule(
        "reclaim_buy_down",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="DOWN",
        kind="reclaim_dn",
    ),
)

RULES_15M: tuple[MispriceRule, ...] = ()


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the active rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    if p != "BTC":
        return ()
    return RULES_5M if int(window_minutes) <= 5 else RULES_15M


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    if rule.kind == "reclaim_up":
        return btc > start_btc and rule.price_min <= mid_up <= rule.cheap_max
    if rule.kind == "reclaim_dn":
        return btc < start_btc and rule.price_min <= mid_dn <= rule.cheap_max
    return False
