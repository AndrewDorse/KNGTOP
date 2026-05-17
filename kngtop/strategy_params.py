"""Single-strategy presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ENTRY_PRICE_MIN = 0.29
ENTRY_PRICE_MAX = 0.42
MARKET_BUY_MAX_PRICE = 0.44
CLOSE_TO_START_BPS = 14.0
HEDGE_PRICE_SUM = 0.83
MIN_ELAPSED_SEC = 8


RuleKind = Literal["win_up", "win_dn"]
RuleGroup = Literal["hedge"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    price_min: float
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: RuleKind
    group: RuleGroup = "hedge"
    close_bps: float = CLOSE_TO_START_BPS
    hedge_price_sum: float = HEDGE_PRICE_SUM
    min_elapsed_sec: int = MIN_ELAPSED_SEC
    notional_fraction: float | None = None
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "close_buy_up",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="UP",
        kind="win_up",
    ),
    MispriceRule(
        "close_buy_down",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="DOWN",
        kind="win_dn",
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
    if rule.kind == "win_up":
        return btc > start_btc and rule.price_min <= mid_up <= rule.cheap_max
    if rule.kind == "win_dn":
        return btc < start_btc and rule.price_min <= mid_dn <= rule.cheap_max
    return False
