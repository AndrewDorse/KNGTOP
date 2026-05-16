"""Single-strategy presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CHEAP_PRICE_MAX = 0.14
MARKET_BUY_MAX_PRICE = 0.16
CLOSE_TO_START_BPS = 5.0


RuleKind = Literal["close_up", "close_dn"]
RuleGroup = Literal["quaternary"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: RuleKind
    group: RuleGroup = "quaternary"
    close_bps: float = CLOSE_TO_START_BPS
    notional_fraction: float | None = None
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "close_buy_up",
        cheap_max=CHEAP_PRICE_MAX,
        side="UP",
        kind="close_up",
    ),
    MispriceRule(
        "close_buy_down",
        cheap_max=CHEAP_PRICE_MAX,
        side="DOWN",
        kind="close_dn",
    ),
)

RULES_15M: tuple[MispriceRule, ...] = ()


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the active rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    return RULES_5M if int(window_minutes) <= 5 else RULES_15M


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    diff_bps = abs((btc - start_btc) / start_btc * 10_000.0) if start_btc > 0 else 0.0
    if diff_bps > rule.close_bps:
        return False
    if rule.kind == "close_up":
        return btc > start_btc and mid_up <= rule.cheap_max
    if rule.kind == "close_dn":
        return btc < start_btc and mid_dn <= rule.cheap_max
    return False
