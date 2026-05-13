"""Cheap-side presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CHEAP_PRICE_MAX = 0.15
SECONDARY_NOTIONAL_FRACTION = 0.02


RuleKind = Literal["cheap_up", "cheap_dn", "revert_up", "revert_dn"]
RuleGroup = Literal["primary", "secondary"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: RuleKind
    group: RuleGroup = "primary"
    lookback_sec: int = 0
    lead_bps: float = 0.0
    close_bps: float = 0.0
    notional_fraction: float | None = None


_CHEAP_RULES: tuple[MispriceRule, ...] = (
    MispriceRule(
        "cheap_buy_up",
        cheap_max=CHEAP_PRICE_MAX,
        side="UP",
        kind="cheap_up",
    ),
    MispriceRule(
        "cheap_buy_down",
        cheap_max=CHEAP_PRICE_MAX,
        side="DOWN",
        kind="cheap_dn",
    ),
)


def _revert_rules(*, lookback_sec: int, lead_bps: float, close_bps: float) -> tuple[MispriceRule, ...]:
    return (
        MispriceRule(
            "revert_buy_up",
            cheap_max=CHEAP_PRICE_MAX,
            side="UP",
            kind="revert_up",
            group="secondary",
            lookback_sec=lookback_sec,
            lead_bps=lead_bps,
            close_bps=close_bps,
            notional_fraction=SECONDARY_NOTIONAL_FRACTION,
        ),
        MispriceRule(
            "revert_buy_down",
            cheap_max=CHEAP_PRICE_MAX,
            side="DOWN",
            kind="revert_dn",
            group="secondary",
            lookback_sec=lookback_sec,
            lead_bps=lead_bps,
            close_bps=close_bps,
            notional_fraction=SECONDARY_NOTIONAL_FRACTION,
        ),
    )


BTC_SECONDARY = _revert_rules(lookback_sec=120, lead_bps=2.0, close_bps=10.0)
ETH_SECONDARY = _revert_rules(lookback_sec=120, lead_bps=2.0, close_bps=10.0)
SOL_SECONDARY = _revert_rules(lookback_sec=90, lead_bps=3.0, close_bps=10.0)
NO_SECONDARY: tuple[MispriceRule, ...] = ()


RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES + BTC_SECONDARY
RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES + BTC_SECONDARY
ETH_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES + ETH_SECONDARY
ETH_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES + ETH_SECONDARY
XRP_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES + NO_SECONDARY
XRP_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES + NO_SECONDARY
SOL_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES + SOL_SECONDARY
SOL_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES + SOL_SECONDARY


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the cheap-side rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    if p == "ETH":
        return ETH_RULES_15M if int(window_minutes) >= 15 else ETH_RULES_5M
    if p == "XRP":
        return XRP_RULES_15M if int(window_minutes) >= 15 else XRP_RULES_5M
    if p == "SOL":
        return SOL_RULES_15M if int(window_minutes) >= 15 else SOL_RULES_5M
    return RULES_15M if int(window_minutes) >= 15 else RULES_5M


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    if rule.kind == "cheap_up":
        return btc > start_btc and mid_up <= rule.cheap_max
    if rule.kind == "cheap_dn":
        return btc < start_btc and mid_dn <= rule.cheap_max
    return False
