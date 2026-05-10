"""Inverted side presets for KNGTOP live trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CHEAP_PRICE_MAX = 0.30


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One trigger rule for a timeframe."""

    key: str
    cheap_max: float
    side: Literal["UP", "DOWN"]
    kind: Literal["cheap_up", "cheap_dn"]


_CHEAP_RULES: tuple[MispriceRule, ...] = (
    MispriceRule(
        "cheap_buy_up",
        cheap_max=CHEAP_PRICE_MAX,
        side="DOWN",
        kind="cheap_up",
    ),
    MispriceRule(
        "cheap_buy_down",
        cheap_max=CHEAP_PRICE_MAX,
        side="UP",
        kind="cheap_dn",
    ),
)

RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES
RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES
ETH_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES
ETH_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES
XRP_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES
XRP_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES
SOL_RULES_5M: tuple[MispriceRule, ...] = _CHEAP_RULES
SOL_RULES_15M: tuple[MispriceRule, ...] = _CHEAP_RULES


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the inverted-side rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, or SOL)")
    return RULES_15M if int(window_minutes) >= 15 else RULES_5M


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    if rule.kind == "cheap_up":
        return btc > start_btc and mid_up <= rule.cheap_max
    if rule.kind == "cheap_dn":
        return btc < start_btc and mid_dn <= rule.cheap_max
    return False
