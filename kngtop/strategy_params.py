"""Preset thresholds from sweep: best total PnL (5m min 6 trades, 15m min 30 trades).

See ``kng_bot3`` exports ``SWEEP_FOUR_MISPRICE_PARAMS.csv`` / ``PALADIN/sweep_four_misprice_params.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One of four strategies for a timeframe."""

    key: str
    gap_usd: float
    cheap_max: float | None
    rich_strong: float | None
    side: Literal["UP", "DOWN"]
    kind: Literal["under_up", "under_dn", "fade_up_s", "fade_dn_s"]


# 5m pool — best total PnL rows (cheap_max / rich_strong inert dims noted in README)
RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=5.0, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=5.0, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=5.0, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=8.0, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
)

# 15m pool
RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=5.0, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=8.0, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=5.0, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=5.0, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
)


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    g = rule.gap_usd
    if rule.kind == "under_up":
        assert rule.cheap_max is not None
        return btc > start_btc + g and mid_up <= rule.cheap_max
    if rule.kind == "under_dn":
        assert rule.cheap_max is not None
        return btc < start_btc - g and mid_dn <= rule.cheap_max
    if rule.kind == "fade_up_s":
        assert rule.rich_strong is not None
        return btc < start_btc - g and mid_up >= rule.rich_strong
    if rule.kind == "fade_dn_s":
        assert rule.rich_strong is not None
        return btc > start_btc + g and mid_dn >= rule.rich_strong
    return False
