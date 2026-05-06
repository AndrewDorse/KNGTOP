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
    #: If set, gap = ``start_px * gap_pct_of_start / 100`` (overrides ``gap_usd``).
    gap_pct_of_start: float | None = None


# 5m pool — best total PnL rows (cheap_max / rich_strong inert dims noted in README)
RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=6.0, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=6.0, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=6.0, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=9.0, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
)

# 15m pool
RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=6.0, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=6.0, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=6.0, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=9.0, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
)

# ETH / XRP — Binance spot vs window open: fixed USD gap for all four rules (5m and 15m).
# Market buy notional stays $1 for every asset (see ``KngtopConfig.notional_usd``).
_ETH_SIGNAL_GAP_USD = 0.05
_XRP_SIGNAL_GAP_USD = 0.0003
_SOL_SIGNAL_GAP_USD = 0.005

ETH_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule(
        "u_dn_cheap", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)

ETH_RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule(
        "u_dn_cheap", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_ETH_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)

XRP_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"
    ),
    MispriceRule(
        "u_dn_cheap", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)

XRP_RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"
    ),
    MispriceRule(
        "u_dn_cheap", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_XRP_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)


SOL_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"
    ),
    MispriceRule(
        "u_dn_cheap", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)

SOL_RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"
    ),
    MispriceRule(
        "u_dn_cheap", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"
    ),
    MispriceRule(
        "o_fade_up_s", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"
    ),
    MispriceRule(
        "o_fade_dn_s", gap_usd=_SOL_SIGNAL_GAP_USD, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"
    ),
)


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the four-rule preset for a Gamma asset key (BTC/ETH/XRP) and timeframe."""
    p = (pair or "").strip().upper()
    is_15 = int(window_minutes) >= 15
    if p == "BTC":
        return RULES_15M if is_15 else RULES_5M
    if p == "ETH":
        return ETH_RULES_15M if is_15 else ETH_RULES_5M
    if p == "XRP":
        return XRP_RULES_15M if is_15 else XRP_RULES_5M
    if p == "SOL":
        return SOL_RULES_15M if is_15 else SOL_RULES_5M
    raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, or SOL)")


def effective_gap_px(rule: MispriceRule, start_px: float) -> float:
    if rule.gap_pct_of_start is not None:
        return abs(float(start_px)) * (float(rule.gap_pct_of_start) / 100.0)
    return float(rule.gap_usd)


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    g = effective_gap_px(rule, start_btc)
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
