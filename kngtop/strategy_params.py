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

# ETH — gap as %% of Binance spot at window open (~$5 BTC-style move at ~$3.3k => ~0.15%%).
_ETH_GAP_5 = 0.15
_ETH_GAP_DN_5 = 0.24  # mirrors 8 vs 5 USD on BTC fades / under_dn spread
_ETH_GAP_15_UNDN = 0.24

ETH_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap", gap_usd=0.0, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up", gap_pct_of_start=_ETH_GAP_5
    ),
    MispriceRule(
        "u_dn_cheap",
        gap_usd=0.0,
        cheap_max=0.35,
        rich_strong=None,
        side="DOWN",
        kind="under_dn",
        gap_pct_of_start=_ETH_GAP_5,
    ),
    MispriceRule(
        "o_fade_up_s",
        gap_usd=0.0,
        cheap_max=None,
        rich_strong=0.68,
        side="DOWN",
        kind="fade_up_s",
        gap_pct_of_start=_ETH_GAP_5,
    ),
    MispriceRule(
        "o_fade_dn_s",
        gap_usd=0.0,
        cheap_max=None,
        rich_strong=0.68,
        side="UP",
        kind="fade_dn_s",
        gap_pct_of_start=_ETH_GAP_DN_5,
    ),
)

ETH_RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "u_up_cheap",
        gap_usd=0.0,
        cheap_max=0.38,
        rich_strong=None,
        side="UP",
        kind="under_up",
        gap_pct_of_start=_ETH_GAP_5,
    ),
    MispriceRule(
        "u_dn_cheap",
        gap_usd=0.0,
        cheap_max=0.38,
        rich_strong=None,
        side="DOWN",
        kind="under_dn",
        gap_pct_of_start=_ETH_GAP_15_UNDN,
    ),
    MispriceRule(
        "o_fade_up_s",
        gap_usd=0.0,
        cheap_max=None,
        rich_strong=0.72,
        side="DOWN",
        kind="fade_up_s",
        gap_pct_of_start=_ETH_GAP_5,
    ),
    MispriceRule(
        "o_fade_dn_s",
        gap_usd=0.0,
        cheap_max=None,
        rich_strong=0.68,
        side="UP",
        kind="fade_dn_s",
        gap_pct_of_start=_ETH_GAP_5,
    ),
)

# XRP — absolute USD move on USDT quote (tiny dollar gaps; tune via fork / env presets later).
_XRP_U5 = 0.035
_XRP_U15_DN = 0.055
_XRP_FDN = 0.055

XRP_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=_XRP_U5, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=_XRP_U5, cheap_max=0.35, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=_XRP_U5, cheap_max=None, rich_strong=0.68, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=_XRP_FDN, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
)

XRP_RULES_15M: tuple[MispriceRule, ...] = (
    MispriceRule("u_up_cheap", gap_usd=_XRP_U5, cheap_max=0.38, rich_strong=None, side="UP", kind="under_up"),
    MispriceRule("u_dn_cheap", gap_usd=_XRP_U15_DN, cheap_max=0.38, rich_strong=None, side="DOWN", kind="under_dn"),
    MispriceRule("o_fade_up_s", gap_usd=_XRP_U5, cheap_max=None, rich_strong=0.72, side="DOWN", kind="fade_up_s"),
    MispriceRule("o_fade_dn_s", gap_usd=_XRP_U5, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s"),
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
    raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, or XRP)")


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
