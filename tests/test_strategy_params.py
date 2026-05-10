"""Strategy rule matrix (no network)."""

from __future__ import annotations

from kngtop.strategy_params import (
    RULES_15M,
    RULES_5M,
    XRP_RULES_15M,
    XRP_RULES_5M,
    MispriceRule,
    effective_gap_px,
    rule_fires,
)


def test_rule_fires_under_up() -> None:
    r = MispriceRule("x", gap_usd=5.0, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up")
    assert rule_fires(r, btc=100_010.0, start_btc=100_000.0, mid_up=0.30, mid_dn=0.70)
    assert not rule_fires(r, btc=100_003.0, start_btc=100_000.0, mid_up=0.30, mid_dn=0.70)
    assert not rule_fires(r, btc=100_010.0, start_btc=100_000.0, mid_up=0.40, mid_dn=0.60)


def test_rule_fires_fade_dn_s() -> None:
    r = MispriceRule("o_fade_dn_s", gap_usd=8.0, cheap_max=None, rich_strong=0.68, side="UP", kind="fade_dn_s")
    assert rule_fires(r, btc=100_020.0, start_btc=100_000.0, mid_up=0.4, mid_dn=0.70)
    assert not rule_fires(r, btc=100_005.0, start_btc=100_000.0, mid_up=0.4, mid_dn=0.70)


def test_rules_5m_count() -> None:
    assert len(RULES_5M) == 4


def test_xrp_rules_gap_zero() -> None:
    assert {r.gap_usd for r in XRP_RULES_5M} == {0.0}
    assert {r.gap_usd for r in XRP_RULES_15M} == {0.0}


def test_btc_rules_uniform_gap_matches_paladin_presets() -> None:
    """BTC 5m/15m: one gap_usd per horizon (PALADIN misprice_kngtop_preset_defs_*)."""
    g5 = {r.gap_usd for r in RULES_5M}
    g15 = {r.gap_usd for r in RULES_15M}
    assert g5 == {5.0}
    assert g15 == {10.0}


def test_effective_gap_pct_overrides_usd() -> None:
    r = MispriceRule(
        "x",
        gap_usd=999.0,
        cheap_max=0.35,
        rich_strong=None,
        side="UP",
        kind="under_up",
        gap_pct_of_start=0.1,
    )
    assert abs(effective_gap_px(r, 10_000.0) - 10.0) < 1e-9


def test_rule_eth_signal_gap_usd_fires_under_up() -> None:
    r = MispriceRule("u_up", gap_usd=0.05, cheap_max=0.35, rich_strong=None, side="UP", kind="under_up")
    start = 3000.0
    spot = start + 0.06
    assert rule_fires(r, btc=spot, start_btc=start, mid_up=0.33, mid_dn=0.67)
    assert not rule_fires(r, btc=start + 0.04, start_btc=start, mid_up=0.33, mid_dn=0.67)
