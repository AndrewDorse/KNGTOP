"""Strategy rule matrix (no network)."""

from __future__ import annotations

from kngtop.strategy_params import (
    RULES_5M,
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


def test_rule_eth_style_pct_fires_under_up() -> None:
    r = MispriceRule(
        "u_up",
        gap_usd=0.0,
        cheap_max=0.35,
        rich_strong=None,
        side="UP",
        kind="under_up",
        gap_pct_of_start=0.15,
    )
    # 3000 ETH * 0.15% = $4.5 gap
    start = 3000.0
    spot = start + 5.0
    assert rule_fires(r, btc=spot, start_btc=start, mid_up=0.33, mid_dn=0.67)
