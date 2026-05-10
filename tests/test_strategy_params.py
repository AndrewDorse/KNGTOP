"""Strategy rule matrix (no network)."""

from __future__ import annotations

from kngtop.strategy_params import (
    CHEAP_PRICE_MAX,
    RULES_15M,
    RULES_5M,
    MispriceRule,
    rule_fires,
    rules_for_asset,
)


def test_rule_fires_cheap_up() -> None:
    r = MispriceRule("cheap_buy_up", cheap_max=CHEAP_PRICE_MAX, side="UP", kind="cheap_up")
    assert rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.19, mid_dn=0.81)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.19, mid_dn=0.81)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.20, mid_dn=0.80)


def test_rule_fires_cheap_down() -> None:
    r = MispriceRule("cheap_buy_down", cheap_max=CHEAP_PRICE_MAX, side="DOWN", kind="cheap_dn")
    assert rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.81, mid_dn=0.19)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.81, mid_dn=0.19)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.80, mid_dn=0.20)


def test_rules_count_per_window() -> None:
    assert len(RULES_5M) == 2
    assert len(RULES_15M) == 2


def test_rules_are_uniform_across_assets() -> None:
    assert rules_for_asset("BTC", 5) == RULES_5M
    assert rules_for_asset("ETH", 15) == RULES_15M
    assert rules_for_asset("XRP", 5) == RULES_5M
    assert rules_for_asset("SOL", 15) == RULES_15M
