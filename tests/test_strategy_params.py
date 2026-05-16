"""Strategy rule matrix (no network)."""

from __future__ import annotations

from kngtop.strategy_params import (
    CHEAP_PRICE_MAX,
    CLOSE_TO_START_BPS,
    RULES_15M,
    RULES_5M,
    MispriceRule,
    rule_fires,
    rules_for_asset,
)


def test_rule_fires_close_up() -> None:
    r = MispriceRule("close_buy_up", cheap_max=CHEAP_PRICE_MAX, side="UP", kind="lose_up")
    assert rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.25, mid_dn=0.85)
    assert rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.25, mid_dn=0.85)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.25, mid_dn=0.85)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.26, mid_dn=0.79)


def test_rule_fires_close_down() -> None:
    r = MispriceRule("close_buy_down", cheap_max=CHEAP_PRICE_MAX, side="DOWN", kind="lose_dn")
    assert rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.85, mid_dn=0.25)
    assert rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.85, mid_dn=0.25)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.85, mid_dn=0.25)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.79, mid_dn=0.26)


def test_rules_count_per_window() -> None:
    assert len(RULES_5M) == 2
    assert len(RULES_15M) == 0
    assert all(rule.close_bps == CLOSE_TO_START_BPS for rule in RULES_5M)


def test_rules_are_selected_per_asset() -> None:
    assert rules_for_asset("BTC", 5) == RULES_5M
    assert rules_for_asset("ETH", 5) == RULES_5M
    assert rules_for_asset("XRP", 5) == RULES_5M
    assert rules_for_asset("SOL", 5) == RULES_5M
    assert rules_for_asset("DOGE", 5) == RULES_5M
    assert rules_for_asset("BNB", 5) == RULES_5M
    assert rules_for_asset("HYPE", 5) == RULES_5M
    assert rules_for_asset("LINK", 5) == RULES_5M
    assert rules_for_asset("BTC", 15) == RULES_15M
