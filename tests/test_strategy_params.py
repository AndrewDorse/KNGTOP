"""Strategy rule matrix (no network)."""

from __future__ import annotations

import pytest

from kngtop.strategy_params import (
    CLOSE_TO_START_BPS,
    ENTRY_PRICE_MAX,
    ENTRY_PRICE_MIN,
    HEDGE_PRICE_SUM,
    MIN_ELAPSED_SEC,
    RULES_15M,
    RULES_5M,
    MispriceRule,
    rule_fires,
    rules_for_asset,
)


def test_rule_fires_win_up() -> None:
    r = MispriceRule(
        "close_buy_up",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="UP",
        kind="win_up",
    )
    assert rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.35, mid_dn=0.70)
    assert not rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.35, mid_dn=0.70)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.35, mid_dn=0.70)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.45, mid_dn=0.70)


def test_rule_fires_win_down() -> None:
    r = MispriceRule(
        "close_buy_down",
        price_min=ENTRY_PRICE_MIN,
        cheap_max=ENTRY_PRICE_MAX,
        side="DOWN",
        kind="win_dn",
    )
    assert rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.35)
    assert not rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.35)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.35)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.27)


def test_rules_count_and_defaults() -> None:
    assert len(RULES_5M) == 2
    assert len(RULES_15M) == 0
    assert all(rule.close_bps == CLOSE_TO_START_BPS for rule in RULES_5M)
    assert all(rule.hedge_price_sum == HEDGE_PRICE_SUM for rule in RULES_5M)
    assert all(rule.min_elapsed_sec == MIN_ELAPSED_SEC for rule in RULES_5M)


def test_rules_are_selected_for_btc_only() -> None:
    assert rules_for_asset("BTC", 5) == RULES_5M
    assert rules_for_asset("BTC", 15) == RULES_15M
    assert rules_for_asset("ETH", 5) == ()
    assert rules_for_asset("DOGE", 5) == ()


def test_rules_reject_unknown_asset() -> None:
    with pytest.raises(ValueError, match="unsupported asset pair"):
        rules_for_asset("ADA", 5)
