"""Strategy rule matrix (no network)."""

from __future__ import annotations

import pytest

from kngtop.strategy_params import (
    ENTRY_PRICE_MAX_15M,
    ENTRY_PRICE_MAX_5M,
    ENTRY_PRICE_MIN_15M,
    ENTRY_PRICE_MIN_5M,
    MAX_ELAPSED_SEC_15M,
    MAX_ELAPSED_SEC_5M,
    MIN_ELAPSED_SEC_15M,
    MIN_ELAPSED_SEC_5M,
    RECLAIM_GAP_MIN,
    RECLAIM_LOOKBACK_SEC_15M,
    RECLAIM_LOOKBACK_SEC_5M,
    RULES_15M,
    RULES_5M,
    MispriceRule,
    rule_fires,
    rules_for_asset,
)


def test_rule_fires_reclaim_up() -> None:
    r = MispriceRule(
        "reclaim_buy_up",
        price_min=ENTRY_PRICE_MIN_5M,
        cheap_max=ENTRY_PRICE_MAX_5M,
        side="UP",
        kind="reclaim_up",
    )
    assert rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.20, mid_dn=0.70)
    assert not rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.20, mid_dn=0.70)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.20, mid_dn=0.70)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.46, mid_dn=0.70)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.30, mid_dn=0.27)


def test_rule_fires_reclaim_down() -> None:
    r = MispriceRule(
        "reclaim_buy_down",
        price_min=ENTRY_PRICE_MIN_5M,
        cheap_max=ENTRY_PRICE_MAX_5M,
        side="DOWN",
        kind="reclaim_dn",
    )
    assert rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.20)
    assert not rule_fires(r, btc=100_000.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.20)
    assert not rule_fires(r, btc=100_001.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.20)
    assert rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.70, mid_dn=0.25)
    assert not rule_fires(r, btc=99_999.0, start_btc=100_000.0, mid_up=0.30, mid_dn=0.27)


def test_rules_count_and_defaults() -> None:
    assert len(RULES_5M) == 2
    assert len(RULES_15M) == 2
    assert all(rule.min_elapsed_sec == MIN_ELAPSED_SEC_5M for rule in RULES_5M)
    assert all(rule.max_elapsed_sec == MAX_ELAPSED_SEC_5M for rule in RULES_5M)
    assert all(rule.lookback_sec == RECLAIM_LOOKBACK_SEC_5M for rule in RULES_5M)
    assert all(rule.price_min == ENTRY_PRICE_MIN_5M for rule in RULES_5M)
    assert all(rule.cheap_max == ENTRY_PRICE_MAX_5M for rule in RULES_5M)
    assert all(rule.min_elapsed_sec == MIN_ELAPSED_SEC_15M for rule in RULES_15M)
    assert all(rule.max_elapsed_sec == MAX_ELAPSED_SEC_15M for rule in RULES_15M)
    assert all(rule.lookback_sec == RECLAIM_LOOKBACK_SEC_15M for rule in RULES_15M)
    assert all(rule.price_min == ENTRY_PRICE_MIN_15M for rule in RULES_15M)
    assert all(rule.cheap_max == ENTRY_PRICE_MAX_15M for rule in RULES_15M)
    assert all(rule.gap_min == RECLAIM_GAP_MIN for rule in RULES_5M)
    assert all(rule.gap_min == RECLAIM_GAP_MIN for rule in RULES_15M)


def test_rules_are_selected_for_eth_only() -> None:
    assert rules_for_asset("ETH", 5) == RULES_5M
    assert rules_for_asset("ETH", 15) == RULES_15M
    assert rules_for_asset("BTC", 5) == ()
    assert rules_for_asset("DOGE", 5) == ()


def test_rules_reject_unknown_asset() -> None:
    with pytest.raises(ValueError, match="unsupported asset pair"):
        rules_for_asset("ADA", 5)
