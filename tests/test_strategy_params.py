"""Strategy rule matrix (no network)."""

from __future__ import annotations

import pytest

from kngtop.strategy_params import (
    BTC_HEDGE_START_LIMIT_5M,
    BTC_HEDGE_TARGET_SUM_5M,
    BTC_RULES_15M,
    BTC_RULES_5M,
    ETH_RULES_15M,
    ETH_RULES_5M,
    MAX_ELAPSED_SEC_5M,
    RULES_15M,
    RULES_5M,
    MispriceRule,
    rule_fires,
    rules_for_asset,
)


def test_rule_fires_serial_hedge() -> None:
    rule = MispriceRule(
        "serial_hedge_12c_sum68",
        price_min=0.01,
        cheap_max=0.12,
        side="BOTH",
        kind="serial_hedge",
    )
    assert rule_fires(rule, btc=100_001.0, start_btc=100_000.0, mid_up=0.08, mid_dn=0.92)
    assert rule_fires(rule, btc=99_999.0, start_btc=100_000.0, mid_up=0.88, mid_dn=0.12)
    assert not rule_fires(rule, btc=100_001.0, start_btc=100_000.0, mid_up=0.14, mid_dn=0.86)


def test_rules_count_and_defaults() -> None:
    assert len(BTC_RULES_5M) == 1
    assert len(BTC_RULES_15M) == 0
    assert len(ETH_RULES_5M) == 0
    assert len(ETH_RULES_15M) == 0

    rule = BTC_RULES_5M[0]
    assert rule.kind == "serial_hedge"
    assert rule.price_min == 0.01
    assert rule.cheap_max == BTC_HEDGE_START_LIMIT_5M == 0.12
    assert rule.min_elapsed_sec == 0
    assert rule.max_elapsed_sec == MAX_ELAPSED_SEC_5M
    assert BTC_HEDGE_TARGET_SUM_5M == 0.68


def test_rules_are_selected_for_btc_only() -> None:
    assert rules_for_asset("BTC", 5) == BTC_RULES_5M
    assert rules_for_asset("BTC", 15) == ()
    assert rules_for_asset("ETH", 5) == ()
    assert rules_for_asset("ETH", 15) == ()
    assert rules_for_asset("DOGE", 5) == ()
    assert RULES_5M == BTC_RULES_5M
    assert RULES_15M == BTC_RULES_15M


def test_rules_reject_unknown_asset() -> None:
    with pytest.raises(ValueError, match="unsupported asset pair"):
        rules_for_asset("ADA", 5)
