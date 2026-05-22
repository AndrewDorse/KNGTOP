"""Config validation."""

from __future__ import annotations

import pytest

from kngtop.config import KngtopConfig, parse_trading_pairs


def test_from_env_requires_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POLY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("POLY_FUNDER", raising=False)
    with pytest.raises(RuntimeError, match="POLY_PRIVATE_KEY"):
        KngtopConfig.from_env()


def test_dry_run_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("POLY_DRY_RUN", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.dry_run is False


def test_pairs_default_to_btc_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_PAIRS", raising=False)
    cfg = KngtopConfig.from_env()
    assert tuple(cfg.trading_pairs) == (("BTC", "BTCUSDT"),)


def test_parse_pairs_accepts_multiple_assets() -> None:
    assert parse_trading_pairs("BTC:BTCUSDT,ETH:ETHUSDT") == (
        ("BTC", "BTCUSDT"),
        ("ETH", "ETHUSDT"),
    )


def test_pairs_reject_unknown_asset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.setenv("KNGTOP_PAIRS", "ADA:ADAUSDT")
    with pytest.raises(RuntimeError, match="Unsupported asset"):
        KngtopConfig.from_env()


def test_market_buy_max_price_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_MARKET_BUY_MAX_PRICE", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.market_buy_max_price == 0.85


def test_max_shares_per_side_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_MAX_SHARES_PER_SIDE", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.max_shares_per_side == 15.0

    monkeypatch.setenv("KNGTOP_MAX_SHARES_PER_SIDE", "12.5")
    cfg = KngtopConfig.from_env()
    assert cfg.max_shares_per_side == 12.5


def test_balance_guard_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_MAX_SHARE_GAP", raising=False)
    monkeypatch.delenv("KNGTOP_REPAIR_AVG_SUM_CAP", raising=False)
    monkeypatch.delenv("KNGTOP_LOCKED_PROFIT_ROI", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.max_share_gap == 2.0
    assert cfg.repair_avg_sum_cap == 0.95
    assert cfg.locked_profit_roi == 0.10

    monkeypatch.setenv("KNGTOP_MAX_SHARE_GAP", "1.5")
    monkeypatch.setenv("KNGTOP_REPAIR_AVG_SUM_CAP", "0.93")
    monkeypatch.setenv("KNGTOP_LOCKED_PROFIT_ROI", "0.12")
    cfg = KngtopConfig.from_env()
    assert cfg.max_share_gap == 1.5
    assert cfg.repair_avg_sum_cap == 0.93
    assert cfg.locked_profit_roi == 0.12


def test_ws_rest_poll_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_WS_REST_POLL_ENABLE", raising=False)
    monkeypatch.delenv("KNGTOP_WS_REST_POLL_INTERVAL_SECONDS", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.ws_rest_poll_enabled is True
    assert cfg.ws_rest_poll_interval_sec == 1.0


def test_request_timeout_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_REQUEST_TIMEOUT_SECONDS", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.request_timeout_sec == 5.0
