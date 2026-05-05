"""Config validation."""

import pytest

from kngtop.config import KngtopConfig


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
    assert cfg.dry_run is True


def test_pairs_default_includes_three_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_PAIRS", raising=False)
    cfg = KngtopConfig.from_env()
    assert tuple(cfg.trading_pairs) == (
        ("BTC", "BTCUSDT"),
        ("ETH", "ETHUSDT"),
        ("XRP", "XRPUSDT"),
    )


def test_pairs_rejects_unknown_asset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.setenv("KNGTOP_PAIRS", "SOL:SOLUSDT")
    with pytest.raises(RuntimeError, match="Unsupported asset"):
        KngtopConfig.from_env()


def test_notional_per_pair_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("KNGTOP_NOTIONAL_ETH_USD", raising=False)
    monkeypatch.delenv("KNGTOP_NOTIONAL_XRP_USD", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.notional_for_pair("BTC") == cfg.notional_usd == 1.0
    assert cfg.notional_for_pair("ETH") == 0.05
    assert cfg.notional_for_pair("XRP") == 0.0005
