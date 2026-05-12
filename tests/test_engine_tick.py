"""Engine tick path with fakes (no Polymarket / Binance sockets)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kngtop.config import KngtopConfig
from kngtop.engine import (
    ALT_BALANCE_NOTIONAL_FRACTION,
    BALANCE_NOTIONAL_FRACTION,
    MIN_WINDOW_PROGRESS_FRACTION,
    WindowRunner,
    _planned_window_notional_usd,
    _tick_runner,
    _window_elapsed_ready,
)
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.strategy_params import RULES_5M
from kngtop.clob_client import _normalize_usdc_balance


class _FakePoly:
    def __init__(self, mid_up: float, mid_dn: float) -> None:
        self._up = mid_up
        self._dn = mid_dn

    def mid_for(self, token_id: str, max_age_sec: float = 5.0) -> float | None:
        if token_id == "tid_up":
            return self._up
        if token_id == "tid_dn":
            return self._dn
        return None


class _FakeBinanceCombo:
    def __init__(self, px: float, symbol: str = "BTCUSDT") -> None:
        self._px = px
        self._sym = symbol

    def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
        if symbol.strip().upper() != self._sym:
            return None
        return self._px


def _contract(*, slug: str = "btc-updown-5m-1777900500") -> ActiveContract:
    end = datetime.now(timezone.utc) + timedelta(minutes=30)
    return ActiveContract(
        slug=slug,
        question="q",
        end_time=end,
        up=TokenMarket("tid_up", "UP", "0.01", False),
        down=TokenMarket("tid_dn", "DOWN", "0.01", False),
    )


def test_tick_fires_cheap_up_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()

    start = int(datetime.now(timezone.utc).timestamp()) - 90
    c = _contract(slug=f"btc-updown-5m-{start}")
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=c,
        window_minutes=5,
        rules=RULES_5M,
    )
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    runner.traded = False
    poly = _FakePoly(mid_up=0.15, mid_dn=0.85)
    bn = _FakeBinanceCombo(100_020.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert runner.traded


def test_tick_no_fire_when_price_not_cheap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "b" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    c = _contract(slug=f"btc-updown-5m-{start}")
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=c,
        window_minutes=5,
        rules=RULES_5M,
    )
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    poly = _FakePoly(mid_up=0.16, mid_dn=0.80)
    bn = _FakeBinanceCombo(100_002.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert not runner.traded


def test_tick_fires_cheap_down_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "c" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()

    start = int(datetime.now(timezone.utc).timestamp()) - 90
    c = _contract(slug=f"btc-updown-5m-{start}")
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=c,
        window_minutes=5,
        rules=RULES_5M,
    )
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    poly = _FakePoly(mid_up=0.85, mid_dn=0.15)
    bn = _FakeBinanceCombo(99_980.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert runner.traded


class _FakeClobBalance:
    def __init__(self, balance: float | None) -> None:
        self._balance = balance

    def available_balance_usdc(self) -> float | None:
        return self._balance


def test_planned_window_notional_uses_balance_fraction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "d" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    clob = _FakeClobBalance(50.0)
    assert _planned_window_notional_usd(cfg, clob, pair_key="BTC", window_minutes=5) == 50.0 * BALANCE_NOTIONAL_FRACTION


def test_planned_window_notional_has_one_dollar_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "e" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    clob = _FakeClobBalance(5.0)
    assert _planned_window_notional_usd(cfg, clob, pair_key="BTC", window_minutes=5) == 1.0


def test_planned_window_notional_uses_five_percent_for_new_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "f" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    clob = _FakeClobBalance(50.0)
    assert _planned_window_notional_usd(cfg, clob, pair_key="DOGE", window_minutes=5) == 50.0 * ALT_BALANCE_NOTIONAL_FRACTION


def test_planned_window_notional_uses_one_dollar_for_hourly_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    clob = _FakeClobBalance(500.0)
    assert _planned_window_notional_usd(cfg, clob, pair_key="BTC", window_minutes=60) == 1.0
    assert _planned_window_notional_usd(cfg, clob, pair_key="DOGE", window_minutes=240) == 1.0


def test_normalize_usdc_balance_converts_base_units() -> None:
    assert _normalize_usdc_balance(28_812_657) == 28.812657
    assert _normalize_usdc_balance("28812657") == 28.812657
    assert _normalize_usdc_balance(50.25) == 50.25


def test_window_elapsed_ready_blocks_early_window() -> None:
    now = datetime.now(timezone.utc)
    start = int(now.timestamp()) - 30
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=_contract(slug=f"btc-updown-5m-{start}"),
        window_minutes=5,
        rules=RULES_5M,
    )
    assert not _window_elapsed_ready(runner, now)


def test_window_elapsed_ready_allows_after_20_percent() -> None:
    now = datetime.now(timezone.utc)
    min_elapsed = int(5 * 60 * MIN_WINDOW_PROGRESS_FRACTION)
    start = int(now.timestamp()) - min_elapsed
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=_contract(slug=f"btc-updown-5m-{start}"),
        window_minutes=5,
        rules=RULES_5M,
    )
    assert _window_elapsed_ready(runner, now)


def test_tick_no_fire_before_min_window_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()

    start = int(datetime.now(timezone.utc).timestamp()) - 30
    runner = WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=_contract(slug=f"btc-updown-5m-{start}"),
        window_minutes=5,
        rules=RULES_5M,
    )
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    poly = _FakePoly(mid_up=0.15, mid_dn=0.85)
    bn = _FakeBinanceCombo(100_020.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert not runner.traded
