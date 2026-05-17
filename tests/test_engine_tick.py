"""Engine tick path with fakes (no Polymarket / Binance sockets)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from kngtop.clob_client import _normalize_usdc_balance
from kngtop.config import KngtopConfig
from kngtop.engine import (
    DISCOVERY_RETRY_SEC_WHEN_MISSING,
    ENTRY_BALANCE_FRACTION,
    ENTRY_MAX_NOTIONAL_USD,
    ENTRY_MIN_NOTIONAL_USD,
    DiscoveryState,
    WindowRunner,
    _current_window_start_sec,
    _finalize_runner_window,
    _planned_window_notional_usd,
    _run_iteration,
    _runner_matches_current_window,
    _should_discover_contract,
    _tick_runner,
    _window_elapsed_ready,
)
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.strategy_params import MIN_ELAPSED_SEC, RULES_5M


class _FakePoly:
    def __init__(self, ask_up: float | None, ask_dn: float | None) -> None:
        self._up = ask_up
        self._dn = ask_dn

    def best_bid_ask_for(self, token_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        if token_id == "tid_up" and self._up is not None:
            return (max(0.0, self._up - 0.01), self._up)
        if token_id == "tid_dn" and self._dn is not None:
            return (max(0.0, self._dn - 0.01), self._dn)
        return None


class _FakeBinanceCombo:
    def __init__(self, px: float, symbol: str = "BTCUSDT") -> None:
        self._px = px
        self._sym = symbol

    def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
        if symbol.strip().upper() != self._sym:
            return None
        return self._px


class _FakeClobBalance:
    def __init__(self, balance: float | None) -> None:
        self._balance = balance

    def available_balance_usdc(self) -> float | None:
        return self._balance


class _FakeClobExec(_FakeClobBalance):
    def __init__(self, balance: float | None) -> None:
        super().__init__(balance)
        self.market_calls: list[tuple[float, float | None]] = []

    def market_buy_usdc(self, token: TokenMarket, *, usdc: float, max_price: float | None = None):  # noqa: ANN201
        self.market_calls.append((usdc, max_price))
        return {"ok": True, "orderID": "buy123"}


class _FakeClobPrewarm(_FakeClobBalance):
    def __init__(self, balance: float | None) -> None:
        super().__init__(balance)
        self.prewarmed: list[str] = []

    def prewarm_market_metadata(self, token: TokenMarket) -> None:
        self.prewarmed.append(token.token_id)


def _contract(*, slug: str = "btc-updown-5m-1777900500") -> ActiveContract:
    end = datetime.now(timezone.utc) + timedelta(minutes=30)
    return ActiveContract(
        slug=slug,
        question="q",
        end_time=end,
        up=TokenMarket("tid_up", "UP", "0.01", False),
        down=TokenMarket("tid_dn", "DOWN", "0.01", False),
    )


def _cfg(monkeypatch: pytest.MonkeyPatch, *, dry_run: bool = True) -> KngtopConfig:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true" if dry_run else "false")
    monkeypatch.setenv("KNGTOP_ORDER_RETRY_ON_ERROR", "2")
    return KngtopConfig.from_env()


def test_tick_fires_winning_up_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.30, ask_dn=0.70),
        binance=_FakeBinanceCombo(100_001.0),
        clob=None,
        cfg=cfg,
        runtime_state={},
    )
    assert "close_buy_up" in runner.traded_rule_keys


def test_tick_fires_winning_down_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.70, ask_dn=0.30),
        binance=_FakeBinanceCombo(99_999.0),
        clob=None,
        cfg=cfg,
        runtime_state={},
    )
    assert "close_buy_down" in runner.traded_rule_keys


def test_tick_does_not_fire_when_spot_far_from_start(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.30, ask_dn=0.70),
        binance=_FakeBinanceCombo(100_300.0),
        clob=None,
        cfg=cfg,
        runtime_state={},
    )
    assert not runner.traded_rule_keys


def test_tick_does_not_fire_when_price_outside_band(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.45, ask_dn=0.70),
        binance=_FakeBinanceCombo(100_001.0),
        clob=None,
        cfg=cfg,
        runtime_state={},
    )
    assert not runner.traded_rule_keys


def test_planned_window_notional_clamps_to_fraction_min_and_max(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=0.99) == 0.0
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=10.0) == pytest.approx(ENTRY_MIN_NOTIONAL_USD)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=100.0) == pytest.approx(100.0 * ENTRY_BALANCE_FRACTION)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=10_000.0) == pytest.approx(ENTRY_MAX_NOTIONAL_USD)
    assert _planned_window_notional_usd(cfg, pair_key="ETH", window_minutes=5, available_balance_usdc=10.0) == 0.0


def test_normalize_usdc_balance_converts_base_units() -> None:
    assert _normalize_usdc_balance(28_812_657) == 28.812657
    assert _normalize_usdc_balance("28812657") == 28.812657
    assert _normalize_usdc_balance(50.25) == 50.25


def test_window_elapsed_ready_blocks_early_window() -> None:
    now = datetime.now(timezone.utc)
    start = int(now.timestamp()) - (MIN_ELAPSED_SEC - 1)
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    assert not _window_elapsed_ready(runner, now)


def test_window_elapsed_ready_allows_after_rule_min_elapsed() -> None:
    now = datetime.now(timezone.utc)
    start = int(now.timestamp()) - MIN_ELAPSED_SEC
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    assert _window_elapsed_ready(runner, now)


def test_tick_executes_first_leg_only(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    clob = _FakeClobExec(100.0)
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.30, ask_dn=0.70),
        binance=_FakeBinanceCombo(100_001.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    assert "close_buy_up" in runner.traded_rule_keys
    assert clob.market_calls == [(ENTRY_MIN_NOTIONAL_USD, 0.44)]


def test_tick_trades_only_once_per_window(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    runner.traded_rule_keys.add("close_buy_up")
    clob = _FakeClobExec(100.0)
    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.30, ask_dn=0.70),
        binance=_FakeBinanceCombo(99_999.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    assert clob.market_calls == []


def test_tick_does_not_mark_rule_traded_on_buy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD

    class _FailingClob(_FakeClobBalance):
        def market_buy_usdc(self, token: TokenMarket, *, usdc: float, max_price: float | None = None):  # noqa: ANN201
            raise RuntimeError("market failed")

    _tick_runner(
        runner,
        poly=_FakePoly(ask_up=0.30, ask_dn=0.70),
        binance=_FakeBinanceCombo(100_001.0),
        clob=_FailingClob(100.0),
        cfg=cfg,
        runtime_state={},
    )
    assert "close_buy_up" not in runner.traded_rule_keys
    assert runner.rule_retry_not_before["close_buy_up"] > 0.0


def test_runner_matches_current_window() -> None:
    now_ts = 1_777_900_589
    start = _current_window_start_sec(now_ts, 5)
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    assert _runner_matches_current_window(runner, now_ts=now_ts, window_minutes=5)


def test_should_not_rediscover_when_runner_matches_current_window() -> None:
    now_ts = 1_777_900_589
    now_mono = 100.0
    start = _current_window_start_sec(now_ts, 5)
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    state = DiscoveryState(last_window_start_sec=start, last_checked_monotonic=95.0)
    assert not _should_discover_contract(runner, state, now_ts=now_ts, now_monotonic=now_mono, window_minutes=5)


def test_should_retry_missing_discovery_only_after_interval() -> None:
    now_ts = 1_777_900_589
    start = _current_window_start_sec(now_ts, 5)
    state = DiscoveryState(last_window_start_sec=start, last_checked_monotonic=100.0)
    assert not _should_discover_contract(
        None,
        state,
        now_ts=now_ts,
        now_monotonic=100.0 + DISCOVERY_RETRY_SEC_WHEN_MISSING - 0.1,
        window_minutes=5,
    )
    assert _should_discover_contract(
        None,
        state,
        now_ts=now_ts,
        now_monotonic=100.0 + DISCOVERY_RETRY_SEC_WHEN_MISSING,
        window_minutes=5,
    )


def test_should_discover_on_new_window_even_with_previous_runner() -> None:
    now_ts = 1_777_900_589
    current_start = _current_window_start_sec(now_ts, 5)
    previous_start = current_start - 300
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{previous_start}"), 5, RULES_5M)
    state = DiscoveryState(last_window_start_sec=previous_start, last_checked_monotonic=50.0)
    assert _should_discover_contract(runner, state, now_ts=now_ts, now_monotonic=55.0, window_minutes=5)


def test_finalize_runner_window_logs_result(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    runner = WindowRunner("BTC", "BTCUSDT", _contract(), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.executed_rule_sides["close_buy_up"] = "UP"
    with patch("kngtop.engine._event") as event_mock:
        _finalize_runner_window(runner, binance=_FakeBinanceCombo(100_010.0), cfg=cfg)
    event_mock.assert_called_once()
    assert event_mock.call_args.args[0] == "DEAL_WINDOW_CLOSED"
    assert event_mock.call_args.kwargs["result"] == "RIGHT"


def test_run_iteration_prewarms_token_metadata_for_new_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)

    class _FakeFeed:
        def set_assets(self, asset_ids):  # noqa: ANN201
            self.asset_ids = list(asset_ids)

    class _FakeBinance:
        def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
            return None

    start = _current_window_start_sec(int(datetime.now(timezone.utc).timestamp()), 5)
    contract = _contract(slug=f"btc-updown-5m-{start}")
    clob = _FakeClobPrewarm(100.0)
    with (
        patch("kngtop.engine.discover_active_btc_window", return_value=contract),
        patch.object(WindowRunner, "refresh_start_px", lambda self, cfg: setattr(self, "start_px", 100_000.0)),
    ):
        runners: dict[tuple[str, int], WindowRunner | None] = {}
        discovery: dict[tuple[str, int], DiscoveryState] = {}
        subscribed: set[str] = set()
        _run_iteration(
            cfg,
            runners=runners,
            discovery=discovery,
            subscribed_asset_ids=subscribed,
            poly=_FakeFeed(),
            binance=_FakeBinance(),
            clob=clob,
            runtime_state={},
        )
    assert contract.up.token_id in clob.prewarmed
    assert contract.down.token_id in clob.prewarmed
    assert runners[("BTC", 5)] is not None
    assert runners[("BTC", 5)].trade_notional_usd == pytest.approx(5.0)
