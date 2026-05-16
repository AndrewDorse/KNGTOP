"""Engine tick path with fakes (no Polymarket / Binance sockets)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from kngtop.clob_client import _normalize_usdc_balance
from kngtop.config import KngtopConfig
from kngtop.engine import (
    ALT_BALANCE_NOTIONAL_FRACTION,
    BALANCE_NOTIONAL_FRACTION,
    DISCOVERY_RETRY_SEC_WHEN_MISSING,
    DiscoveryState,
    MIN_WINDOW_PROGRESS_FRACTION,
    WindowRunner,
    _current_window_start_sec,
    _finalize_runner_window,
    _planned_window_notional_usd,
    _rule_notional_usd,
    _runner_matches_current_window,
    _run_iteration,
    _should_discover_contract,
    _tick_runner,
    _window_elapsed_ready,
)
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.strategy_params import RULES_5M


class _FakePoly:
    def __init__(self, mid_up: float | None, mid_dn: float | None) -> None:
        self._up = mid_up
        self._dn = mid_dn

    def mid_for(self, token_id: str, max_age_sec: float = 5.0) -> float | None:
        if token_id == "tid_up":
            return self._up
        if token_id == "tid_dn":
            return self._dn
        return None

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
        self.limit_calls = 0

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.market_calls.append((usdc, max_price))
        return {"ok": True}

    def limit_buy(self, token: TokenMarket, *, price: float, usdc: float):  # noqa: ANN201
        self.limit_calls += 1
        return {"ok": True}


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


def test_tick_fires_close_up_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=0.85), binance=_FakeBinanceCombo(99_999.0), clob=None, cfg=cfg)
    assert "close_buy_up" in runner.traded_rule_keys


def test_tick_fires_close_down_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    _tick_runner(runner, poly=_FakePoly(mid_up=0.85, mid_dn=0.25), binance=_FakeBinanceCombo(100_001.0), clob=None, cfg=cfg)
    assert "close_buy_down" in runner.traded_rule_keys


def test_tick_does_not_fire_when_spot_too_far_from_start(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=0.85), binance=_FakeBinanceCombo(100_101.0), clob=None, cfg=cfg)
    assert not runner.traded_rule_keys


def test_tick_fires_when_only_needed_pm_side_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=None), binance=_FakeBinanceCombo(99_999.0), clob=None, cfg=cfg)
    assert "close_buy_up" in runner.traded_rule_keys


def test_planned_window_notional_uses_balance_fraction(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=50.0) == 50.0 * BALANCE_NOTIONAL_FRACTION


def test_planned_window_notional_uses_five_percent_for_new_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    assert _planned_window_notional_usd(cfg, pair_key="DOGE", window_minutes=5, available_balance_usdc=50.0) == 50.0 * ALT_BALANCE_NOTIONAL_FRACTION


def test_planned_window_notional_has_one_dollar_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=5.0) == 1.0


def test_normalize_usdc_balance_converts_base_units() -> None:
    assert _normalize_usdc_balance(28_812_657) == 28.812657
    assert _normalize_usdc_balance("28812657") == 28.812657
    assert _normalize_usdc_balance(50.25) == 50.25


def test_window_elapsed_ready_blocks_early_window() -> None:
    now = datetime.now(timezone.utc)
    start = int(now.timestamp()) - 30
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    assert not _window_elapsed_ready(runner, now)


def test_window_elapsed_ready_allows_after_20_percent() -> None:
    now = datetime.now(timezone.utc)
    min_elapsed = int(5 * 60 * MIN_WINDOW_PROGRESS_FRACTION)
    start = int(now.timestamp()) - min_elapsed
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    assert _window_elapsed_ready(runner, now)


def test_tick_logs_signal_blocked_before_min_window_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 30
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0
    with patch("kngtop.engine._event") as event_mock:
        _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=0.85), binance=_FakeBinanceCombo(99_999.0), clob=None, cfg=cfg)
    assert not runner.traded_rule_keys
    assert any(call.args and call.args[0] == "SIGNAL_BLOCKED" for call in event_mock.call_args_list)


def test_rule_notional_uses_preplanned_window_size() -> None:
    runner = WindowRunner("BTC", "BTCUSDT", _contract(), 5, RULES_5M)
    runner.trade_notional_usd = 5.0
    runner.rule_notional_usd["close_buy_up"] = 1.25
    rule = next(rule for rule in RULES_5M if rule.key == "close_buy_up")
    assert _rule_notional_usd(rule, runner) == 1.25


def test_tick_executes_full_fak_with_rule_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 5.0
    runner.rule_notional_usd["close_buy_up"] = 1.0
    clob = _FakeClobExec(100.0)
    _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=0.85), binance=_FakeBinanceCombo(99_999.0), clob=clob, cfg=cfg)
    assert "close_buy_up" in runner.traded_rule_keys
    assert clob.market_calls == [(1.0, 0.27)]
    assert clob.limit_calls == 0


def test_tick_does_not_mark_rule_traded_on_buy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 5.0
    runner.rule_notional_usd["close_buy_up"] = 1.0

    class _FailingClob(_FakeClobBalance):
        def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
            raise RuntimeError("market failed")

    _tick_runner(runner, poly=_FakePoly(mid_up=0.25, mid_dn=0.85), binance=_FakeBinanceCombo(99_999.0), clob=_FailingClob(100.0), cfg=cfg)
    assert "close_buy_up" not in runner.traded_rule_keys
    assert runner.rule_retry_not_before["close_buy_up"] > 0.0


def test_tick_uses_best_ask_not_mid_for_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    start = int(datetime.now(timezone.utc).timestamp()) - 90
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = 1.0

    class _AskPoly(_FakePoly):
        def best_bid_ask_for(self, token_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            if token_id == "tid_up":
                return (0.01, 0.26)
            return None

    _tick_runner(runner, poly=_AskPoly(mid_up=0.25, mid_dn=None), binance=_FakeBinanceCombo(99_999.0), clob=None, cfg=cfg)
    assert "close_buy_up" not in runner.traded_rule_keys


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
    assert not _should_discover_contract(None, state, now_ts=now_ts, now_monotonic=100.0 + DISCOVERY_RETRY_SEC_WHEN_MISSING - 0.1, window_minutes=5)
    assert _should_discover_contract(None, state, now_ts=now_ts, now_monotonic=100.0 + DISCOVERY_RETRY_SEC_WHEN_MISSING, window_minutes=5)


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
        )
    assert contract.up.token_id in clob.prewarmed
    assert contract.down.token_id in clob.prewarmed
