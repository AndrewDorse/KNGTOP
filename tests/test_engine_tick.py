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
    HEDGE_START_LIMIT_PRICE,
    HEDGE_TARGET_SUM,
    DiscoveryState,
    WindowRunner,
    _current_window_start_sec,
    _finalize_runner_window,
    _planned_window_notional_usd,
    _run_iteration,
    _runner_matches_current_window,
    _shares_for_budget,
    _should_discover_contract,
    _tick_runner,
)
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.strategy_params import RULES_15M, RULES_5M


class _FakePoly:
    def best_bid_ask_for(self, token_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
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
        self.limit_calls: list[tuple[str, float, float]] = []
        self.open_orders: dict[str, str] = {}
        self.cancelled: list[str] = []
        self._next_id = 1

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float):  # noqa: ANN201
        order_id = f"ord{self._next_id}"
        self._next_id += 1
        self.limit_calls.append((token.token_id, price, shares))
        self.open_orders[order_id] = token.token_id
        return {"ok": True, "orderID": order_id}

    def is_order_open_for_asset(self, token: TokenMarket, order_id: str) -> bool:
        return self.open_orders.get(order_id) == token.token_id

    def cancel_order_by_id(self, order_id: str):  # noqa: ANN201
        self.cancelled.append(order_id)
        self.open_orders.pop(order_id, None)
        return {"ok": True}

    def prewarm_market_metadata(self, token: TokenMarket) -> None:
        del token


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
    monkeypatch.setenv("KNGTOP_PAIRS", "BTC:BTCUSDT")
    return KngtopConfig.from_env()


def _runner_for_start(start_offset_sec: int) -> WindowRunner:
    start = int(datetime.now(timezone.utc).timestamp()) - start_offset_sec
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5, RULES_5M)
    runner.start_px = 100_000.0
    runner.trade_notional_usd = ENTRY_MIN_NOTIONAL_USD
    return runner


def test_tick_places_serial_hedge_starters(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    runner = _runner_for_start(10)
    clob = _FakeClobExec(100.0)
    _tick_runner(
        runner,
        poly=_FakePoly(),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    assert runner.starter_orders_posted is True
    assert len(clob.limit_calls) == 2
    assert clob.limit_calls[0] == ("tid_up", pytest.approx(HEDGE_START_LIMIT_PRICE), pytest.approx(10.41))
    assert clob.limit_calls[1] == ("tid_dn", pytest.approx(HEDGE_START_LIMIT_PRICE), pytest.approx(10.41))


def test_tick_places_hedge_after_first_fill(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    runner = _runner_for_start(10)
    clob = _FakeClobExec(100.0)
    _tick_runner(
        runner,
        poly=_FakePoly(),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    up_id = runner.starter_order_ids["UP"]
    clob.open_orders.pop(up_id, None)
    _tick_runner(
        runner,
        poly=_FakePoly(),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    assert runner.pair_started is True
    assert runner.hedge_order_side == "DOWN"
    assert runner.hedge_limit_price == pytest.approx(round(HEDGE_TARGET_SUM - HEDGE_START_LIMIT_PRICE, 2))
    assert runner.starter_order_ids["DOWN"] == ""
    assert len(clob.cancelled) == 1
    assert clob.limit_calls[-1] == ("tid_dn", pytest.approx(0.56), pytest.approx(10.41))


def test_tick_marks_pair_completed_when_both_starters_fill(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    runner = _runner_for_start(10)
    clob = _FakeClobExec(100.0)
    _tick_runner(
        runner,
        poly=_FakePoly(),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    clob.open_orders.clear()
    _tick_runner(
        runner,
        poly=_FakePoly(),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state={},
    )
    assert runner.pair_completed is True
    assert runner.hedge_order_id is None


def test_planned_window_notional_clamps_to_fraction_min_and_max(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=0.99) == 0.0
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=1.0) == 0.0
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=1.25) == pytest.approx(1.25)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=100.0) == pytest.approx(100.0 * ENTRY_BALANCE_FRACTION)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=5, available_balance_usdc=10_000.0) == pytest.approx(ENTRY_MAX_NOTIONAL_USD)
    assert _planned_window_notional_usd(cfg, pair_key="BTC", window_minutes=15, available_balance_usdc=10.0) == 0.0


def test_shares_for_budget_is_quantized() -> None:
    shares, cost = _shares_for_budget(budget_usd=1.25, limit_price=0.12)
    assert shares == pytest.approx(10.41)
    assert cost == pytest.approx(1.2492)


def test_normalize_usdc_balance_converts_base_units() -> None:
    assert _normalize_usdc_balance(28_812_657) == 28.812657
    assert _normalize_usdc_balance("28812657") == 28.812657
    assert _normalize_usdc_balance(50.25) == 50.25


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


def test_finalize_runner_window_skips_serial_hedge(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=True)
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug="btc-updown-5m-1777900200"), 5, RULES_5M)
    runner.start_px = 100_000.0
    with patch("kngtop.engine._event") as event_mock:
        _finalize_runner_window(runner, binance=_FakeBinanceCombo(100_010.0, symbol="BTCUSDT"), cfg=cfg)
    event_mock.assert_not_called()


def test_run_iteration_prewarms_token_metadata_for_new_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)

    class _FakeFeed:
        def set_assets(self, asset_ids):  # noqa: ANN201
            self.asset_ids = list(asset_ids)

        def best_bid_ask_for(self, token_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            return None

    class _FakeBinance:
        def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
            return 100_000.0 if symbol == "BTCUSDT" else None

    start = _current_window_start_sec(int(datetime.now(timezone.utc).timestamp()), 5)
    contract = _contract(slug=f"btc-updown-5m-{start}")
    clob = _FakeClobExec(100.0)
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
    assert runners[("BTC", 5)] is not None
    assert runners[("BTC", 5)].trade_notional_usd == pytest.approx(5.0)
    assert runners[("BTC", 15)] is not None
    assert runners[("BTC", 15)].trade_notional_usd == 0.0


def test_15m_rules_are_empty_for_runner() -> None:
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug="btc-updown-15m-1777900500"), 15, RULES_15M)
    assert len(runner.rules) == 0
