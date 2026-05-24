"""BTC 5m spike-pair engine tick path with fakes."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from kngtop.config import KngtopConfig
from kngtop.engine import (
    DiscoveryState,
    WindowRunner,
    _candidate_window_starts,
    _compute_limit_buy_price,
    _current_window_start_sec,
    _run_iteration,
    _tick_runner,
)
from kngtop.gamma import ActiveContract, TokenMarket


class _FakePoly:
    def __init__(self, quotes: dict[str, tuple[float, float]] | None = None) -> None:
        self.quotes = quotes or {}
        self.asset_ids: list[str] = []

    def set_assets(self, asset_ids):  # noqa: ANN201
        self.asset_ids = list(asset_ids)

    def best_bid_ask_for(self, token_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        return self.quotes.get(token_id)


class _FakeBinanceCombo:
    def __init__(
        self,
        *,
        px: float = 100_000.0,
        current_px: float = 100_100.0,
        past_px: float = 100_000.0,
        volume_ratio: float = 2.0,
        symbol: str = "BTCUSDT",
    ) -> None:
        self._px = px
        self._sym = symbol
        self._current_px = current_px
        self._past_px = past_px
        self._volume_ratio = volume_ratio

    def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
        del max_age_sec
        if symbol.strip().upper() != self._sym:
            return None
        return self._px

    def price_then_now(self, symbol: str, lookback_sec: int, max_age_sec: float):  # noqa: ANN201
        del lookback_sec, max_age_sec
        if symbol.strip().upper() != self._sym:
            return None
        return (self._current_px, self._past_px)

    def current_volume_ratio(self, symbol: str, lookback_sec: int, max_age_sec: float) -> float | None:
        del lookback_sec, max_age_sec
        if symbol.strip().upper() != self._sym:
            return None
        return self._volume_ratio


class _FakeClob:
    def __init__(self) -> None:
        self.limit_calls: list[tuple[str, float, float]] = []
        self.cancelled: list[str] = []
        self.open_orders: list[dict[str, object]] = []
        self.prewarmed: list[str] = []
        self._next_id = 1

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float, post_only: bool = True):  # noqa: ANN201
        assert post_only is True
        order_id = f"ord{self._next_id}"
        self._next_id += 1
        self.limit_calls.append((token.token_id, price, shares))
        self.open_orders.append(
            {
                "id": order_id,
                "asset_id": token.token_id,
                "side": "BUY",
                "price": price,
                "original_size": shares,
                "size_matched": 0,
                "size_left": shares,
            }
        )
        return {"ok": True, "orderID": order_id}

    def cancel_order_by_id(self, order_id: str):  # noqa: ANN201
        self.cancelled.append(order_id)
        self.open_orders = [row for row in self.open_orders if str(row.get("id")) != str(order_id)]
        return {"ok": True}

    def get_open_orders(self) -> list[dict[str, object]]:
        return list(self.open_orders)

    def prewarm_market_metadata(self, token: TokenMarket) -> None:
        self.prewarmed.append(token.token_id)


def _contract(*, slug: str) -> ActiveContract:
    end = datetime.now(timezone.utc) + timedelta(minutes=30)
    return ActiveContract(
        slug=slug,
        question="q",
        end_time=end,
        up=TokenMarket("tid_up", "UP", "0.01", False),
        down=TokenMarket("tid_dn", "DOWN", "0.01", False),
    )


def _cfg(monkeypatch: pytest.MonkeyPatch, *, dry_run: bool = False) -> KngtopConfig:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true" if dry_run else "false")
    monkeypatch.setenv("KNGTOP_PAIRS", "BTC:BTCUSDT")
    return KngtopConfig.from_env()


def _runtime_cache(
    *,
    positions: list[dict[str, object]] | None = None,
    open_orders: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "btc_binance_symbol": "BTCUSDT",
        "reconcile_cache_at": time.perf_counter(),
        "reconcile_positions": positions or [],
        "reconcile_open_orders": open_orders or [],
    }


def test_compute_limit_buy_price_clamps_and_stays_below_ask() -> None:
    assert _compute_limit_buy_price(best_bid=0.40, best_ask=0.45) == pytest.approx(0.41)
    assert _compute_limit_buy_price(best_bid=0.64, best_ask=0.65) == pytest.approx(0.64)
    assert _compute_limit_buy_price(best_bid=0.10, best_ask=0.11) is None


def test_candidate_window_starts_adds_next_window_inside_20_seconds() -> None:
    current = 1_777_900_500
    now_ts = current + 281
    assert _candidate_window_starts(now_ts) == (current, current + 300)


def test_tick_places_trigger_ask_and_discounted_hedge_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)})
    binance = _FakeBinanceCombo(current_px=100_030.0, past_px=100_000.0, volume_ratio=2.1)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )

    assert clob.limit_calls == [
        ("tid_up", pytest.approx(0.55), pytest.approx(5.0)),
        ("tid_dn", pytest.approx(0.40), pytest.approx(5.0)),
    ]


def test_tick_boosts_smaller_side_size_when_imbalanced(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.47, 0.49), "tid_dn": (0.44, 0.46)})
    positions = [{"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 10, "avgPrice": 0.50}]
    binance = _FakeBinanceCombo(current_px=99_970.0, past_px=100_000.0, volume_ratio=2.2)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )

    assert clob.limit_calls == [
        ("tid_up", pytest.approx(0.43), pytest.approx(5.0)),
        ("tid_dn", pytest.approx(0.46), pytest.approx(10.0)),
    ]


def test_new_signal_cancels_hanging_order_before_restage(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    runner.last_signal_monotonic = time.perf_counter() - (cfg.pair_cooldown_sec + 1.0)
    runner.order_first_seen = {"ord1": time.perf_counter() - 20}
    clob = _FakeClob()
    open_orders = [
        {"id": "ord1", "asset_id": "tid_dn", "side": "BUY", "price": 0.40, "original_size": 5, "size_left": 5}
    ]
    positions = [{"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 5, "avgPrice": 0.55}]
    poly = _FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)})
    binance = _FakeBinanceCombo(current_px=100_030.0, past_px=100_000.0, volume_ratio=2.2)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions, open_orders=open_orders),
    )

    assert clob.cancelled == ["ord1"]
    assert clob.limit_calls == []


def test_tick_cancels_orders_after_30_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    runner.order_first_seen = {"ord1": time.perf_counter() - 31}
    clob = _FakeClob()
    open_orders = [{"id": "ord1", "asset_id": "tid_up", "side": "BUY", "price": 0.55, "original_size": 5, "size_left": 5}]
    poly = _FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)})
    binance = _FakeBinanceCombo(current_px=100_030.0, past_px=100_000.0, volume_ratio=2.1)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(open_orders=open_orders),
    )

    assert clob.cancelled == ["ord1"]
    assert clob.limit_calls == []


def test_tick_respects_max_shares_per_side(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)})
    positions = [{"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 15, "avgPrice": 0.50}]
    binance = _FakeBinanceCombo(current_px=99_970.0, past_px=100_000.0, volume_ratio=2.2)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )

    assert clob.limit_calls == []


def test_tick_skips_pair_when_avg_sum_cap_would_break(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.59, 0.61), "tid_dn": (0.39, 0.41)})
    binance = _FakeBinanceCombo(current_px=100_030.0, past_px=100_000.0, volume_ratio=2.1)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )

    assert clob.limit_calls == []


def test_tick_stops_new_buys_after_220_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 221
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()

    _tick_runner(
        runner,
        poly=_FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)}),
        binance=_FakeBinanceCombo(current_px=100_030.0, past_px=100_000.0, volume_ratio=2.1),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )

    assert clob.limit_calls == []


def test_tick_skips_without_spike_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.53, 0.55), "tid_dn": (0.44, 0.46)})
    binance = _FakeBinanceCombo(current_px=100_005.0, past_px=100_000.0, volume_ratio=1.2)

    _tick_runner(
        runner,
        poly=poly,
        binance=binance,
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )

    assert clob.limit_calls == []


def test_run_iteration_discovers_and_subscribes_current_and_next(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    clob = _FakeClob()
    poly = _FakePoly()
    discovery: dict[int, DiscoveryState] = {}
    runners: dict[int, WindowRunner] = {}
    runtime_state = {"btc_binance_symbol": "BTCUSDT"}
    current_start = _current_window_start_sec(int(datetime.now(timezone.utc).timestamp()), 5)
    fake_starts = (current_start, current_start + 300)

    def _discover(*, market_symbol: str, window_minutes: int, start_sec: int, timeout: float):  # noqa: ANN201
        del market_symbol, window_minutes, timeout
        return _contract(slug=f"btc-updown-5m-{start_sec}")

    with (
        patch("kngtop.engine._candidate_window_starts", return_value=fake_starts),
        patch("kngtop.engine.discover_updown_window_by_start", side_effect=_discover),
        patch("kngtop.engine.fetch_user_positions", return_value=[]),
    ):
        _run_iteration(
            cfg,
            runners=runners,
            discovery=discovery,
            subscribed_asset_ids=set(),
            poly=poly,
            binance=_FakeBinanceCombo(),
            clob=clob,
            runtime_state=runtime_state,
        )
    assert sorted(runners) == list(fake_starts)
    assert poly.asset_ids == ["tid_up", "tid_dn", "tid_up", "tid_dn"]
    assert clob.prewarmed.count("tid_up") == 2
    assert clob.prewarmed.count("tid_dn") == 2


def test_current_window_start_rounds_down() -> None:
    now_ts = 1_777_900_589
    assert _current_window_start_sec(now_ts, 5) == 1_777_900_500
