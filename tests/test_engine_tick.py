"""Balanced BTC 5m maker tick path with fakes."""

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
    def __init__(self, px: float, symbol: str = "BTCUSDT") -> None:
        self._px = px
        self._sym = symbol

    def last_price(self, symbol: str, max_age_sec: float = 6.0) -> float | None:
        del max_age_sec
        if symbol.strip().upper() != self._sym:
            return None
        return self._px


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


def _runtime_cache(*, positions: list[dict[str, object]] | None = None, open_orders: list[dict[str, object]] | None = None) -> dict[str, object]:
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


def test_tick_places_one_buy_per_side_when_flat(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.48)})
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )
    assert clob.limit_calls == [
        ("tid_up", pytest.approx(0.41), pytest.approx(5.0)),
        ("tid_dn", pytest.approx(0.43), pytest.approx(5.0)),
    ]


def test_tick_only_places_smaller_side_when_imbalanced(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.48)})
    positions = [{"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 5, "avgPrice": 0.44}]
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )
    assert clob.limit_calls == [("tid_dn", pytest.approx(0.43), pytest.approx(5.0))]


def test_tick_allows_second_pair_only_for_strong_first_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.50, 0.58), "tid_dn": (0.52, 0.60)})
    positions = [
        {"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 5, "avgPrice": 0.51},
        {"slug": runner.contract.slug, "asset": "tid_dn", "outcome": "DOWN", "size": 5, "avgPrice": 0.52},
    ]
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )
    assert runner.second_pair_gate_state == "allowed"
    assert runner.stopped is False
    assert clob.limit_calls == [
        ("tid_up", pytest.approx(0.51), pytest.approx(5.0)),
        ("tid_dn", pytest.approx(0.53), pytest.approx(5.0)),
    ]


def test_tick_blocks_second_pair_and_stops_window(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.50, 0.54), "tid_dn": (0.52, 0.56)})
    positions = [
        {"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 5, "avgPrice": 0.51},
        {"slug": runner.contract.slug, "asset": "tid_dn", "outcome": "DOWN", "size": 5, "avgPrice": 0.52},
    ]
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )
    assert runner.second_pair_gate_state == "blocked"
    assert runner.stopped is True
    assert runner.stop_reason == "second_pair_blocked"
    assert clob.limit_calls == []


def test_tick_respects_max_pairs_per_side(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.48)})
    positions = [{"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 10, "avgPrice": 0.44}]
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )
    assert clob.limit_calls == [("tid_dn", pytest.approx(0.43), pytest.approx(5.0))]
    clob.limit_calls.clear()
    positions = [
        {"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 10, "avgPrice": 0.44},
        {"slug": runner.contract.slug, "asset": "tid_dn", "outcome": "DOWN", "size": 10, "avgPrice": 0.45},
    ]
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions),
    )
    assert clob.limit_calls == []


def test_tick_positive_gross_lock_cancels_buys(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    runner.order_first_seen = {"ord1": time.perf_counter() - 10, "ord2": time.perf_counter() - 10}
    clob = _FakeClob()
    open_orders = [
        {"id": "ord1", "asset_id": "tid_up", "side": "BUY", "price": 0.41, "original_size": 5, "size_left": 5},
        {"id": "ord2", "asset_id": "tid_dn", "side": "BUY", "price": 0.43, "original_size": 5, "size_left": 5},
    ]
    positions = [
        {"slug": runner.contract.slug, "asset": "tid_up", "outcome": "UP", "size": 10, "avgPrice": 0.45},
        {"slug": runner.contract.slug, "asset": "tid_dn", "outcome": "DOWN", "size": 10, "avgPrice": 0.45},
    ]
    _tick_runner(
        runner,
        poly=_FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.47)}),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(positions=positions, open_orders=open_orders),
    )
    assert runner.stopped is True
    assert runner.stop_reason == "positive_gross_lock"
    assert sorted(clob.cancelled) == ["ord1", "ord2"]


def test_tick_cancels_orders_after_240_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 241
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    runner.order_first_seen = {"ord1": time.perf_counter() - 10}
    clob = _FakeClob()
    open_orders = [{"id": "ord1", "asset_id": "tid_up", "side": "BUY", "price": 0.41, "original_size": 5, "size_left": 5}]
    _tick_runner(
        runner,
        poly=_FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.47)}),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(open_orders=open_orders),
    )
    assert clob.cancelled == ["ord1"]
    assert clob.limit_calls == []


def test_tick_stops_new_buys_after_220_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 221
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    _tick_runner(
        runner,
        poly=_FakePoly({"tid_up": (0.40, 0.45), "tid_dn": (0.42, 0.47)}),
        binance=_FakeBinanceCombo(100_000.0),
        clob=clob,
        cfg=cfg,
        runtime_state=_runtime_cache(),
    )
    assert clob.limit_calls == []


def test_tick_skips_side_when_maker_edge_too_small(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    start = int(datetime.now(timezone.utc).timestamp()) - 60
    runner = WindowRunner("BTC", "BTCUSDT", _contract(slug=f"btc-updown-5m-{start}"), 5)
    clob = _FakeClob()
    poly = _FakePoly({"tid_up": (0.48, 0.50), "tid_dn": (0.48, 0.50)})
    _tick_runner(
        runner,
        poly=poly,
        binance=_FakeBinanceCombo(100_000.0),
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
            binance=_FakeBinanceCombo(100_000.0),
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
