from __future__ import annotations

from datetime import datetime, timezone
import inspect
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
import kngtop.live_kilemo2 as live_kilemo2
from kngtop.live_kilemo2 import (
    ORDER_IN_FLIGHT,
    PositionState,
    WindowRunner,
    _avg_sum,
    _choose_active_repair_side,
    _order_amount_usd,
    _tick_runner,
)


def _cfg() -> KngtopConfig:
    return KngtopConfig(
        private_key="pk",
        funder="0xabc",
        signature_type=1,
        relayer_api_key="",
        relayer_secret="",
        relayer_passphrase="",
        dry_run=False,
        poll_interval_sec=0.2,
        eval_debounce_sec=0.0,
        request_timeout_sec=5.0,
        notional_usd=2.0,
        trading_pairs=(("BTC", "BTCUSDT"),),
        log_level="INFO",
        order_cutoff_remaining_sec=20.0,
        order_retry_on_error=0,
        market_buy_max_price=0.90,
        binance_max_age_sec=6.0,
        poly_mid_max_age_sec=5.0,
        ws_rest_poll_enabled=False,
        ws_rest_poll_interval_sec=1.0,
        hedge_max_orders_per_side=2,
    )


def _runner(start: int, *, positions: PositionState | None = None) -> WindowRunner:
    return WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=ActiveContract(
            slug=f"btc-updown-5m-{start}",
            question="",
            end_time=datetime.fromtimestamp(start + 300, timezone.utc),
            up=TokenMarket(token_id="up-token", outcome="UP", minimum_tick_size="0.01", neg_risk=False),
            down=TokenMarket(token_id="down-token", outcome="DOWN", minimum_tick_size="0.01", neg_risk=False),
        ),
        window_minutes=5,
        window_open_px=100_000.0,
        positions=positions if positions is not None else PositionState(),
    )


class _FakeClob:
    def __init__(self, responses: list[object] | None = None) -> None:
        self.calls: list[tuple[str, float, float]] = []
        self.responses = list(responses or [])

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.calls.append((token.token_id, usdc, 0.0 if max_price is None else max_price))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        shares = usdc / max(0.01, float(max_price or 0.5))
        return {"orderID": f"buy-{len(self.calls)}", "size_matched": shares}


class _FakeBinance:
    def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        return 100_000.0


def test_tick_runner_bootstraps_cheaper_side_before_15s() -> None:
    runner = _runner(1_700_000_000)
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.49, 0.52) if asset_id == "up-token" else (0.58, 0.60)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_010, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 2.0, 0.52)]
    assert runner.positions.orders_up == 1
    assert runner.positions.orders_down == 0


def test_initial_bootstrap_uses_2_usd() -> None:
    runner = _runner(1_700_000_000)
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.49, 0.52) if asset_id == "up-token" else (0.58, 0.60)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_010, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls[0][1] == 2.0


def test_tick_runner_forces_opposite_bootstrap_side_at_15s() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_up=1.0, shares_up=2.0, orders_up=1, total_deals=1))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.49, 0.51) if asset_id == "up-token" else (0.63, 0.66)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_020, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("down-token", 1.0, 0.66)]
    assert runner.positions.orders_down == 1
    assert runner.positions.both_sides_traded()


def test_tick_runner_active_repair_buys_smaller_side_on_imbalance() -> None:
    runner = _runner(
        1_700_000_000,
        positions=PositionState(
            spent_up=1.0,
            shares_up=2.0,
            spent_down=3.0,
            shares_down=8.0,
            orders_up=1,
            orders_down=3,
            total_deals=4,
        ),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.24, 0.26) if asset_id == "up-token" else (0.69, 0.71)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 2.0, 0.26)]
    assert runner.positions.orders_up == 2


def test_later_buy_uses_1_usd_default() -> None:
    cfg = _cfg()
    state = PositionState(spent_up=2.0, shares_up=4.0, spent_down=2.0, shares_down=4.0, orders_up=1, orders_down=1, total_deals=2)
    assert _order_amount_usd(ask_px=0.45, state=state, cfg=cfg) == 1.0


def test_later_buy_uses_2_usd_only_when_cheap_or_imbalanced() -> None:
    cfg = _cfg()
    balanced = PositionState(spent_up=2.0, shares_up=4.0, spent_down=2.0, shares_down=4.0, orders_up=1, orders_down=1, total_deals=2)
    imbalanced = PositionState(spent_up=2.0, shares_up=2.0, spent_down=2.0, shares_down=8.0, orders_up=1, orders_down=1, total_deals=2)
    assert _order_amount_usd(ask_px=0.30, state=balanced, cfg=cfg) == 2.0
    assert _order_amount_usd(ask_px=0.45, state=imbalanced, cfg=cfg) == 2.0
    assert _order_amount_usd(ask_px=0.45, state=balanced, cfg=cfg) == 1.0


def test_tick_runner_rescue_60_cap080_buys_missing_side() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_up=0.10, shares_up=2.0, orders_up=1, total_deals=1))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.50, 0.52) if asset_id == "up-token" else (0.74, 0.79)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_240, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("down-token", 1.0, 0.79)]
    assert runner.positions.orders_down == 1
    assert runner.positions.both_sides_traded()


def test_tick_runner_rescue_skips_missing_side_above_cap() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_up=0.10, shares_up=2.0, orders_up=1, total_deals=1))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.50, 0.52) if asset_id == "up-token" else (0.82, 0.85)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_240, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []
    assert runner.positions.orders_down == 0


def test_bootstrap_opposite_fak_failure_does_not_mark_side_open() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]")
        ]
    )
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.68, 0.70) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 1.0, 0.7)]
    assert runner.positions.orders_up == 0
    assert runner.positions.shares_up == 0.0
    assert not runner.positions.both_sides_traded()


def test_repeated_nofill_cannot_send_same_bootstrap_2usd_every_second() -> None:
    runner = _runner(1_700_000_000)
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]"),
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]"),
        ]
    )
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.49, 0.50) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_001, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_002, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_003, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 2.0, 0.5)]


def test_missing_side_retries_after_fak_failure() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]"),
            {"orderID": "buy-2", "size_matched": 2.08},
        ]
    )
    cfg = _cfg()

    class _Poly:
        asks = [0.70, 0.48]
        call = 0

        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            if asset_id == "up-token":
                value = self.asks[min(self.call, len(self.asks) - 1)]
                self.call += 1
                return (max(0.01, value - 0.01), value)
            return (0.49, 0.50)

    poly = _Poly()
    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_041, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 1.0, 0.7), ("up-token", 1.0, 0.48)]
    assert runner.positions.orders_up == 1
    assert runner.positions.shares_up > 0.0
    assert runner.positions.both_sides_traded()


def test_active_repair_blocked_until_both_sides_have_real_shares() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=3.0, shares_down=8.0, orders_down=3, total_deals=3))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.24, 0.26) if asset_id == "up-token" else (0.69, 0.71)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 2.0, 0.26)]
    assert runner.positions.orders_up == 1


def test_missing_side_bought_when_price_becomes_cheap_after_failed_bootstrap() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]"),
            {"orderID": "buy-2", "size_matched": 2.08},
        ]
    )
    cfg = _cfg()

    class _Poly:
        asks = [0.70, 0.48]
        call = 0

        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            if asset_id == "up-token":
                value = self.asks[min(self.call, len(self.asks) - 1)]
                self.call += 1
                return (max(0.01, value - 0.01), value)
            return (0.49, 0.50)

    poly = _Poly()
    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_041, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.orders_up == 1
    assert runner.positions.shares_up > 0.0
    assert runner.positions.both_sides_traded()


def test_failed_buy_does_not_increment_order_count_or_spent() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]")
        ]
    )
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.68, 0.70) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.total_deals == 1
    assert runner.positions.orders_up == 0
    assert runner.positions.spent_up == 0.0


def test_window_budget_never_exceeds_20() -> None:
    runner = _runner(
        1_700_000_000,
        positions=PositionState(
            spent_up=10.0,
            shares_up=20.0,
            spent_down=10.0,
            shares_down=20.0,
            orders_up=5,
            orders_down=5,
            total_deals=10,
        ),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.24, 0.26) if asset_id == "up-token" else (0.69, 0.71)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []
    assert runner.positions.spent_total() <= 20.0 + 1e-12


def test_no_more_than_one_order_per_tick() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.44, 0.45)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_240, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert len(fake_clob.calls) == 1


def test_pending_order_blocks_new_buy() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    runner.pending_order = True
    runner.pending_side = "DOWN"
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.44, 0.45)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_240, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []


def test_no_second_order_while_intent_open() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    runner.pending_order = True
    runner.pending_side = "UP"
    runner.pending_reason = "bootstrap_opposite"
    runner.execution_state = ORDER_IN_FLIGHT
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.44, 0.45)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_240, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []


def test_failed_order_sets_ready_and_retries_after_1s() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(
        responses=[
            Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]"),
            {"orderID": "buy-2", "size_matched": 2.08},
        ]
    )
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        assert runner.execution_state != ORDER_IN_FLIGHT
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_034, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_031, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 1.0, 0.48), ("up-token", 1.0, 0.48)]
    assert runner.positions.orders_up == 1


def test_100_ticks_in_1_second_produce_max_1_buy_attempt() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[Exception("PolyApiException[status_code=400, error_message={'error': 'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.'}]")])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        for _ in range(100):
            fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
            _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 1.0, 0.48)]


def test_position_state_updates_only_from_real_fill() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.orders_up == 0
    assert runner.positions.shares_up == 0.0
    assert runner.positions.spent_up == 0.0


def test_no_state_update_on_nofill() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.total_deals == 1
    assert runner.positions.spent_total() == 1.0


def test_no_state_update_on_failed_order() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[Exception("boom")])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.total_deals == 1
    assert runner.positions.spent_total() == 1.0


def test_after_send_order_next_tick_waits_for_result() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    runner.pending_order = True
    runner.execution_state = ORDER_IN_FLIGHT
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []


def test_order_count_changes_only_after_real_fill() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}, {"orderID": "buy-2", "size_matched": 2.0}])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_031, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.orders_up == 1
    assert runner.positions.total_deals == 2


def test_order_count_only_after_fill() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=1.0, shares_down=2.0, orders_down=1, total_deals=1))
    fake_clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}])
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.47, 0.48) if asset_id == "up-token" else (0.49, 0.50)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_030, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert runner.positions.orders_up == 0


def test_no_cooldown_logs_exist() -> None:
    source = inspect.getsource(live_kilemo2)
    assert "BUY SKIP COOLDOWN" not in source
    assert "buy_cooldown_until_ts" not in source


def test_active_repair_requires_both_sides_filled() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_down=3.0, shares_down=8.0, orders_down=3, total_deals=3))
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.24, 0.26) if asset_id == "up-token" else (0.40, 0.42)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls[0][0] == "up-token"


def test_missing_side_priority_before_active_repair() -> None:
    runner = _runner(
        1_700_000_000,
        positions=PositionState(
            spent_up=2.0,
            shares_up=4.0,
            spent_down=0.0,
            shares_down=0.0,
            orders_up=1,
            orders_down=0,
            total_deals=1,
        ),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.20, 0.22) if asset_id == "up-token" else (0.64, 0.66)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("down-token", 1.0, 0.66)]


def test_strategy_matches_backtest_decision_sequence_on_sample_window() -> None:
    runner = _runner(1_700_000_000)
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        quotes = {
            10: {"up-token": (0.49, 0.52), "down-token": (0.58, 0.60)},
            20: {"up-token": (0.50, 0.51), "down-token": (0.63, 0.66)},
            90: {"up-token": (0.24, 0.26), "down-token": (0.69, 0.71)},
        }

        def __init__(self) -> None:
            self.current = 10

        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return self.quotes[self.current][asset_id]

    poly = _Poly()
    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_010, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
        poly.current = 20
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_020, timezone.utc)
        _tick_runner(runner, poly=poly, binance=_FakeBinance(), clob=fake_clob, cfg=cfg)
    side = _choose_active_repair_side(runner, up_ask=0.26, down_ask=0.71, remaining=210.0)

    assert fake_clob.calls[:2] == [
        ("up-token", 2.0, 0.52),
        ("down-token", 1.0, 0.66),
    ]
    assert side == "UP"


def test_after_both_sides_open_active_repair_continues_on_price_swings() -> None:
    runner = _runner(
        1_700_000_000,
        positions=PositionState(spent_up=2.0, shares_up=4.0, spent_down=1.0, shares_down=1.5151515, orders_up=1, orders_down=1, total_deals=2),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.00, 0.01) if asset_id == "up-token" else (0.79, 0.80)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 2.0, 0.01)]


def test_bot_does_not_stop_at_avg_sum_0985_if_side_gets_cheap() -> None:
    up_shares = 2.0 / 0.50
    down_shares = 2.0 / 0.485
    runner = _runner(
        1_700_000_000,
        positions=PositionState(spent_up=2.0, shares_up=up_shares, spent_down=2.0, shares_down=down_shares, orders_up=1, orders_down=1, total_deals=2),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()
    assert _avg_sum(runner.positions) > 0.98

    class _Poly:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            return (0.33, 0.35) if asset_id == "up-token" else (0.79, 0.80)

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_090, timezone.utc)
        _tick_runner(runner, poly=_Poly(), binance=_FakeBinance(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("up-token", 1.0, 0.35)]
