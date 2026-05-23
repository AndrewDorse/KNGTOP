from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_kilemo2 import ORDER_IN_FLIGHT, PositionState, WindowRunner, _choose_guarded_pnl_buy, _effective_state_with_pending, _tick_runner


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
        max_shares_per_side=15.0,
        max_share_gap=2.0,
        repair_avg_sum_cap=0.95,
        locked_profit_roi=0.10,
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
        self.open_order_ids: set[str] = set()
        self.responses = list(responses or [])

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.calls.append((token.token_id, usdc, 0.0 if max_price is None else max_price))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return {"orderID": f"buy-{len(self.calls)}", "size_matched": usdc / max(0.01, float(max_price or 0.5))}

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float, post_only: bool = True):  # noqa: ANN201
        del post_only
        usdc = shares * price
        self.calls.append((token.token_id, usdc, price))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            order_id = str(response.get("orderID") or response.get("orderId") or response.get("id") or f"limit-{len(self.calls)}") if isinstance(response, dict) else f"limit-{len(self.calls)}"
        else:
            order_id = f"limit-{len(self.calls)}"
        self.open_order_ids.add(order_id)
        return {"orderID": order_id, "success": True}

    def is_order_open_for_asset(self, token: TokenMarket, order_id: str) -> bool:  # noqa: ANN201
        del token
        return str(order_id) in self.open_order_ids


class _FakeBinance:
    def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        return 100_000.0


class _Poly:
    def __init__(self, *, up: float, down: float) -> None:
        self.up = up
        self.down = down

    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        ask = self.up if asset_id == "up-token" else self.down
        return (max(0.0, ask - 0.01), ask)


def _positions_row(*, slug: str, outcome: str, token_id: str, size: float, avg_price: float) -> dict[str, object]:
    return {
        "slug": slug,
        "outcome": outcome,
        "asset": token_id,
        "size": size,
        "avgPrice": avg_price,
    }


def _tick(
    runner: WindowRunner,
    *,
    elapsed: int,
    up: float,
    down: float,
    clob: _FakeClob | None = None,
    positions_seq: list[list[dict[str, object]]] | None = None,
) -> _FakeClob:
    fake_clob = clob or _FakeClob()
    seq = list(positions_seq or [[]])

    def _fake_positions(*, user: str, timeout: float, limit: int = 500):  # noqa: ANN202
        del user, timeout, limit
        if len(seq) > 1:
            return seq.pop(0)
        return seq[0]

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_000 + elapsed, timezone.utc)
        with patch("kngtop.live_kilemo2.fetch_user_positions", side_effect=_fake_positions):
            _tick_runner(runner, poly=_Poly(up=up, down=down), binance=_FakeBinance(), clob=fake_clob, cfg=_cfg())
    return fake_clob


def test_initial_buy_is_2_usd_lower_ask_under_bootstrap_cap() -> None:
    runner = _runner(1_700_000_000)
    clob = _tick(
        runner,
        elapsed=0,
        up=0.52,
        down=0.49,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.081632653, avg_price=0.49)],
        ],
    )

    assert clob.calls == [("down-token", 2.4, 0.48)]
    assert runner.positions.orders_down == 1
    assert abs(runner.positions.spent_total() - 2.0) < 1e-6


def test_initial_buy_skips_when_lower_ask_above_bootstrap_cap() -> None:
    runner = _runner(1_700_000_000)
    clob = _tick(runner, elapsed=0, up=0.57, down=0.56)

    assert clob.calls == []
    assert runner.positions.total_deals == 0


def test_weak_outcome_cheap_repair_blocks_when_price_does_not_improve_side_avg() -> None:
    state = PositionState(spent_up=2.0, shares_up=3.8461538, spent_down=2.0, shares_down=4.0816326, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=10,
        up=0.43,
        down=0.60,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=3.8461538, avg_price=0.52),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0816326, avg_price=0.49),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.positions.orders_up == 1


def test_weak_outcome_cheap_repair_blocks_when_min_order_would_overshoot_balance() -> None:
    state = PositionState(spent_up=2.0, shares_up=7.8, spent_down=2.0, shares_down=8.0, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=10,
        up=0.30,
        down=0.70,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=7.8, avg_price=2.0 / 7.8),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.0, avg_price=0.25),
            ],
        ],
    )

    assert clob.calls == []


def test_high_guard_060_blocks_when_price_does_not_improve_side_avg() -> None:
    state = PositionState(spent_up=2.0, shares_up=3.8461538, spent_down=2.0, shares_down=4.0816326, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=10,
        up=0.60,
        down=0.40,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=3.8461538, avg_price=0.52),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0816326, avg_price=0.49),
            ],
        ],
    )

    assert clob.calls == []


def test_high_price_above_065_is_blocked_before_240s() -> None:
    state = PositionState(spent_up=2.0, shares_up=2.0, spent_down=2.0, shares_down=4.0816326, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=120,
        up=0.70,
        down=0.40,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=2.0, avg_price=1.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0816326, avg_price=0.49),
            ]
        ],
    )

    assert clob.calls == []


def test_final_60_blocks_when_existing_position_is_over_cap() -> None:
    state = PositionState(spent_up=2.0, shares_up=1.0, spent_down=8.0, shares_down=25.0, orders_up=1, orders_down=4, total_deals=5)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=250,
        up=0.80,
        down=0.20,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=1.0, avg_price=2.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=25.0, avg_price=0.32),
            ],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=3.5, avg_price=1.142857),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=25.0, avg_price=0.32),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.stop_reason == "over_cap_position"


def test_locked_profit_only_allows_cheap_or_imbalanced_buys() -> None:
    state = PositionState(spent_up=3.0, shares_up=8.0, spent_down=3.0, shares_down=8.2, orders_up=3, orders_down=3, total_deals=6)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.58,
        down=0.52,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=8.0, avg_price=0.375),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.2, avg_price=3.0 / 8.2),
            ]
        ],
    )

    assert runner.positions.pnl_if_up() >= 0.5
    assert runner.positions.pnl_if_down() >= 0.5
    assert clob.calls == []


def test_cheap_weak_buy_blocks_when_min_order_would_flip_balance() -> None:
    state = PositionState(spent_up=3.0, shares_up=8.0, spent_down=3.0, shares_down=8.2, orders_up=3, orders_down=3, total_deals=6)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.52,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=8.0, avg_price=0.375),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.2, avg_price=3.0 / 8.2),
            ]
        ],
    )

    assert clob.calls == []


def test_budget_cap_blocks_new_buy() -> None:
    state = PositionState(spent_up=10.0, shares_up=20.0, spent_down=10.0, shares_down=21.0, orders_up=5, orders_down=5, total_deals=10)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.52,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=20.0, avg_price=0.50),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=21.0, avg_price=10.0 / 21.0),
            ]
        ],
    )

    assert clob.calls == []
    assert runner.positions.spent_total() <= 20.0


def test_over_cap_other_side_stops_instead_of_chasing_balance() -> None:
    state = PositionState(spent_up=8.0, shares_up=12.0, spent_down=8.0, shares_down=20.0, orders_up=5, orders_down=4, total_deals=9)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.52,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=12.0, avg_price=8.0 / 12.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=20.0, avg_price=0.40),
            ],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.727272727, avg_price=9.2 / 14.727272727),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=20.0, avg_price=0.40),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.stop_reason == "over_cap_position"


def test_share_cap_blocks_repair_past_max_even_when_other_side_over_cap() -> None:
    state = PositionState(spent_up=8.0, shares_up=14.5, spent_down=8.0, shares_down=20.0, orders_up=4, orders_down=4, total_deals=8)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.40,
        down=0.60,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.5, avg_price=8.0 / 14.5),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=20.0, avg_price=0.40),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.positions.shares_up == 14.5


def test_overloaded_17_vs_12_does_not_buy_larger_up_side() -> None:
    state = PositionState(spent_up=8.5, shares_up=17.0, spent_down=6.0, shares_down=12.0, orders_up=6, orders_down=4, total_deals=10)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.30,
        down=0.70,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=17.0, avg_price=0.50),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=12.0, avg_price=0.50),
            ]
        ],
    )

    assert all(call[0] != "up-token" for call in clob.calls)
    assert runner.positions.shares_up == 17.0


def test_14_5_vs_12_allows_small_overtake_when_gap_stays_within_limit() -> None:
    state = PositionState(spent_up=6.5, shares_up=14.5, spent_down=5.0, shares_down=12.0, orders_up=5, orders_down=4, total_deals=9)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.30,
        down=0.39,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.5, avg_price=6.5 / 14.5),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=12.0, avg_price=5.0 / 12.0),
            ],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.5, avg_price=6.5 / 14.5),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=14.564102564, avg_price=6.0 / 14.564102564),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.positions.shares_down == 12.0


def test_repair_avg_sum_guard_blocks_projected_bad_avg_sum() -> None:
    state = PositionState(spent_up=4.75, shares_up=5.0, spent_down=4.75, shares_down=5.0, orders_up=3, orders_down=3, total_deals=6)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.45,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.95),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=5.0, avg_price=0.95),
            ]
        ],
    )

    assert clob.calls == []


def test_post_open_repair_requires_price_below_same_side_average() -> None:
    state = PositionState(
        spent_up=6.0,
        shares_up=13.6,
        spent_down=7.17,
        shares_down=12.77,
        orders_up=2,
        orders_down=2,
        total_deals=4,
    )
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.38,
        down=0.62,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=13.6, avg_price=6.0 / 13.6),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=12.77, avg_price=7.17 / 12.77),
            ]
        ],
    )

    assert clob.calls == []


def test_zero_avg_price_from_pm_uses_pending_order_price_for_cost() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 4.081632653}])

    _tick(
        runner,
        elapsed=0,
        up=0.52,
        down=0.49,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.081632653, avg_price=0.0)],
        ],
    )

    assert clob.calls == [("down-token", 2.4, 0.48)]
    assert runner.positions.shares_down == 4.081632653
    assert abs(runner.positions.spent_down - 1.95918367344) < 1e-6
    assert runner.positions.total_deals == 1


def test_over_cap_large_side_does_not_allow_smaller_side_past_max_shares() -> None:
    state = PositionState(
        spent_up=6.45,
        shares_up=19.5454,
        spent_down=6.02,
        shares_down=13.6844,
        orders_up=3,
        orders_down=2,
        total_deals=5,
    )
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.80,
        down=0.20,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=19.5454, avg_price=6.45 / 19.5454),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=13.6844, avg_price=6.02 / 13.6844),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.positions.shares_down == 13.6844


def test_bootstrap_amount_is_capped_by_max_shares_per_side() -> None:
    runner = _runner(1_700_000_000)
    clob = _tick(
        runner,
        elapsed=0,
        up=0.10,
        down=0.90,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=15.0, avg_price=0.10)],
        ],
    )

    assert clob.calls == [("up-token", 1.05, 0.09000000000000001)]
    assert runner.positions.shares_up == 15.0


def test_balanced_profit_under_roi_target_does_not_stop_window() -> None:
    state = PositionState(spent_up=6.5, shares_up=14.0, spent_down=6.5, shares_down=14.1, orders_up=4, orders_down=4, total_deals=8)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.44,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.0, avg_price=6.5 / 14.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=14.1, avg_price=6.5 / 14.1),
            ]
        ],
    )

    assert clob.calls == []
    assert runner.stop_reason is None


def test_locked_profit_balanced_state_stops_only_after_roi_target() -> None:
    state = PositionState(spent_up=5.5, shares_up=14.0, spent_down=5.5, shares_down=14.1, orders_up=4, orders_down=4, total_deals=8)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.44,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.0, avg_price=5.5 / 14.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=14.1, avg_price=5.5 / 14.1),
            ]
        ],
    )

    assert clob.calls == []
    assert runner.stop_reason == "locked_profit"


def test_twelve_vs_eight_position_does_not_stop_before_roi_and_balance() -> None:
    state = PositionState(spent_up=4.0, shares_up=12.0, spent_down=4.0, shares_down=8.0, orders_up=4, orders_down=3, total_deals=7)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.44,
        down=0.50,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=12.0, avg_price=4.0 / 12.0),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.0, avg_price=0.50),
            ]
        ],
    )

    assert runner.stop_reason is None
    assert all(call[0] != "up-token" for call in clob.calls)


def test_pending_order_blocks_new_buy() -> None:
    runner = _runner(1_700_000_000)
    runner.pending_order = True
    runner.execution_state = ORDER_IN_FLIGHT
    clob = _tick(runner, elapsed=0, up=0.52, down=0.49)

    assert clob.calls == []


def test_failed_order_does_not_update_position_and_waits_before_retry() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[Exception("boom"), {"orderID": "buy-2", "size_matched": 4.0}])

    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)
    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)
    _tick(
        runner,
        elapsed=10,
        up=0.52,
        down=0.49,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0, avg_price=0.245)],
        ],
    )

    assert clob.calls == [("down-token", 2.4, 0.48), ("down-token", 2.4, 0.48)]
    assert runner.positions.total_deals == 1
    assert runner.positions.orders_down == 1


def test_initial_two_usd_attempt_happens_only_once_after_nofill() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}, {"orderID": "buy-2", "size_matched": 2.0}])

    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)
    _tick(
        runner,
        elapsed=5,
        up=0.52,
        down=0.49,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=1.923076923, avg_price=0.52)],
        ],
    )

    assert clob.calls == [("down-token", 2.4, 0.48)]
    assert runner.initial_intent_attempted is True
    assert runner.pending_order is True
    assert runner.positions.spent_total() == 0.0
    assert runner.intent_count_down == 1
    assert runner.intent_count_up == 0


def test_after_one_side_fill_next_buy_is_missing_side_not_more_same_side() -> None:
    state = PositionState(spent_down=2.0, shares_down=8.0, orders_down=1, total_deals=1)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=10,
        up=0.44,
        down=0.20,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.0, avg_price=0.25)],
            [
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.0, avg_price=0.25),
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=4.545454545, avg_price=0.44),
            ],
        ],
    )

    assert clob.calls == [("up-token", 2.15, 0.43)]
    assert runner.positions.orders_up == 1


def test_missing_side_opens_at_061_before_high_repair_projection_guard() -> None:
    state = PositionState(spent_down=3.0, shares_down=13.5555, orders_down=1, total_deals=1)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=50,
        up=0.61,
        down=0.40,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=13.5555, avg_price=3.0 / 13.5555)],
            [
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=13.5555, avg_price=3.0 / 13.5555),
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=3.278688525, avg_price=0.61),
            ],
        ],
    )

    assert clob.calls == [("up-token", 3.0, 0.60)]
    assert runner.positions.orders_up == 1


def test_later_sizing_can_use_fractional_amount_to_optimize_projected_pnl() -> None:
    state = PositionState(spent_up=2.0, shares_up=5.0, spent_down=2.0, shares_down=8.43, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=10,
        up=0.35,
        down=0.70,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.4),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.43, avg_price=2.0 / 8.43),
            ],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=8.428571429, avg_price=0.379661017),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=8.43, avg_price=2.0 / 8.43),
            ],
        ],
    )

    assert clob.calls == [("up-token", 1.6999999999999997, 0.33999999999999997)]


def test_balance_or_allowance_error_stops_window() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[Exception("not enough balance / allowance: balance is not enough")])

    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)
    _tick(runner, elapsed=10, up=0.52, down=0.49, clob=clob)

    assert len(clob.calls) == 1
    assert runner.stop_reason == "balance_or_allowance"


def test_initial_retry_stops_after_two_failures_per_side() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(
        responses=[
            {"orderID": "buy-1", "size_matched": 0.0},
            {"orderID": "buy-2", "size_matched": 0.0},
            {"orderID": "buy-3", "size_matched": 0.0},
            {"orderID": "buy-4", "size_matched": 0.0},
        ]
    )

    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)
    _tick(runner, elapsed=5, up=0.52, down=0.50, clob=clob)
    _tick(runner, elapsed=10, up=0.48, down=0.49, clob=clob)
    _tick(runner, elapsed=15, up=0.48, down=0.49, clob=clob)
    _tick(runner, elapsed=20, up=0.48, down=0.49, clob=clob)

    assert clob.calls == [("down-token", 2.4, 0.48)]
    assert runner.stop_reason is None
    assert runner.pending_order is True
    assert runner.intent_count_down == 1
    assert runner.intent_count_up == 0


def test_bootstrap_waits_for_other_side_after_first_side_exhausted() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(
        responses=[
            {"orderID": "buy-1", "size_matched": 0.0},
            Exception("no orders found to match with FAK order"),
            {"orderID": "buy-3", "size_matched": 2.0},
        ]
    )

    _tick(runner, elapsed=0, up=0.34, down=0.66, clob=clob)
    _tick(runner, elapsed=5, up=0.29, down=0.71, clob=clob)
    _tick(runner, elapsed=10, up=0.30, down=0.60, clob=clob)
    assert runner.stop_reason is None

    _tick(
        runner,
        elapsed=15,
        up=0.49,
        down=0.50,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=2.0, avg_price=0.50)],
        ],
    )

    assert clob.calls == [("up-token", 1.6500000000000001, 0.33)]
    assert runner.pending_order is True
    assert runner.positions.orders_down == 0
    assert runner.stop_reason is None


def test_nofill_response_still_counts_fill_when_pm_confirms_position() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}])

    _tick(
        runner,
        elapsed=0,
        up=0.34,
        down=0.66,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.882352941, avg_price=0.34)],
        ],
    )

    assert clob.calls == [("up-token", 1.6500000000000001, 0.33)]
    assert runner.positions.orders_up == 1
    assert runner.positions.total_deals == 1
    assert runner.initial_failed_up == 0
    assert runner.initial_filled is True


def test_no_match_error_still_counts_fill_when_pm_confirms_position() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[Exception("no orders found to match with FAK order")])

    _tick(
        runner,
        elapsed=0,
        up=0.33,
        down=0.67,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.060606061, avg_price=0.33)],
        ],
    )

    assert clob.calls == [("up-token", 1.6, 0.32)]
    assert runner.positions.orders_up == 0
    assert runner.positions.total_deals == 0
    assert runner.initial_failed_up == 1
    assert runner.initial_filled is False


def test_pm_discovered_bootstrap_fill_triggers_missing_side_buy() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(
        responses=[
            {"orderID": "buy-1", "size_matched": 0.0},
            {"orderID": "buy-2", "size_matched": 2.0},
        ]
    )

    _tick(
        runner,
        elapsed=0,
        up=0.33,
        down=0.67,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.060606061, avg_price=0.33)],
        ],
    )
    _tick(
        runner,
        elapsed=5,
        up=0.50,
        down=0.50,
        clob=clob,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.060606061, avg_price=0.33)],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.060606061, avg_price=0.33),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0, avg_price=0.50),
            ],
        ],
    )

    assert clob.calls == [("up-token", 1.6, 0.32), ("down-token", 2.45, 0.49)]
    assert runner.positions.orders_up == 1
    assert runner.positions.orders_down == 1
    assert runner.positions.total_deals == 2


def test_pm_refresh_promotes_existing_position_to_open_side() -> None:
    runner = _runner(1_700_000_000)

    clob = _tick(
        runner,
        elapsed=5,
        up=0.60,
        down=0.50,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.0, avg_price=0.33)],
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=6.0, avg_price=0.33),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0, avg_price=0.50),
            ],
        ],
    )

    assert clob.calls == [("down-token", 2.45, 0.49)]
    assert runner.positions.orders_up == 1
    assert runner.positions.orders_down == 1
    assert runner.positions.total_deals == 2


def test_nofill_does_not_update_position() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 0.0}])

    _tick(runner, elapsed=0, up=0.52, down=0.49, clob=clob)

    assert runner.positions.total_deals == 0
    assert runner.positions.spent_total() == 0.0


def test_partial_fill_updates_from_confirmed_filled_shares_conservatively() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 2.0}])

    _tick(
        runner,
        elapsed=0,
        up=0.52,
        down=0.49,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=2.0, avg_price=0.49)],
        ],
    )

    assert runner.positions.shares_down == 2.0
    assert runner.positions.spent_down == 0.98
    assert runner.positions.total_deals == 1


def test_successful_fak_without_fill_field_records_local_risk_immediately() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"success": True, "orderID": "buy-1"}])

    _tick(
        runner,
        elapsed=0,
        up=0.60,
        down=0.33,
        clob=clob,
        positions_seq=[[], []],
    )

    assert clob.calls == [("down-token", 1.6, 0.32)]
    assert runner.pending_order is True
    assert runner.positions.shares_down == 0.0


def test_after_both_sides_open_repair_targets_smaller_side_not_larger_winning_side() -> None:
    state = PositionState(
        spent_up=3.91,
        shares_up=8.59,
        spent_down=4.00,
        shares_down=9.76,
        orders_up=3,
        orders_down=2,
        total_deals=5,
    )
    runner = _runner(1_700_000_000, positions=state)

    action = _choose_guarded_pnl_buy(
        runner,
        up_ask=0.44,
        down_ask=0.33,
        elapsed=240,
        remaining=60,
        cfg=_cfg(),
        current_winning_side="DOWN",
    )

    assert action is not None
    assert action.side == "UP"
    assert action.amount_usd == 1.0


def test_api_underreport_does_not_reduce_confirmed_local_position() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(responses=[{"orderID": "buy-1", "size_matched": 4.0}])

    _tick(
        runner,
        elapsed=0,
        up=0.50,
        down=0.49,
        clob=clob,
        positions_seq=[
            [],
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=4.0, avg_price=0.49)],
        ],
    )
    _tick(
        runner,
        elapsed=5,
        up=0.90,
        down=0.90,
        clob=clob,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=1.0, avg_price=0.49)],
        ],
    )

    assert runner.positions.shares_down == 4.0
    assert abs(runner.positions.spent_down - 1.96) < 1e-6


def test_pending_reserved_shares_count_toward_effective_share_cap() -> None:
    state = PositionState(spent_up=7.0, shares_up=14.0, spent_down=3.0, shares_down=8.0, orders_up=4, orders_down=2, total_deals=6)
    runner = _runner(1_700_000_000, positions=state)
    runner.pending_order = True
    runner.pending_side = "UP"
    runner.pending_amount_usd = 0.40
    runner.pending_reserved_shares = 1.0

    effective = _effective_state_with_pending(runner)

    assert effective.shares_up == 15.0
    assert effective.spent_up == 7.4


def test_hard_cap_uses_api_plus_local_effective_state() -> None:
    state = PositionState(spent_up=6.0, shares_up=14.0, spent_down=4.0, shares_down=10.0, orders_up=4, orders_down=3, total_deals=7)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=100,
        up=0.25,
        down=0.70,
        positions_seq=[
            [
                _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=14.8, avg_price=6.0 / 14.8),
                _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=10.0, avg_price=0.40),
            ],
        ],
    )

    assert clob.calls == []
    assert runner.positions.shares_up == 14.8


def test_late_one_sided_window_stops_when_missing_side_cannot_open() -> None:
    state = PositionState(spent_up=2.0, shares_up=5.0, spent_down=0.0, shares_down=0.0, orders_up=1, orders_down=0, total_deals=1)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(
        runner,
        elapsed=285,
        up=0.10,
        down=0.90,
        positions_seq=[
            [_positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.40)],
        ],
    )

    assert clob.calls == []
    assert runner.stop_reason == "one_sided_unhedged"


def test_one_order_per_tick_even_when_many_conditions_true() -> None:
    state = PositionState(spent_up=2.0, shares_up=2.0, spent_down=2.0, shares_down=9.0, orders_up=1, orders_down=1, total_deals=2)
    runner = _runner(1_700_000_000, positions=state)
    clob = _tick(runner, elapsed=100, up=0.29, down=0.29)

    assert len(clob.calls) == 1
