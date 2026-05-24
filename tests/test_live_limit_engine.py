from __future__ import annotations

from datetime import datetime, timezone

import pytest

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_limit_engine import (
    INITIAL_PRICE,
    MAX_HEDGE_PRICE,
    ORDER_SIZE_SHARES,
    LocalPendingOrder,
    PositionState,
    StrategyDecision,
    WindowRunner,
    _c_target_prices,
    _execute_decision,
)
from kngtop.live_limit_replay import DOWN_TOKEN_ID, UP_TOKEN_ID, ReplayClob, ReplayPoly, open_order_row, run_live_engine_tick


START = 1_700_000_000


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


def _runner(start: int = START, *, positions: PositionState | None = None) -> WindowRunner:
    return WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=ActiveContract(
            slug=f"btc-updown-5m-{start}",
            question="",
            end_time=datetime.fromtimestamp(start + 300, timezone.utc),
            up=TokenMarket(token_id=UP_TOKEN_ID, outcome="UP", minimum_tick_size="0.01", neg_risk=False),
            down=TokenMarket(token_id=DOWN_TOKEN_ID, outcome="DOWN", minimum_tick_size="0.01", neg_risk=False),
        ),
        window_minutes=5,
        window_open_px=100_000.0,
        positions=positions or PositionState(),
    )


def _positions_row(*, outcome: str, token_id: str, size: float, avg_price: float) -> dict[str, object]:
    return {"slug": f"btc-updown-5m-{START}", "outcome": outcome, "asset": token_id, "size": size, "avgPrice": avg_price}


def _open_order(*, order_id: str, token_id: str, price: float, size_left: float = 5.0) -> dict[str, object]:
    return open_order_row(order_id=order_id, token_id=token_id, price=price, size_left=size_left)


def _tick(
    runner: WindowRunner,
    *,
    elapsed: float,
    up: float = 0.47,
    down: float = 0.47,
    clob: ReplayClob | None = None,
    positions: list[dict[str, object]] | None = None,
    runtime_extra: dict[str, object] | None = None,
) -> ReplayClob:
    fake_clob = clob or ReplayClob()
    runtime_state: dict[str, object] = {"reconcile_positions": positions or [], "reconcile_open_orders": fake_clob.get_open_orders()}
    if runtime_extra:
        runtime_state.update(runtime_extra)
    run_live_engine_tick(
        runner,
        poly=ReplayPoly(up=up, down=down),
        clob=fake_clob,
        cfg=_cfg(),
        elapsed=elapsed,
        runtime_state=runtime_state,
    )
    return fake_clob


def _mark_pending_resolved(runner: WindowRunner) -> None:
    for side in ("UP", "DOWN"):
        for pending in runner.local_pending_orders_by_side[side]:
            pending.resolved = True
            pending.status = "FILL_CONFIRMED"
    runner.action_lock_until = 0.0
    runner.last_action_time = 0.0


def test_strategy_flat_window_targets_initial_pair() -> None:
    runner = _runner()

    targets = _c_target_prices(runner, up_ask=0.65, down_ask=0.35, cfg=_cfg())

    assert targets == {"UP": INITIAL_PRICE, "DOWN": INITIAL_PRICE}


def test_initial_gate_sends_exactly_up_and_down_batch() -> None:
    runner = _runner()
    clob = _tick(runner, elapsed=-10)

    assert clob.limit_calls == [
        (UP_TOKEN_ID, pytest.approx(INITIAL_PRICE), pytest.approx(ORDER_SIZE_SHARES)),
        (DOWN_TOKEN_ID, pytest.approx(INITIAL_PRICE), pytest.approx(ORDER_SIZE_SHARES)),
    ]
    assert runner.initial_batch_sent is True
    assert len(runner.local_pending_orders_by_side["UP"]) == 1
    assert len(runner.local_pending_orders_by_side["DOWN"]) == 1


def test_initial_gate_missed_does_not_enter_midwindow_at_market_prices() -> None:
    runner = _runner()
    clob = _tick(runner, elapsed=30, up=0.65, down=0.35)

    assert clob.limit_calls == []
    assert runner.initial_batch_sent is False


def test_up_initial_fills_down_initial_remains_open_no_duplicates() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="manual-down", token_id=DOWN_TOKEN_ID, price=INITIAL_PRICE)]
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=INITIAL_PRICE)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.limit_calls == []
    assert clob.cancelled == []


def test_up_fills_down_order_missing_creates_one_down_capped_at_70c() -> None:
    runner = _runner()
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    clob = _tick(runner, elapsed=5, up=0.80, down=0.91, positions=positions)

    assert len(clob.limit_calls) == 1
    token_id, price, shares = clob.limit_calls[0]
    assert token_id == DOWN_TOKEN_ID
    assert price <= MAX_HEDGE_PRICE
    assert shares == pytest.approx(ORDER_SIZE_SHARES)


def test_manual_down_order_covers_imbalance_without_duplicate_or_cancel() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="manual-down", token_id=DOWN_TOKEN_ID, price=0.25)]
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.limit_calls == []
    assert clob.cancelled == []


def test_balanced_next_pair_only_when_avg_sum_after_buy_is_safe() -> None:
    runner = _runner()
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.40),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.40),
    ]

    clob = _tick(runner, elapsed=5, up=0.46, down=0.46, positions=positions)

    assert clob.limit_calls == [
        (UP_TOKEN_ID, pytest.approx(0.38), pytest.approx(ORDER_SIZE_SHARES)),
        (DOWN_TOKEN_ID, pytest.approx(0.38), pytest.approx(ORDER_SIZE_SHARES)),
    ]

    blocked = _runner()
    blocked_positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.90),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.90),
    ]
    blocked_clob = _tick(blocked, elapsed=5, up=0.60, down=0.60, positions=blocked_positions)

    assert blocked_clob.limit_calls == []


def test_imbalanced_up10_down5_allows_only_down_order() -> None:
    runner = _runner()
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=10.0, avg_price=0.47),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.47),
    ]

    clob = _tick(runner, elapsed=5, up=0.20, down=0.45, positions=positions)

    assert clob.limit_calls == [(DOWN_TOKEN_ID, pytest.approx(0.45), pytest.approx(ORDER_SIZE_SHARES))]


def test_empty_open_orders_delay_with_local_pending_sends_nothing() -> None:
    runner = _runner()
    _tick(runner, elapsed=-10)
    clob = ReplayClob()

    _tick(runner, elapsed=-9, clob=clob, runtime_extra={"reconcile_open_orders": []})

    assert clob.limit_calls == []


def test_place_timeout_marks_unknown_and_blocks_duplicate_until_reconciled() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.fail_next_place = True
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)
    assert clob.limit_calls == [(DOWN_TOKEN_ID, pytest.approx(0.47), pytest.approx(ORDER_SIZE_SHARES))]
    assert runner.local_pending_orders_by_side["DOWN"][0].status == "UNKNOWN"

    _tick(runner, elapsed=6.1, clob=clob, positions=positions)

    assert len(clob.limit_calls) == 1


def test_cancel_replacement_waits_for_confirmed_cancel_before_next_place() -> None:
    runner = _runner()
    clob = ReplayClob()
    bot_order = _open_order(order_id="bot-up", token_id=UP_TOKEN_ID, price=0.47)
    clob.open_orders = [bot_order]
    runner.local_pending_orders_by_side["UP"].append(
        LocalPendingOrder(
            client_order_id="kngtop-c47-test-up",
            side="UP",
            token_id=UP_TOKEN_ID,
            price=0.47,
            shares=5.0,
            sent_at=START - 10,
            exchange_order_id="bot-up",
            resolved=True,
            status="OPEN_CONFIRMED",
        )
    )
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.cancelled == []
    assert clob.limit_calls == []

    _tick(runner, elapsed=7.1, clob=clob, positions=positions)

    assert clob.cancelled == ["bot-up"]
    assert clob.limit_calls == []

    _tick(runner, elapsed=8.2, clob=clob, positions=positions)

    assert clob.limit_calls == [(DOWN_TOKEN_ID, pytest.approx(0.47), pytest.approx(ORDER_SIZE_SHARES))]


def test_cheap_useful_smaller_side_order_is_not_cancelled_or_replaced() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="cheap-down", token_id=DOWN_TOKEN_ID, price=0.25)]
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.cancelled == []
    assert clob.limit_calls == []


def test_partial_fill_updates_shares_remaining_order_counts_live_no_duplicate() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="partial-down", token_id=DOWN_TOKEN_ID, price=0.47, size_left=2.0)]
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=3.0, avg_price=0.47),
    ]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert runner.positions.shares_down == pytest.approx(3.0)
    assert runner.open_orders["DOWN"][0].remaining_shares == pytest.approx(2.0)
    assert clob.limit_calls == []


def test_max_shares_reached_sends_no_more_orders() -> None:
    runner = _runner()
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=15.0, avg_price=0.47),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=15.0, avg_price=0.47),
    ]

    clob = _tick(runner, elapsed=5, positions=positions)

    assert clob.limit_calls == []


def test_imbalanced_larger_side_bot_order_cancel_is_one_action_only() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="bot-up", token_id=UP_TOKEN_ID, price=0.47)]
    runner.local_pending_orders_by_side["UP"].append(
        LocalPendingOrder(
            client_order_id="kngtop-c47-test-up",
            side="UP",
            token_id=UP_TOKEN_ID,
            price=0.47,
            shares=5.0,
            sent_at=START - 10,
            exchange_order_id="bot-up",
            resolved=True,
            status="OPEN_CONFIRMED",
        )
    )
    positions = [_positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.cancelled == []
    assert clob.limit_calls == []

    _tick(runner, elapsed=7.1, clob=clob, positions=positions)

    assert clob.cancelled == ["bot-up"]
    assert clob.limit_calls == []


def test_manual_larger_side_order_is_not_cancelled_and_blocks_larger_side_creation() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="manual-up", token_id=UP_TOKEN_ID, price=0.47)]
    positions = [_positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.47)]

    _tick(runner, elapsed=5, clob=clob, positions=positions)

    assert clob.cancelled == []
    assert clob.limit_calls == []


def test_balanced_with_one_live_down_order_repairs_by_placing_up_only() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [_open_order(order_id="down-live", token_id=DOWN_TOKEN_ID, price=0.37)]
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.47),
    ]

    _tick(runner, elapsed=5, clob=clob, up=0.27, down=0.74, positions=positions)

    assert clob.limit_calls == []
    assert clob.cancelled == []

    _tick(runner, elapsed=7.1, clob=clob, up=0.27, down=0.74, positions=positions)

    assert clob.limit_calls == [(UP_TOKEN_ID, pytest.approx(0.27), pytest.approx(ORDER_SIZE_SHARES))]
    assert clob.cancelled == []


def test_duplicate_bot_orders_self_fix_after_repeated_confirmation() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.open_orders = [
        _open_order(order_id="bot-down-high", token_id=DOWN_TOKEN_ID, price=0.47),
        _open_order(order_id="bot-down-low", token_id=DOWN_TOKEN_ID, price=0.37),
    ]
    for order_id, price in (("bot-down-high", 0.47), ("bot-down-low", 0.37)):
        runner.local_pending_orders_by_side["DOWN"].append(
            LocalPendingOrder(
                client_order_id=f"kngtop-c47-test-{order_id}",
                side="DOWN",
                token_id=DOWN_TOKEN_ID,
                price=price,
                shares=5.0,
                sent_at=START - 10,
                exchange_order_id=order_id,
                resolved=True,
                status="OPEN_CONFIRMED",
            )
        )
    positions = [
        _positions_row(outcome="UP", token_id=UP_TOKEN_ID, size=5.0, avg_price=0.47),
        _positions_row(outcome="DOWN", token_id=DOWN_TOKEN_ID, size=5.0, avg_price=0.47),
    ]

    _tick(runner, elapsed=5, clob=clob, positions=positions)
    assert clob.cancelled == []

    _tick(runner, elapsed=7.1, clob=clob, positions=positions)

    assert clob.cancelled == ["bot-down-high"]
    assert clob.limit_calls == []


def test_trade_history_counts_resting_fill_even_when_side_field_is_sell() -> None:
    runner = _runner()
    clob = ReplayClob()
    clob.recent_trades[DOWN_TOKEN_ID] = [{"side": "SELL", "price": 0.47, "size": 5.0, "id": "down-fill"}]

    _tick(runner, elapsed=5, clob=clob, up=0.45, down=0.39, positions=[])

    assert runner.positions.shares_down == pytest.approx(5.0)
    assert clob.limit_calls == [(UP_TOKEN_ID, pytest.approx(0.45), pytest.approx(ORDER_SIZE_SHARES))]


def test_execution_boundary_blocks_order_on_already_larger_effective_side() -> None:
    runner = _runner(positions=PositionState(spent_down=2.10, shares_down=5.0))
    clob = ReplayClob()

    sent = _execute_decision(
        runner,
        clob=clob,
        decision=StrategyDecision("PLACE", "bad_stale_decision", [("DOWN", 0.39, ORDER_SIZE_SHARES)]),
        now_ts=START + 5,
    )

    assert sent is False
    assert clob.limit_calls == []
    assert runner.local_pending_orders_by_side["DOWN"] == []
