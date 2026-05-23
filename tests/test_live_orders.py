from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop import live_orders as lo
from kngtop.live_kilemo2 import PositionState, WindowRunner


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


def _runner(start: int) -> WindowRunner:
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
        positions=PositionState(),
    )


class _FakeClob:
    def __init__(self, *, open_orders: list[dict[str, object]] | None = None) -> None:
        self.open_orders = list(open_orders or [])
        self.order_payloads: dict[str, dict[str, object]] = {}

    def get_open_orders(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.open_orders]

    def get_order(self, order_id: str) -> dict[str, object]:
        return dict(self.order_payloads.get(str(order_id), {}))


def test_register_intent_before_post() -> None:
    runner = _runner(1_700_000_000)
    order = lo.register_intent(
        runner,
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    assert order.status == lo.ORDER_INTENT
    assert lo.has_active_order(runner)


def test_reconcile_updates_partial_status() -> None:
    runner = _runner(1_700_000_000)
    order = lo.register_intent(
        runner,
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    lo.mark_posted(order, order_id="ord-1")
    clob = _FakeClob(
        open_orders=[
            {
                "id": "ord-1",
                "asset_id": "down-token",
                "side": "BUY",
                "price": 0.48,
                "original_size": 5.0,
                "size_left": 3.0,
            }
        ]
    )
    lo.reconcile_runner_orders(runner, clob=clob, open_order_rows=clob.get_open_orders(), now_ts=1_700_000_001.0)
    assert lo.has_active_order(runner)
    assert order.status == lo.ORDER_PARTIAL
    assert order.matched_shares == 2.0


def test_missing_from_open_marks_filled_when_fully_matched() -> None:
    runner = _runner(1_700_000_000)
    order = lo.register_intent(
        runner,
        side="UP",
        token_id="up-token",
        price=0.40,
        shares=5.0,
        reason="balance",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    lo.mark_posted(order, order_id="ord-2")
    order.matched_shares = 5.0
    order.status = lo.ORDER_OPEN
    clob = _FakeClob(open_orders=[])
    clob.order_payloads["ord-2"] = {
        "id": "ord-2",
        "status": "filled",
        "size_matched": 5.0,
        "original_size": 5.0,
        "size_left": 0.0,
    }
    lo.reconcile_runner_orders(runner, clob=clob, open_order_rows=[], now_ts=1_700_000_002.0)
    assert order.status == lo.ORDER_FILLED
    assert not lo.has_active_order(runner)


def test_orphan_open_order_is_adopted() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob(
        open_orders=[
            {
                "id": "orphan-1",
                "asset_id": "down-token",
                "side": "BUY",
                "price": 0.50,
                "original_size": 5.0,
                "size_left": 5.0,
            }
        ]
    )
    lo.reconcile_runner_orders(runner, clob=clob, open_order_rows=clob.get_open_orders(), now_ts=1_700_000_000.0)
    assert lo.has_active_order(runner)
    assert runner.pending_order_id == "orphan-1"


def test_clear_window_orders() -> None:
    runner = _runner(1_700_000_000)
    lo.register_intent(
        runner,
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    lo.clear_window_orders(runner)
    assert runner.orders == {}
    assert runner.open_orders == {"UP": [], "DOWN": []}


def test_register_intent_refuses_second_in_flight() -> None:
    runner = _runner(1_700_000_000)
    first = lo.register_intent(
        runner,
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    second = lo.register_intent(
        runner,
        side="UP",
        token_id="up-token",
        price=0.40,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_001.0,
    )
    assert first is not None
    assert second is None
    assert runner.orders_sent == 1


def test_post_failure_does_not_block_forever() -> None:
    runner = _runner(1_700_000_000)
    order = lo.register_intent(
        runner,
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        pre_shares=0.0,
        sent_ts=1_700_000_000.0,
    )
    lo.mark_failed(order, error="boom")
    assert not lo.has_active_order(runner)
