from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop import live_reconcile as lr
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


def test_register_sent_order_tracks_active_status() -> None:
    runner = _runner(1_700_000_000)
    record = lr.register_sent_order(
        runner,
        order_id="ord-1",
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        sent_ts=1_700_000_000.0,
    )

    assert record.status == lr.SENT_SENT
    assert record.is_active()
    assert runner.sent_orders["ord-1"] is record


def test_refresh_live_reconcile_cache_updates_open_orders_and_sent_status() -> None:
    runner = _runner(1_700_000_000)
    lr.register_sent_order(
        runner,
        order_id="ord-1",
        side="DOWN",
        token_id="down-token",
        price=0.48,
        shares=5.0,
        reason="bootstrap",
        sent_ts=1_700_000_000.0,
    )
    clob = _FakeClob(
        open_orders=[
            {
                "id": "ord-1",
                "asset_id": "down-token",
                "side": "BUY",
                "price": 0.48,
                "original_size": 5.0,
                "size_left": 3.0,
                "size_matched": 2.0,
            }
        ]
    )
    runtime_state: dict[str, object] = {"reconcile_seq": 0}
    positions = [
        {
            "slug": runner.contract.slug,
            "outcome": "DOWN",
            "asset": "down-token",
            "size": 2.0,
            "avgPrice": 0.48,
        }
    ]

    with patch("kngtop.live_reconcile.fetch_user_positions", return_value=positions):
        lr.refresh_live_reconcile_cache(
            clob=clob,  # type: ignore[arg-type]
            cfg=_cfg(),
            runtime_state=runtime_state,
            runners={1_700_000_000: runner},
        )

    assert lr.runner_has_active_open_order(runner)
    assert runner.sent_orders["ord-1"].status == lr.SENT_PARTIAL
    assert runner.sent_orders["ord-1"].matched_shares == 2.0
    assert len(runner.open_orders["DOWN"]) == 1
    assert runtime_state["reconcile_seq"] == 1


def test_sent_order_marked_filled_when_missing_from_open_orders() -> None:
    runner = _runner(1_700_000_000)
    lr.register_sent_order(
        runner,
        order_id="ord-2",
        side="UP",
        token_id="up-token",
        price=0.40,
        shares=5.0,
        reason="balance",
        sent_ts=1_700_000_000.0,
    )
    runner.sent_orders["ord-2"].status = lr.SENT_OPEN
    runner.sent_orders["ord-2"].matched_shares = 5.0
    clob = _FakeClob(open_orders=[])
    runtime_state: dict[str, object] = {"reconcile_seq": 0}

    with patch("kngtop.live_reconcile.fetch_user_positions", return_value=[]):
        lr.refresh_live_reconcile_cache(
            clob=clob,  # type: ignore[arg-type]
            cfg=_cfg(),
            runtime_state=runtime_state,
            runners={1_700_000_000: runner},
        )

    assert runner.sent_orders["ord-2"].status == lr.SENT_FILLED
    assert not lr.runner_has_active_open_order(runner)
