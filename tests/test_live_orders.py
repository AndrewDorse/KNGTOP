from __future__ import annotations

from datetime import datetime, timezone

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


def test_cycle_begin_primary_marks_busy() -> None:
    runner = _runner(1_700_000_000)
    lo.cycle_begin_primary(runner, side="DOWN", price=0.48, shares=5.0, reason="bootstrap")
    assert lo.has_active_order(runner)
    assert runner.cycle.phase == lo.PHASE_WAIT_PRIMARY
    assert runner.orders_sent == 1


def test_order_on_clob_detects_open_order() -> None:
    runner = _runner(1_700_000_000)
    lo.cycle_begin_primary(runner, side="DOWN", price=0.48, shares=5.0, reason="bootstrap")
    lo.cycle_mark_primary_id(runner, "ord-1")
    clob = _FakeClob(
        open_orders=[
            {
                "id": "ord-1",
                "asset_id": "down-token",
                "side": "BUY",
                "price": 0.48,
                "original_size": 5.0,
                "size_left": 5.0,
            }
        ]
    )
    assert lo.order_on_clob(runner, clob=clob, order_id="ord-1", side="DOWN")


def test_pm_stable_requires_five_checks() -> None:
    runner = _runner(1_700_000_000)
    lo.cycle_start_pm_wait(runner, up_shares=5.0, down_shares=5.0, now_ts=1_700_000_000.0)
    ts = 1_700_000_000.0
    for i in range(1, 4):
        assert not lo.tick_pm_stable(runner, up_shares=5.0, down_shares=5.0, now_ts=ts + i)
    assert lo.tick_pm_stable(runner, up_shares=5.0, down_shares=5.0, now_ts=ts + 4.0)


def test_clear_window_orders() -> None:
    runner = _runner(1_700_000_000)
    lo.cycle_begin_primary(runner, side="DOWN", price=0.48, shares=5.0, reason="bootstrap")
    lo.clear_window_orders(runner)
    assert runner.orders == {}
    assert runner.open_orders == {"UP": [], "DOWN": []}
    assert not lo.has_active_order(runner)


def test_cycle_reset_returns_idle() -> None:
    runner = _runner(1_700_000_000)
    lo.cycle_begin_primary(runner, side="DOWN", price=0.48, shares=5.0, reason="bootstrap")
    lo.cycle_reset(runner)
    assert lo.cycle_is_idle(runner)
