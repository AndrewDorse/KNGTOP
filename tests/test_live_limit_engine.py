from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_limit_engine import PositionState, WindowRunner, _tick_runner


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
        positions=positions or PositionState(),
    )


class _FakePoly:
    def __init__(self, *, up: float, down: float) -> None:
        self.up = up
        self.down = down

    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        ask = self.up if asset_id == "up-token" else self.down
        return max(0.01, ask - 0.01), ask


class _FakeClob:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, float]] = []
        self.cancelled: list[str] = []
        self.open_orders: list[dict[str, object]] = []
        self.next_id = 1

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float, post_only: bool = True):  # noqa: ANN201
        del post_only
        oid = f"ord-{self.next_id}"
        self.next_id += 1
        self.calls.append((token.token_id, price, shares))
        self.open_orders.append(
            {"id": oid, "asset_id": token.token_id, "side": "BUY", "price": price, "original_size": shares, "size_left": shares}
        )
        return {"orderID": oid}

    def cancel_order_by_id(self, order_id: str):  # noqa: ANN201
        self.cancelled.append(str(order_id))
        self.open_orders = [row for row in self.open_orders if str(row.get("id")) != str(order_id)]
        return {"success": True}

    def get_open_orders(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.open_orders]


def _positions_row(*, slug: str, outcome: str, token_id: str, size: float, avg_price: float) -> dict[str, object]:
    return {"slug": slug, "outcome": outcome, "asset": token_id, "size": size, "avgPrice": avg_price}


def _tick(
    runner: WindowRunner,
    *,
    elapsed: int,
    up: float,
    down: float,
    clob: _FakeClob | None = None,
    positions: list[dict[str, object]] | None = None,
) -> _FakeClob:
    fake_clob = clob or _FakeClob()
    with patch("kngtop.live_limit_engine.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_000 + elapsed, timezone.utc)
        _tick_runner(
            runner,
            poly=_FakePoly(up=up, down=down),
            clob=fake_clob,
            cfg=_cfg(),
            runtime_state={"reconcile_positions": positions or [], "reconcile_open_orders": fake_clob.get_open_orders()},
        )
    return fake_clob


def _tick_with_stale_empty_cache(
    runner: WindowRunner,
    *,
    elapsed: int,
    up: float,
    down: float,
    clob: _FakeClob,
) -> None:
    with patch("kngtop.live_limit_engine.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(1_700_000_000 + elapsed, timezone.utc)
        _tick_runner(
            runner,
            poly=_FakePoly(up=up, down=down),
            clob=clob,
            cfg=_cfg(),
            runtime_state={"reconcile_positions": [], "reconcile_open_orders": []},
        )


def test_prestart_places_one_order_per_side_at_47() -> None:
    runner = _runner(1_700_000_000)
    clob = _tick(runner, elapsed=-10, up=0.55, down=0.55)

    assert clob.calls == [("up-token", 0.47, 5.0), ("down-token", 0.47, 5.0)]


def test_repeated_ticks_do_not_duplicate_orders_with_stale_empty_reconcile_cache() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob()

    _tick_with_stale_empty_cache(runner, elapsed=-10, up=0.55, down=0.55, clob=clob)
    _tick_with_stale_empty_cache(runner, elapsed=-10, up=0.55, down=0.55, clob=clob)
    _tick_with_stale_empty_cache(runner, elapsed=-9, up=0.55, down=0.55, clob=clob)

    assert clob.calls == [("up-token", 0.47, 5.0), ("down-token", 0.47, 5.0)]
    assert len([row for row in clob.open_orders if row["asset_id"] == "up-token"]) == 1
    assert len([row for row in clob.open_orders if row["asset_id"] == "down-token"]) == 1


def test_duplicate_same_side_order_is_cancelled_before_new_work() -> None:
    runner = _runner(1_700_000_000)
    clob = _FakeClob()
    clob.open_orders = [
        {"id": "up-1", "asset_id": "up-token", "side": "BUY", "price": 0.47, "original_size": 5.0, "size_left": 5.0},
        {"id": "up-2", "asset_id": "up-token", "side": "BUY", "price": 0.46, "original_size": 5.0, "size_left": 5.0},
        {"id": "down-1", "asset_id": "down-token", "side": "BUY", "price": 0.47, "original_size": 5.0, "size_left": 5.0},
    ]

    _tick(runner, elapsed=-5, up=0.55, down=0.55, clob=clob)

    assert clob.cancelled == ["up-2"]
    assert len([row for row in clob.open_orders if row["asset_id"] == "up-token"]) == 1


def test_better_lower_existing_buy_is_kept_when_desired_price_is_higher() -> None:
    runner = _runner(1_700_000_000)
    positions = [
        _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.47),
        _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=5.0, avg_price=0.47),
    ]
    clob = _FakeClob()
    clob.open_orders = [
        {"id": "up-old", "asset_id": "up-token", "side": "BUY", "price": 0.30, "original_size": 5.0, "size_left": 5.0},
        {"id": "down-ok", "asset_id": "down-token", "side": "BUY", "price": 0.30, "original_size": 5.0, "size_left": 5.0},
    ]

    _tick(runner, elapsed=30, up=0.39, down=0.45, clob=clob, positions=positions)

    assert "up-old" not in clob.cancelled
    assert clob.calls == []


def test_worse_higher_existing_buy_is_replaced_downward() -> None:
    runner = _runner(1_700_000_000)
    positions = [
        _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.47),
        _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=5.0, avg_price=0.47),
    ]
    clob = _FakeClob()
    clob.open_orders = [
        {"id": "up-high", "asset_id": "up-token", "side": "BUY", "price": 0.45, "original_size": 5.0, "size_left": 5.0},
        {"id": "down-ok", "asset_id": "down-token", "side": "BUY", "price": 0.30, "original_size": 5.0, "size_left": 5.0},
    ]

    _tick(runner, elapsed=30, up=0.39, down=0.45, clob=clob, positions=positions)

    assert "up-high" in clob.cancelled
    assert ("up-token", 0.39, 5.0) in clob.calls


def test_missing_smaller_side_order_is_posted_when_cheap() -> None:
    runner = _runner(1_700_000_000)
    positions = [
        _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=5.0, avg_price=0.47),
        _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=10.0, avg_price=0.45),
    ]
    clob = _FakeClob()
    clob.open_orders = [
        {"id": "down-ok", "asset_id": "down-token", "side": "BUY", "price": 0.43, "original_size": 5.0, "size_left": 5.0},
    ]

    _tick(runner, elapsed=30, up=0.39, down=0.45, clob=clob, positions=positions)

    assert ("up-token", 0.39, 5.0) in clob.calls
    assert all(call[0] != "down-token" for call in clob.calls)


def test_local_sent_ledger_blocks_spam_when_pm_positions_lag() -> None:
    runner = _runner(1_700_000_000)
    positions = [
        _positions_row(slug=runner.contract.slug, outcome="UP", token_id="up-token", size=10.0, avg_price=0.47),
        _positions_row(slug=runner.contract.slug, outcome="DOWN", token_id="down-token", size=10.0, avg_price=0.47),
    ]
    clob = _FakeClob()

    for i in range(6):
        clob.open_orders = [row for row in clob.open_orders if row["asset_id"] != "down-token"]
        _tick(runner, elapsed=240 + i, up=0.60, down=0.30, clob=clob, positions=positions)

    down_calls = [call for call in clob.calls if call[0] == "down-token"]
    assert len(down_calls) == 1
    assert runner.sent_shares["DOWN"] == 5.0
