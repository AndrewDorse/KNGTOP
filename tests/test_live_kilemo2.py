from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_kilemo2 import PositionState, WindowRunner, _tick_runner


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
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, float]] = []

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.calls.append((token.token_id, usdc, 0.0 if max_price is None else max_price))
        return {"orderID": f"buy-{len(self.calls)}"}


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

    assert fake_clob.calls == [("up-token", 1.0, 0.52)]
    assert runner.positions.orders_up == 1
    assert runner.positions.orders_down == 0


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


def test_tick_runner_rescue_60_cap080_buys_missing_side() -> None:
    runner = _runner(1_700_000_000, positions=PositionState(spent_up=0.10, shares_up=2.0, orders_up=1, total_deals=1))
    runner.bootstrap_second_attempted = True
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
    runner.bootstrap_second_attempted = True
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
