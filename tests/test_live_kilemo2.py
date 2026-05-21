from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_kilemo2 import PositionState, WindowRunner, _tick_runner, evaluate_seed_signal, target_amount_for_side


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
        market_buy_max_price=0.85,
        binance_max_age_sec=6.0,
        poly_mid_max_age_sec=5.0,
        ws_rest_poll_enabled=False,
        ws_rest_poll_interval_sec=1.0,
        hedge_max_orders_per_side=2,
    )


def test_seed_signal_requires_winner_side_band_and_move() -> None:
    decision = evaluate_seed_signal(
        window_open_px=100_000.0,
        spot_px=100_003.0,
        up_bid=0.41,
        up_ask=0.44,
        down_bid=0.56,
        down_ask=0.59,
        price_then_now_10s=(100_003.0, 100_000.0),
    )
    assert decision is not None
    assert decision.side == "UP"
    assert decision.ask_px == 0.44
    assert decision.move_10s == 3.0


def test_seed_signal_rejects_price_outside_band() -> None:
    decision = evaluate_seed_signal(
        window_open_px=100_000.0,
        spot_px=99_996.0,
        up_bid=0.62,
        up_ask=0.65,
        down_bid=0.30,
        down_ask=0.33,
        price_then_now_10s=(99_996.0, 100_001.0),
    )
    assert decision is None


def test_target_amount_for_side_rebalances_weaker_leg() -> None:
    state = PositionState(
        spent_up=1.0,
        spent_down=1.0,
        shares_up=5.5,
        shares_down=1.5,
        orders_up=1,
        orders_down=1,
    )
    amount = target_amount_for_side(
        state=state,
        side="DOWN",
        price=0.25,
        target_roi=0.0,
        rebalance_mult=1.0,
        max_order_usd=1.0,
        imbalance_slack_usd=0.5,
    )
    assert amount == 1.0


class _FakePoly:
    def __init__(self) -> None:
        self.calls = 0

    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        self.calls += 1
        hedging_phase = self.calls > 2
        if asset_id == "up-token":
            return (0.43, 0.45) if not hedging_phase else (0.73, 0.75)
        return (0.54, 0.56) if not hedging_phase else (0.23, 0.25)


class _FakeBinance:
    def __init__(self) -> None:
        self.price_then_calls = 0

    def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        return 100_003.0

    def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        self.price_then_calls += 1
        assert lookback_sec == 10
        return 100_003.0, 100_000.0


class _FakeClob:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, float]] = []

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.calls.append((token.token_id, usdc, 0.0 if max_price is None else max_price))
        return {"orderID": f"buy-{len(self.calls)}"}


def test_tick_runner_submits_seed_without_delay() -> None:
    start = 1_700_000_000
    runner = WindowRunner(
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
    fake_clob = _FakeClob()
    cfg = _cfg()
    poly = _FakePoly()
    binance = _FakeBinance()

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + 60, timezone.utc)
        _tick_runner(runner, poly=poly, binance=binance, clob=fake_clob, cfg=cfg)

    assert fake_clob.calls[0] == ("up-token", 2.0, 0.45)
    assert runner.positions is not None
    assert runner.positions.orders_up == 1
    assert runner.positions.orders_down == 0
    assert runner.positions.spent_total == 2.0
    assert runner.positions.pnl_if_up() > 0.0
    assert runner.positions.pnl_if_down() < 0.0


def test_tick_runner_rounds_small_beneficial_hedge_to_exchange_minimum() -> None:
    start = 1_700_000_000
    runner = WindowRunner(
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
        positions=PositionState(spent_up=2.0, shares_up=3.0, spent_down=0.0, shares_down=0.0, orders_up=1, orders_down=0),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _PolyOnlyDownCheap:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            if asset_id == "up-token":
                return 0.73, 0.75
            return 0.23, 0.25

    class _BinanceFixed:
        def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, max_age_sec
            return 100_003.0

        def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, lookback_sec, max_age_sec
            return 100_003.0, 100_000.0

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + 70, timezone.utc)
        _tick_runner(runner, poly=_PolyOnlyDownCheap(), binance=_BinanceFixed(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("down-token", 1.0, 0.25)]
    assert runner.positions is not None
    assert runner.positions.orders_down == 1
    assert runner.positions.pnl_if_up() >= -1e-12
    assert runner.positions.pnl_if_down() > 0.0


def test_tick_runner_submits_hedge_when_deficit_needs_full_order() -> None:
    start = 1_700_000_000
    runner = WindowRunner(
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
        positions=PositionState(spent_up=2.0, shares_up=6.0, spent_down=0.0, shares_down=0.0, orders_up=2, orders_down=0),
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    class _PolyHedge:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            if asset_id == "up-token":
                return 0.73, 0.75
            return 0.23, 0.25

    class _BinanceFixed:
        def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, max_age_sec
            return 100_003.0

        def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, lookback_sec, max_age_sec
            return 100_003.0, 100_000.0

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + 70, timezone.utc)
        _tick_runner(runner, poly=_PolyHedge(), binance=_BinanceFixed(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == [("down-token", 1.5, 0.25)]
    assert runner.positions is not None
    assert runner.positions.orders_down == 1
    assert runner.positions.pnl_if_down() > 0.0


def test_tick_runner_respects_max_orders_per_side() -> None:
    start = 1_700_000_000
    runner = WindowRunner(
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
        positions=PositionState(spent_up=1.0, shares_up=2.5, spent_down=1.0, shares_down=1.5, orders_up=1, orders_down=1),
    )
    fake_clob = _FakeClob()
    cfg = replace(_cfg(), hedge_max_orders_per_side=1)

    class _PolyOnlyDownCheap:
        def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
            del max_age_sec
            if asset_id == "up-token":
                return 0.70, 0.72
            return 0.24, 0.25

    class _BinanceFixed:
        def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, max_age_sec
            return 100_003.0

        def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
            del symbol, lookback_sec, max_age_sec
            return 100_003.0, 100_000.0

    with patch("kngtop.live_kilemo2.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + 70, timezone.utc)
        _tick_runner(runner, poly=_PolyOnlyDownCheap(), binance=_BinanceFixed(), clob=fake_clob, cfg=cfg)

    assert fake_clob.calls == []
