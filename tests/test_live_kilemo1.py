from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_kilemo1 import ORDER_LIMIT_PRICE, ORDER_NOTIONAL_USD, WindowRunner, _tick_runner
from kngtop.live_kilemo1 import evaluate_signal


def test_signal_fires_on_move_gate_for_cheap_up() -> None:
    decision = evaluate_signal(
        window_open_px=100_000.0,
        spot_px=100_002.0,
        mid_up=0.14,
        mid_dn=0.86,
        price_then_now_5s=(100_002.0, 100_001.0),
        price_then_now_20s=(100_002.0, 100_000.0),
        volume_ratio_20s=1.5,
    )
    assert decision is not None
    assert decision.side == "UP"
    assert decision.move_gate is True
    assert decision.volume_gate is True


def test_signal_fires_on_volume_gate_for_cheap_down() -> None:
    decision = evaluate_signal(
        window_open_px=100_000.0,
        spot_px=99_998.5,
        mid_up=0.88,
        mid_dn=0.12,
        price_then_now_5s=(99_998.5, 100_000.0),
        price_then_now_20s=(99_998.5, 100_001.0),
        volume_ratio_20s=1.5,
    )
    assert decision is not None
    assert decision.side == "DOWN"
    assert decision.volume_gate is True
    assert decision.move_gate is True


def test_signal_requires_cheap_hit() -> None:
    decision = evaluate_signal(
        window_open_px=100_000.0,
        spot_px=100_003.0,
        mid_up=0.18,
        mid_dn=0.82,
        price_then_now_5s=(100_003.0, 100_001.0),
        price_then_now_20s=(100_003.0, 99_999.0),
        volume_ratio_20s=2.0,
    )
    assert decision is None


def test_signal_rejects_when_move_and_volume_disagree() -> None:
    decision = evaluate_signal(
        window_open_px=100_000.0,
        spot_px=99_999.0,
        mid_up=0.15,
        mid_dn=0.85,
        price_then_now_5s=(99_999.0, 100_000.0),
        price_then_now_20s=(99_999.0, 100_001.5),
        volume_ratio_20s=1.6,
    )
    assert decision is None


def test_signal_requires_close_to_open_gate() -> None:
    decision = evaluate_signal(
        window_open_px=100_000.0,
        spot_px=100_040.0,
        mid_up=0.14,
        mid_dn=0.86,
        price_then_now_5s=(100_040.0, 100_038.0),
        price_then_now_20s=(100_040.0, 100_036.0),
        volume_ratio_20s=1.8,
    )
    assert decision is None


class _FakePoly:
    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        if asset_id == "up-token":
            return 0.13, 0.15
        return 0.84, 0.86


class _FakeBinance:
    def last_price(self, symbol: str, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        return 100_002.0

    def price_then_now(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, max_age_sec
        if lookback_sec == 5:
            return 100_002.0, 100_001.0
        return 100_002.0, 100_000.0

    def current_volume_ratio(self, symbol: str, *, lookback_sec: int, max_age_sec: float = 6.0):  # noqa: ANN201
        del symbol, lookback_sec, max_age_sec
        return 1.6


class _FakeClob:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, float]] = []

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None):  # noqa: ANN201
        self.calls.append((token.token_id, usdc, 0.0 if max_price is None else max_price))
        return {"orderID": "buy-1"}


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
        notional_usd=1.0,
        trading_pairs=(("BTC", "BTCUSDT"),),
        log_level="INFO",
        order_cutoff_remaining_sec=20.0,
        order_retry_on_error=0,
        market_buy_max_price=0.85,
        binance_max_age_sec=6.0,
        poly_mid_max_age_sec=5.0,
        ws_rest_poll_enabled=False,
        ws_rest_poll_interval_sec=1.0,
        hedge_max_orders_per_side=5,
        max_shares_per_side=15.0,
        max_share_gap=2.0,
        repair_avg_sum_cap=0.95,
        locked_profit_roi=0.10,
    )


def test_tick_runner_submits_one_dollar_fak_buy() -> None:
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
    )
    fake_clob = _FakeClob()
    cfg = _cfg()

    with patch("kngtop.live_kilemo1.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + 60, timezone.utc)
        _tick_runner(
            runner,
            poly=_FakePoly(),
            binance=_FakeBinance(),
            clob=fake_clob,
            cfg=cfg,
        )
    assert runner.attempted is True
    assert fake_clob.calls == [("up-token", ORDER_NOTIONAL_USD, ORDER_LIMIT_PRICE)]
