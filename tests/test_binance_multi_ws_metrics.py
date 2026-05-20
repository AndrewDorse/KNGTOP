from __future__ import annotations

from collections import deque

from kngtop.binance_multi_ws import (
    TradeSample,
    _price_then_now_from_samples,
    _volume_ratio_from_samples,
)


def test_price_then_now_uses_oldest_available_when_history_is_short() -> None:
    samples = deque(
        [
            TradeSample(wall_ts=100.2, price=100_000.0, qty=1.0),
            TradeSample(wall_ts=104.9, price=100_001.5, qty=1.0),
        ]
    )
    current, past = _price_then_now_from_samples(samples, now_ts=105.0, lookback_sec=30)
    assert current == 100_001.5
    assert past == 100_000.0


def test_volume_ratio_matches_current_second_over_previous_mean() -> None:
    samples = deque(
        [
            TradeSample(wall_ts=100.2, price=100_000.0, qty=1.0),
            TradeSample(wall_ts=101.2, price=100_000.1, qty=1.0),
            TradeSample(wall_ts=102.2, price=100_000.2, qty=1.0),
            TradeSample(wall_ts=103.2, price=100_000.3, qty=1.0),
            TradeSample(wall_ts=104.2, price=100_000.4, qty=1.0),
            TradeSample(wall_ts=105.2, price=100_000.5, qty=5.0),
        ]
    )
    ratio = _volume_ratio_from_samples(samples, now_ts=105.9, lookback_sec=5)
    assert ratio == 5.0


def test_volume_ratio_returns_infinite_when_previous_mean_is_zero() -> None:
    samples = deque([TradeSample(wall_ts=200.1, price=100_000.0, qty=3.0)])
    ratio = _volume_ratio_from_samples(samples, now_ts=200.9, lookback_sec=10)
    assert ratio == float("inf")
