from __future__ import annotations

from kngtop.live_limit_replay import simulate_window_with_live_engine, summarize
from kngtop.simulate_strategies import Tick, WindowData


def _window() -> WindowData:
    ticks = tuple(
        Tick(
            timestamp=f"2026-05-20T00:00:{idx:02d}+00:00",
            elapsed_sec=elapsed,
            remaining_sec=300 - elapsed,
            up_price=up,
            down_price=down,
            btc_price=100_000.0,
            pm_volume=1.0,
            btc_volume=1.0,
            btc_quote_volume=100.0,
            btc_trade_count=1,
        )
        for idx, (elapsed, up, down) in enumerate(
            [
                (-10, 0.50, 0.50),
                (-9, 0.47, 0.47),
                (5, 0.27, 0.74),
                (8, 0.27, 0.74),
            ]
        )
    )
    return WindowData(
        window_id="btc-updown-5m-1700000000",
        start_time=ticks[0].timestamp,
        end_time=ticks[-1].timestamp,
        ticks=ticks,
        final_result="DOWN",
    )


def test_replay_uses_live_engine_and_reports_safety_metrics() -> None:
    row = simulate_window_with_live_engine(_window())

    assert row["orders_placed"] >= 2
    assert row["danger_orders"] == 0
    assert row["max_effective_gap"] == 0.0


def test_replay_summary_is_stable() -> None:
    rows = [simulate_window_with_live_engine(_window())]

    summary = summarize(rows)

    assert summary["windows"] == 1
    assert summary["danger_orders"] == 0
    assert summary["max_effective_gap"] == 0.0
