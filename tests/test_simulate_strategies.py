from __future__ import annotations

import pytest

from kngtop.simulate_strategies import (
    CANCEL_AFTER_SEC,
    Tick,
    WindowData,
    WindowState,
    run_simulation,
    simulate_window,
    strategy_deep_ladder,
)


def _window(*, up: list[float], down: list[float], btc: list[float], volume: list[float] | None = None, final_result: str = "UP") -> WindowData:
    vols = volume or [1.0] * len(up)
    ticks = tuple(
        Tick(
            timestamp=f"2026-05-20T00:00:{idx:02d}+00:00",
            elapsed_sec=idx,
            remaining_sec=300 - idx,
            up_price=up[idx],
            down_price=down[idx],
            btc_price=btc[idx],
            pm_volume=vols[idx],
            btc_volume=vols[idx],
            btc_quote_volume=vols[idx] * 100.0,
            btc_trade_count=1 if vols[idx] > 0 else 0,
        )
        for idx in range(len(up))
    )
    return WindowData(
        window_id="w1",
        start_time=ticks[0].timestamp,
        end_time=ticks[-1].timestamp,
        ticks=ticks,
        final_result=final_result,
    )


def test_budget_never_exceeds_limit() -> None:
    state = WindowState(budget=20.0, share_size=5)
    for _ in range(6):
        assert state.place_order("UP", 0.65, 0) is True
    assert state.place_order("UP", 0.65, 0) is False
    assert state.reserved_budget() <= 20.0


def test_share_size_is_respected() -> None:
    window = _window(up=[0.50, 0.45, 0.45], down=[0.50, 0.45, 0.45], btc=[100.0, 100.0, 100.0], final_result="UP")
    result = simulate_window(window, "Deep Ladder Both Sides", "base", 3, 20.0, strategy_deep_ladder)
    assert result.up_shares % 3 == 0
    assert result.down_shares % 3 == 0


def test_no_fills_after_cancel_time() -> None:
    up = [0.80] * (CANCEL_AFTER_SEC + 3)
    down = [0.80] * (CANCEL_AFTER_SEC + 3)
    up[CANCEL_AFTER_SEC + 1] = 0.30
    down[CANCEL_AFTER_SEC + 1] = 0.30
    btc = [100.0] * (CANCEL_AFTER_SEC + 3)
    window = _window(up=up, down=down, btc=btc, final_result="UP")
    result = simulate_window(window, "Deep Ladder Both Sides", "base", 5, 20.0, strategy_deep_ladder)
    assert result.orders_filled == 0


def test_pnl_and_redeem_payout_are_correct() -> None:
    window = _window(up=[0.50, 0.45, 0.45], down=[0.50, 0.50, 0.50], btc=[100.0, 100.0, 100.0], final_result="UP")
    result = simulate_window(window, "Deep Ladder Both Sides", "base", 5, 20.0, strategy_deep_ladder)
    assert result.up_shares == 5
    assert result.down_shares == 0
    assert result.cost_gross == pytest.approx(2.35)
    assert result.gross_pnl == pytest.approx(2.65)


def test_strategy_results_are_deterministic(tmp_path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    path = data_dir / "w.csv"
    path.write_text(
        "recorded_at,slug,question,elapsed_sec,remaining_sec,up_price,down_price,source,btc_price,btc_volume,btc_quote_volume,btc_trade_count\n"
        "2026-05-20 00:00:00+00:00,w1,q,0,300,0.50,0.50,s,100,1,100,1\n"
        "2026-05-20 00:00:01+00:00,w1,q,1,299,0.45,0.50,s,100,1,100,1\n",
        encoding="utf-8",
    )
    out1 = tmp_path / "r1.csv"
    out2 = tmp_path / "r2.csv"
    rows1, details1 = run_simulation(str(data_dir), str(out1), 20.0, (1,))
    rows2, details2 = run_simulation(str(data_dir), str(out2), 20.0, (1,))
    assert rows1 == rows2
    assert details1 == details2
