from __future__ import annotations

import csv
import statistics
from dataclasses import dataclass
from pathlib import Path

from kngtop.live_kilemo2 import (
    EXCHANGE_MIN_ORDER_USD,
    HEDGE_PRICE_CAP,
    IMBALANCE_SLACK_USD,
    MAX_BUDGET_USD,
    REBALANCE_MULT,
    SEED_MOVE_LOOKBACK_SEC,
    TARGET_ROI,
    PositionState,
    _floor_pnl_after_buy,
    evaluate_seed_signal,
    target_amount_for_side,
)
from kngtop.sim_kilemo2_hedge_search import WindowData, load_windows

ORDER_CUTOFF_SEC = 280
SEED_ORDER_USD = 2.0
HEDGE_MAX_ORDER_USD = 2.0
MAX_ORDERS_PER_SIDE = 2
SAMPLE_SIZE = 2000


@dataclass(frozen=True, slots=True)
class ForcedVariant:
    key: str
    label: str
    force_after_sec: int
    force_price_cap: float
    bypass_floor_check: bool
    force_order_mode: str


def _can_buy(state: PositionState, side: str, notional_usd: float, *, max_orders_per_side: int) -> bool:
    if notional_usd + 1e-12 < EXCHANGE_MIN_ORDER_USD:
        return False
    if state.spent_total + notional_usd > MAX_BUDGET_USD + 1e-12:
        return False
    if side == "UP":
        return state.orders_up < max_orders_per_side
    return state.orders_down < max_orders_per_side


def _record_fill(state: PositionState, side: str, price: float, notional_usd: float) -> None:
    shares = float(notional_usd) / float(price)
    if side == "UP":
        state.spent_up += float(notional_usd)
        state.shares_up += shares
        state.orders_up += 1
    else:
        state.spent_down += float(notional_usd)
        state.shares_down += shares
        state.orders_down += 1


def _current_winning_side(window: WindowData, idx: int) -> str | None:
    delta = window.ticks[idx].btc_price - window.ticks[0].btc_price
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def _losing_side_from_positions(state: PositionState) -> str:
    return "UP" if state.pnl_if_up() < state.pnl_if_down() else "DOWN"


def _raw_dynamic_order(state: PositionState, side: str, ask_px: float) -> float:
    return target_amount_for_side(
        state=state,
        side=side,
        price=float(ask_px),
        target_roi=TARGET_ROI,
        rebalance_mult=REBALANCE_MULT,
        max_order_usd=HEDGE_MAX_ORDER_USD,
        imbalance_slack_usd=IMBALANCE_SLACK_USD,
    )


def _normal_hedge_order(state: PositionState, side: str, ask_px: float) -> tuple[float, str]:
    raw = _raw_dynamic_order(state, side, ask_px)
    if raw <= 1e-12:
        return 0.0, "raw_zero"
    order_usd = raw
    if 0.0 < raw < EXCHANGE_MIN_ORDER_USD - 1e-12:
        floor_before = min(state.pnl_if_up(), state.pnl_if_down())
        floor_after_min = _floor_pnl_after_buy(
            state=state,
            side=side,
            price=float(ask_px),
            notional_usd=EXCHANGE_MIN_ORDER_USD,
        )
        if floor_after_min + 1e-12 < floor_before:
            return 0.0, "rounded_min_worsens_floor"
        order_usd = EXCHANGE_MIN_ORDER_USD
        return order_usd, "rounded_to_min"
    return min(order_usd, HEDGE_MAX_ORDER_USD), "dynamic"


def _forced_hedge_order(state: PositionState, side: str, ask_px: float, variant: ForcedVariant) -> tuple[float, str]:
    if variant.force_order_mode == "min1":
        return EXCHANGE_MIN_ORDER_USD, "forced_min1"
    raw = _raw_dynamic_order(state, side, ask_px)
    if raw <= 1e-12:
        raw = EXCHANGE_MIN_ORDER_USD
    order_usd = max(EXCHANGE_MIN_ORDER_USD, min(raw, HEDGE_MAX_ORDER_USD))
    if variant.bypass_floor_check:
        return order_usd, "forced_dynamic_bypass_floor"
    floor_before = min(state.pnl_if_up(), state.pnl_if_down())
    floor_after = _floor_pnl_after_buy(state=state, side=side, price=float(ask_px), notional_usd=order_usd)
    if floor_after + 1e-12 < floor_before:
        return 0.0, "forced_worsens_floor"
    return order_usd, "forced_dynamic_keep_floor"


def _simulate_window(window: WindowData, variant: ForcedVariant) -> dict[str, object]:
    state = PositionState()
    seed_side: str | None = None
    seed_idx: int | None = None
    first_hedge_idx: int | None = None
    first_hedge_mode: str | None = None
    for idx, tick in enumerate(window.ticks):
        if tick.elapsed_sec > ORDER_CUTOFF_SEC:
            break
        price_then_now_10s = (
            tick.btc_price,
            window.ticks[max(0, idx - SEED_MOVE_LOOKBACK_SEC)].btc_price,
        )
        decision = evaluate_seed_signal(
            window_open_px=window.ticks[0].btc_price,
            spot_px=tick.btc_price,
            up_bid=tick.up_price,
            up_ask=tick.up_price,
            down_bid=tick.down_price,
            down_ask=tick.down_price,
            price_then_now_10s=price_then_now_10s,
        )
        if decision is not None and state.spent_total <= 1e-12:
            order_usd = max(SEED_ORDER_USD, EXCHANGE_MIN_ORDER_USD)
            order_usd = min(order_usd, MAX_BUDGET_USD - state.spent_total)
            if _can_buy(state, decision.side, order_usd, max_orders_per_side=MAX_ORDERS_PER_SIDE):
                _record_fill(state, decision.side, decision.ask_px, order_usd)
                seed_side = decision.side
                seed_idx = idx
                continue
        if state.spent_total + 1e-12 < EXCHANGE_MIN_ORDER_USD:
            continue
        side = _losing_side_from_positions(state)
        ask_px = tick.up_price if side == "UP" else tick.down_price
        normal_order, normal_reason = (0.0, "price_cap")
        if ask_px <= HEDGE_PRICE_CAP + 1e-12:
            normal_order, normal_reason = _normal_hedge_order(state, side, ask_px)
        if normal_order > 1e-12 and _can_buy(state, side, normal_order, max_orders_per_side=MAX_ORDERS_PER_SIDE):
            _record_fill(state, side, ask_px, normal_order)
            if first_hedge_idx is None and seed_side is not None and side != seed_side:
                first_hedge_idx = idx
                first_hedge_mode = normal_reason
            continue
        if seed_idx is None or first_hedge_idx is not None:
            continue
        if idx - seed_idx < variant.force_after_sec:
            continue
        if ask_px > variant.force_price_cap + 1e-12:
            continue
        forced_order, forced_reason = _forced_hedge_order(state, side, ask_px, variant)
        if forced_order <= 1e-12:
            continue
        if not _can_buy(state, side, forced_order, max_orders_per_side=MAX_ORDERS_PER_SIDE):
            continue
        _record_fill(state, side, ask_px, forced_order)
        if side != seed_side:
            first_hedge_idx = idx
            first_hedge_mode = forced_reason
    end_btc = window.ticks[-1].btc_price
    start_btc = window.ticks[0].btc_price
    winning_side = "UP" if end_btc > start_btc else "DOWN" if end_btc < start_btc else None
    realized = state.pnl_if_up() if winning_side == "UP" else state.pnl_if_down() if winning_side == "DOWN" else min(state.pnl_if_up(), state.pnl_if_down())
    guaranteed = min(state.pnl_if_up(), state.pnl_if_down())
    seed_only = int(seed_side is not None and first_hedge_idx is None)
    seed_lost = int(seed_side is not None and winning_side is not None and seed_side != winning_side)
    return {
        "traded": int(seed_side is not None),
        "seed_only": seed_only,
        "seed_lost": seed_lost,
        "seed_only_and_lost": int(seed_only and seed_lost),
        "negative_realized": int(realized < -1e-12),
        "hedged": int(first_hedge_idx is not None),
        "first_hedge_idx": first_hedge_idx,
        "first_hedge_from_seed_sec": None if first_hedge_idx is None or seed_idx is None else first_hedge_idx - seed_idx,
        "first_hedge_mode": first_hedge_mode or "",
        "realized": realized,
        "guaranteed": guaranteed,
        "both_10roi": int(
            state.spent_total > 1e-12
            and state.pnl_if_up() >= 0.10 * state.spent_total - 1e-12
            and state.pnl_if_down() >= 0.10 * state.spent_total - 1e-12
        ),
        "spent_total": state.spent_total,
    }


def _summarize(rows: list[dict[str, object]], variant: ForcedVariant) -> dict[str, object]:
    traded = sum(int(r["traded"]) for r in rows)
    hedged = sum(int(r["hedged"]) for r in rows)
    seed_only = sum(int(r["seed_only"]) for r in rows)
    seed_only_and_lost = sum(int(r["seed_only_and_lost"]) for r in rows)
    negative_realized = sum(int(r["negative_realized"]) for r in rows)
    both_10roi = sum(int(r["both_10roi"]) for r in rows)
    realized_total = sum(float(r["realized"]) for r in rows)
    guaranteed_total = sum(float(r["guaranteed"]) for r in rows)
    spent_total = sum(float(r["spent_total"]) for r in rows)
    hedge_secs = [int(r["first_hedge_from_seed_sec"]) for r in rows if r["first_hedge_from_seed_sec"] is not None]
    return {
        "variant_key": variant.key,
        "variant_label": variant.label,
        "windows": len(rows),
        "traded_windows": traded,
        "trade_rate_pct": round(100.0 * traded / len(rows), 4),
        "hedged_windows": hedged,
        "hedged_rate_pct": round(100.0 * hedged / traded, 4) if traded else 0.0,
        "seed_only_windows": seed_only,
        "seed_only_rate_pct": round(100.0 * seed_only / traded, 4) if traded else 0.0,
        "seed_only_and_lost_windows": seed_only_and_lost,
        "seed_only_and_lost_rate_pct": round(100.0 * seed_only_and_lost / traded, 4) if traded else 0.0,
        "negative_realized_windows": negative_realized,
        "negative_realized_rate_pct": round(100.0 * negative_realized / traded, 4) if traded else 0.0,
        "both_10roi_windows": both_10roi,
        "realized_total_pnl_usd": round(realized_total, 6),
        "guaranteed_total_pnl_usd": round(guaranteed_total, 6),
        "guaranteed_roi_pct": round(100.0 * guaranteed_total / spent_total, 4) if spent_total > 1e-12 else 0.0,
        "avg_first_hedge_sec_from_seed": round(sum(hedge_secs) / len(hedge_secs), 4) if hedge_secs else "",
        "median_first_hedge_sec_from_seed": statistics.median(hedge_secs) if hedge_secs else "",
        "p90_first_hedge_sec_from_seed": sorted(hedge_secs)[max(0, int(len(hedge_secs) * 0.9) - 1)] if hedge_secs else "",
        "force_after_sec": variant.force_after_sec,
        "force_price_cap": variant.force_price_cap,
        "bypass_floor_check": int(variant.bypass_floor_check),
        "force_order_mode": variant.force_order_mode,
    }


def _variants() -> list[ForcedVariant]:
    out = [
        ForcedVariant(
            key="BASE",
            label="baseline current live profile",
            force_after_sec=9999,
            force_price_cap=0.0,
            bypass_floor_check=False,
            force_order_mode="min1",
        )
    ]
    seq = 1
    for force_after in (20, 30, 45, 60, 90):
        for price_cap in (0.38, 0.40, 0.45, 0.50):
            for bypass in (False, True):
                for mode in ("min1", "dynamic"):
                    out.append(
                        ForcedVariant(
                            key=f"F{seq:03d}",
                            label=f"force_after={force_after}s cap<={price_cap:.2f} mode={mode} bypass_floor={str(bypass).lower()}",
                            force_after_sec=force_after,
                            force_price_cap=price_cap,
                            bypass_floor_check=bypass,
                            force_order_mode=mode,
                        )
                    )
                    seq += 1
    return out


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    input_dir = repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m"
    windows = load_windows(input_dir, sample_size=SAMPLE_SIZE)
    if len(windows) < SAMPLE_SIZE:
        raise RuntimeError(f"Need {SAMPLE_SIZE} windows, found {len(windows)}")
    all_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    for variant in _variants():
        rows = [_simulate_window(win, variant) for win in windows]
        summary = _summarize(rows, variant)
        summary_rows.append(summary)
        all_rows.extend({"variant_key": variant.key, "variant_label": variant.label, "window_index": i + 1, **row} for i, row in enumerate(rows))
    summary_rows.sort(
        key=lambda row: (
            -float(row["negative_realized_rate_pct"]),
        )
    )
    baseline = next(row for row in summary_rows if row["variant_key"] == "BASE")
    candidates = [
        row
        for row in summary_rows
        if row["variant_key"] != "BASE"
        and float(row["negative_realized_rate_pct"]) < float(baseline["negative_realized_rate_pct"])
        and float(row["guaranteed_total_pnl_usd"]) > 0.0
        and float(row["realized_total_pnl_usd"]) > 0.0
    ]
    candidates.sort(
        key=lambda row: (
            float(row["negative_realized_rate_pct"]),
            -float(row["seed_only_and_lost_rate_pct"]),
            -float(row["guaranteed_total_pnl_usd"]),
            -float(row["both_10roi_windows"]),
        )
    )
    top5 = candidates[:5]
    out_summary = repo_root / "reports" / "kilemo2_forced_hedge_top5.csv"
    out_timing = repo_root / "reports" / "kilemo2_forced_hedge_timing.csv"
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    with out_summary.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(top5[0].keys()) if top5 else list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(top5)
    timing_rows = [
        {
            "metric": "baseline_hedged_windows",
            "value": baseline["hedged_windows"],
        },
        {
            "metric": "baseline_hedged_rate_pct",
            "value": baseline["hedged_rate_pct"],
        },
        {
            "metric": "baseline_avg_first_hedge_sec_from_seed",
            "value": baseline["avg_first_hedge_sec_from_seed"],
        },
        {
            "metric": "baseline_median_first_hedge_sec_from_seed",
            "value": baseline["median_first_hedge_sec_from_seed"],
        },
        {
            "metric": "baseline_p90_first_hedge_sec_from_seed",
            "value": baseline["p90_first_hedge_sec_from_seed"],
        },
        {
            "metric": "baseline_negative_realized_rate_pct",
            "value": baseline["negative_realized_rate_pct"],
        },
        {
            "metric": "baseline_seed_only_and_lost_rate_pct",
            "value": baseline["seed_only_and_lost_rate_pct"],
        },
    ]
    with out_timing.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerows(timing_rows)
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        Workbook = None
    if Workbook is not None:
        wb = Workbook()
        ws = wb.active
        ws.title = "top5"
        top_fields = list(top5[0].keys()) if top5 else list(summary_rows[0].keys())
        ws.append(top_fields)
        for row in top5:
            ws.append([row[key] for key in top_fields])
        ws2 = wb.create_sheet("baseline_timing")
        ws2.append(["metric", "value"])
        for row in timing_rows:
            ws2.append([row["metric"], row["value"]])
        wb.save(out_summary.with_suffix(".xlsx"))
    print("Baseline timing:")
    for row in timing_rows:
        print(f"  {row['metric']}={row['value']}")
    print()
    print("Top 5 forced hedge options:")
    for row in top5:
        print(
            f"  {row['variant_key']}: neg_rate={float(row['negative_realized_rate_pct']):.2f}% "
            f"seed_only_lost={float(row['seed_only_and_lost_rate_pct']):.2f}% "
            f"guaranteed={float(row['guaranteed_total_pnl_usd']):+.2f} "
            f"avg_hedge_s={row['avg_first_hedge_sec_from_seed']} {row['variant_label']}"
        )


if __name__ == "__main__":
    main()
