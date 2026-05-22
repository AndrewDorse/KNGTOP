from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from kngtop.sim_s0184_fak_plus5c import (
    FAK_PRICE_BUFFER,
    NOTIONAL_USD,
    entry_price_for_side,
)
from kngtop.sim_search_start_window_hold import (
    load_windows,
    settle,
    side_price,
    winner_side,
)

DEFAULT_SAMPLE_SIZES = ("100", "200", "500", "1000")
ENTRY_MIN_PRICE = 0.46
ENTRY_MAX_PRICE = 0.56
ENTRY_MAX_SEC = 20
MOVE_FROM_OPEN_MIN_USD = 1.0


@dataclass(frozen=True, slots=True)
class ExitVariant:
    key: str
    label: str
    take_profit_px: float | None


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    traded: bool
    side: str | None
    entry_tick: int | None
    entry_px: float | None
    exit_mode: str | None
    exit_tick: int | None
    exit_px: float | None
    pnl_usd: float


VARIANTS = (
    ExitVariant("hold", "Hold To Resolution", None),
    ExitVariant("tp85", "Sell At 0.85", 0.85),
    ExitVariant("tp90", "Sell At 0.90", 0.90),
    ExitVariant("tp95", "Sell At 0.95", 0.95),
)


def parse_sample_sizes(raw: str, total: int) -> list[int]:
    out: list[int] = []
    for part in (raw or "").split(","):
        token = part.strip().lower()
        if not token:
            continue
        out.append(min(total, max(1, int(token))))
    return out or [100, 200, 500, 1000]


def locate_entry(win) -> tuple[str | None, int | None, float | None]:
    for tick_index in range(ENTRY_MAX_SEC + 1):
        side = winner_side(win, tick_index)
        if side is None:
            continue
        tick = win.ticks[tick_index]
        observed_px = side_price(tick, side)
        if observed_px < ENTRY_MIN_PRICE - 1e-12 or observed_px > ENTRY_MAX_PRICE + 1e-12:
            continue
        btc_move = abs(float(tick.btc_price) - float(win.ticks[0].btc_price))
        if btc_move + 1e-12 < MOVE_FROM_OPEN_MIN_USD:
            continue
        return side, tick_index, entry_price_for_side(observed_px)
    return None, None, None


def evaluate_window(win, variant: ExitVariant) -> TradeOutcome:
    side, entry_tick, entry_px = locate_entry(win)
    if side is None or entry_tick is None or entry_px is None:
        return TradeOutcome(False, None, None, None, None, None, None, 0.0)

    if variant.take_profit_px is None:
        pnl = settle(win, side, entry_px)
        return TradeOutcome(True, side, entry_tick, entry_px, "resolution", len(win.ticks) - 1, None, pnl)

    shares = NOTIONAL_USD / entry_px
    for tick_index in range(entry_tick + 1, len(win.ticks)):
        px = side_price(win.ticks[tick_index], side)
        if px + 1e-12 >= float(variant.take_profit_px):
            pnl = shares * float(variant.take_profit_px) - NOTIONAL_USD
            return TradeOutcome(True, side, entry_tick, entry_px, "take_profit", tick_index, float(variant.take_profit_px), pnl)

    pnl = settle(win, side, entry_px)
    return TradeOutcome(True, side, entry_tick, entry_px, "resolution", len(win.ticks) - 1, None, pnl)


def summarize(windows, sample_sizes: list[int]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    summary_rows: list[dict[str, object]] = []
    diff_rows: list[dict[str, object]] = []

    for sample_size in sample_sizes:
        subset = windows[:sample_size]
        per_variant: dict[str, dict[str, float | int]] = {}
        for variant in VARIANTS:
            trades = 0
            wins = 0
            total_pnl = 0.0
            entry_sum = 0.0
            trigger_sum = 0.0
            tp_hits = 0
            tp_hit_pnl = 0.0
            for win in subset:
                outcome = evaluate_window(win, variant)
                if not outcome.traded or outcome.entry_px is None or outcome.entry_tick is None:
                    continue
                trades += 1
                total_pnl += outcome.pnl_usd
                entry_sum += outcome.entry_px
                trigger_sum += outcome.entry_tick
                if outcome.pnl_usd > 1e-12:
                    wins += 1
                if outcome.exit_mode == "take_profit":
                    tp_hits += 1
                    tp_hit_pnl += outcome.pnl_usd
            trade_rate = (100.0 * trades / sample_size) if sample_size else 0.0
            win_rate = (100.0 * wins / trades) if trades else 0.0
            avg_entry = (entry_sum / trades) if trades else 0.0
            avg_trigger = (trigger_sum / trades) if trades else 0.0
            tp_hit_rate = (100.0 * tp_hits / trades) if trades else 0.0
            row = {
                "sample_size": sample_size,
                "variant_key": variant.key,
                "variant_label": variant.label,
                "trades": trades,
                "trade_rate_pct": round(trade_rate, 4),
                "wins": wins,
                "win_rate_pct": round(win_rate, 4),
                "total_pnl_usd": round(total_pnl, 6),
                "avg_entry_px": round(avg_entry, 6),
                "avg_trigger_sec": round(avg_trigger, 4),
                "take_profit_px": "" if variant.take_profit_px is None else f"{variant.take_profit_px:.2f}",
                "tp_hits": tp_hits,
                "tp_hit_rate_pct": round(tp_hit_rate, 4),
                "tp_hit_pnl_usd": round(tp_hit_pnl, 6),
                "entry_model": f"observed ask + {FAK_PRICE_BUFFER:.2f}",
            }
            summary_rows.append(row)
            per_variant[variant.key] = row

        hold = per_variant["hold"]
        hold_pnl = float(hold["total_pnl_usd"])
        hold_wr = float(hold["win_rate_pct"])
        for variant in VARIANTS[1:]:
            row = per_variant[variant.key]
            diff_rows.append(
                {
                    "sample_size": sample_size,
                    "variant_key": variant.key,
                    "variant_label": variant.label,
                    "delta_total_pnl_usd_vs_hold": round(float(row["total_pnl_usd"]) - hold_pnl, 6),
                    "delta_win_rate_pct_vs_hold": round(float(row["win_rate_pct"]) - hold_wr, 4),
                    "hold_total_pnl_usd": hold["total_pnl_usd"],
                    "variant_total_pnl_usd": row["total_pnl_usd"],
                    "hold_win_rate_pct": hold["win_rate_pct"],
                    "variant_win_rate_pct": row["win_rate_pct"],
                    "hold_trades": hold["trades"],
                    "variant_trades": row["trades"],
                }
            )
    return summary_rows, diff_rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx(path: Path, summary_rows: list[dict[str, object]], diff_rows: list[dict[str, object]]) -> None:
    try:
        from openpyxl import Workbook
    except Exception:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "summary"
    summary_headers = list(summary_rows[0].keys())
    ws.append(summary_headers)
    for row in summary_rows:
        ws.append([row.get(header, "") for header in summary_headers])
    ws.freeze_panes = "A2"

    ws2 = wb.create_sheet("vs_hold")
    diff_headers = list(diff_rows[0].keys())
    ws2.append(diff_headers)
    for row in diff_rows:
        ws2.append([row.get(header, "") for header in diff_headers])
    ws2.freeze_panes = "A2"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Compare S0184 hold-to-resolution vs take-profit exits.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument(
        "--sample-sizes",
        default=",".join(DEFAULT_SAMPLE_SIZES),
        help="Comma list like 100,200,500,1000",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_takeprofit_compare.csv",
    )
    parser.add_argument(
        "--diff-csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_takeprofit_compare_vs_hold.csv",
    )
    parser.add_argument(
        "--xlsx-out",
        type=Path,
        default=repo_root / "reports" / "s0184_takeprofit_compare.xlsx",
    )
    args = parser.parse_args()

    windows = load_windows(args.input_dir)
    if not windows:
        raise SystemExit(f"no windows found under {args.input_dir}")

    summary_rows, diff_rows = summarize(windows, parse_sample_sizes(args.sample_sizes, len(windows)))
    write_csv(args.csv_out, summary_rows)
    write_csv(args.diff_csv_out, diff_rows)
    write_xlsx(args.xlsx_out, summary_rows, diff_rows)

    for row in summary_rows:
        print(
            f"sample={row['sample_size']} variant={row['variant_key']} "
            f"trades={row['trades']} wr={row['win_rate_pct']}% pnl={row['total_pnl_usd']}"
        )
    print(f"Wrote {args.csv_out}")
    print(f"Wrote {args.diff_csv_out}")
    print(f"Wrote {args.xlsx_out}")


if __name__ == "__main__":
    main()
