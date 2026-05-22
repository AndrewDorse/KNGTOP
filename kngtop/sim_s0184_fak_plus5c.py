from __future__ import annotations

import argparse
import csv
from pathlib import Path

from kngtop.sim_search_start_window_hold import (
    DEFAULT_SAMPLE_SIZES,
    load_windows,
    settle,
    side_price,
    winner_side,
)

WINDOW_SEC = 300
NOTIONAL_USD = 1.0
ENTRY_PRICE_MIN = 0.46
ENTRY_PRICE_MAX = 0.56
ENTRY_MAX_SEC = 20
MOVE_FROM_OPEN_MIN_USD = 1.0
FAK_PRICE_BUFFER = 0.05
MAX_ENTRY_PRICE = 0.99


def parse_sample_sizes(raw: str, total: int) -> list[int | str]:
    out: list[int | str] = []
    for part in (raw or "").split(","):
        token = part.strip().lower()
        if not token:
            continue
        if token == "full":
            out.append("full")
            continue
        out.append(max(1, int(token)))
    if not out:
        out = [100, 200, 400, 1000, "full"]
    normalized: list[int | str] = []
    for item in out:
        if item == "full":
            normalized.append("full")
        else:
            normalized.append(min(int(item), total))
    return normalized


def entry_price_for_side(observed_side_px: float) -> float:
    return min(MAX_ENTRY_PRICE, float(observed_side_px) + FAK_PRICE_BUFFER)


def evaluate_window(win) -> tuple[bool, str | None, int | None, float | None, float]:
    for tick_index in range(min(ENTRY_MAX_SEC + 1, WINDOW_SEC)):
        side = winner_side(win, tick_index)
        if side is None:
            continue
        tick = win.ticks[tick_index]
        observed_px = side_price(tick, side)
        if observed_px < ENTRY_PRICE_MIN - 1e-12 or observed_px > ENTRY_PRICE_MAX + 1e-12:
            continue
        btc_move = abs(float(tick.btc_price) - float(win.ticks[0].btc_price))
        if btc_move + 1e-12 < MOVE_FROM_OPEN_MIN_USD:
            continue
        entry_px = entry_price_for_side(observed_px)
        pnl = settle(win, side, entry_px)
        return True, side, tick_index, entry_px, pnl
    return False, None, None, None, 0.0


def summarize(windows, sample_sizes: list[int | str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for sample in sample_sizes:
        subset = windows if sample == "full" else windows[: int(sample)]
        wins = 0
        trades = 0
        total_pnl = 0.0
        entry_sum = 0.0
        first_trigger_sum = 0.0
        for win in subset:
            traded, side, tick_index, entry_px, pnl = evaluate_window(win)
            if not traded or side is None or tick_index is None or entry_px is None:
                continue
            trades += 1
            total_pnl += pnl
            entry_sum += entry_px
            first_trigger_sum += tick_index
            if pnl > 1e-12:
                wins += 1
        windows_count = len(subset)
        win_rate = (100.0 * wins / trades) if trades else 0.0
        trade_rate = (100.0 * trades / windows_count) if windows_count else 0.0
        avg_entry = (entry_sum / trades) if trades else 0.0
        avg_trigger_sec = (first_trigger_sum / trades) if trades else 0.0
        rows.append(
            {
                "strategy_key": "S0184_FAK_PLUS5C",
                "strategy_label": (
                    "winner side, first20s, observed 0.46-0.56, btc move>=1.0, "
                    "entry=observed+0.05 capped at 0.99"
                ),
                "sample_size": "full" if sample == "full" else int(sample),
                "windows": windows_count,
                "trades": trades,
                "trade_rate_pct": round(trade_rate, 4),
                "wins": wins,
                "win_rate_pct": round(win_rate, 4),
                "total_pnl_usd": round(total_pnl, 6),
                "avg_entry_px": round(avg_entry, 6),
                "avg_trigger_sec": round(avg_trigger_sec, 4),
                "order_notional_usd": f"{NOTIONAL_USD:.2f}",
                "fak_price_buffer": f"{FAK_PRICE_BUFFER:.2f}",
                "notes": (
                    "Immediate start-window replay with a conservative FAK approximation: "
                    "fill price is observed winner-side price plus 5c."
                ),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx(path: Path, rows: list[dict[str, object]]) -> None:
    try:
        from openpyxl import Workbook
    except Exception:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "s0184_fak_plus5c"
    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row.get(header, "") for header in headers])
    ws.freeze_panes = "A2"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Replay the S0184 BTC 5m rule with a +5c FAK entry model.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument(
        "--sample-sizes",
        default=",".join(DEFAULT_SAMPLE_SIZES),
        help="Comma list like 100,200,400,1000,full",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_fak_plus5c_report.csv",
    )
    parser.add_argument(
        "--xlsx-out",
        type=Path,
        default=repo_root / "reports" / "s0184_fak_plus5c_report.xlsx",
    )
    args = parser.parse_args()

    windows = load_windows(args.input_dir)
    if not windows:
        raise SystemExit(f"no windows found under {args.input_dir}")
    rows = summarize(windows, parse_sample_sizes(args.sample_sizes, len(windows)))
    write_csv(args.csv_out, rows)
    write_xlsx(args.xlsx_out, rows)

    for row in rows:
        print(
            f"sample={row['sample_size']} trades={row['trades']} "
            f"wr={row['win_rate_pct']}% pnl={row['total_pnl_usd']}"
        )
    print(f"Wrote {args.csv_out}")
    print(f"Wrote {args.xlsx_out}")


if __name__ == "__main__":
    main()
