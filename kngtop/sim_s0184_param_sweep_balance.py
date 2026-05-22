from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from kngtop.sim_search_start_window_hold import load_windows, settle, side_price, winner_side

START_BALANCE_USD = 10.0
MIN_ORDER_NOTIONAL_USD = 1.0
ORDER_SIZE_BALANCE_FRACTION = 0.10


@dataclass(frozen=True, slots=True)
class Variant:
    key: str
    entry_min_price: float
    entry_max_price: float
    entry_max_elapsed_sec: int
    move_from_open_min_usd: float
    buy_price_buffer: float
    take_profit_price: float | None

    @property
    def label(self) -> str:
        tp = "hold" if self.take_profit_price is None else f"tp{self.take_profit_price:.2f}"
        return (
            f"band {self.entry_min_price:.2f}-{self.entry_max_price:.2f} "
            f"first{self.entry_max_elapsed_sec}s move>={self.move_from_open_min_usd:.1f} "
            f"buffer+{self.buy_price_buffer:.2f} {tp}"
        )


@dataclass
class Stats:
    windows: int
    start_balance_usd: float
    end_balance_usd: float
    peak_balance_usd: float
    max_drawdown_usd: float
    max_drawdown_pct: float
    trades: int
    wins: int
    losses: int
    total_pnl_usd: float
    tp_hits: int
    resolution_exits: int
    avg_order_size_usd: float
    best_win_streak: int
    worst_loss_streak: int


def order_size(balance_usd: float) -> float:
    raw = max(MIN_ORDER_NOTIONAL_USD, float(balance_usd) * ORDER_SIZE_BALANCE_FRACTION)
    return min(float(balance_usd), raw) if balance_usd > 0 else 0.0


def longest_streak(flags: list[bool], target: bool) -> int:
    best = 0
    cur = 0
    for flag in flags:
        if flag is target:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def entry_price(observed_ask_px: float, buy_price_buffer: float) -> float:
    return min(0.99, float(observed_ask_px) + float(buy_price_buffer))


def simulate_variant(windows, variant: Variant, *, start_balance_usd: float) -> Stats:
    balance = float(start_balance_usd)
    peak_balance = balance
    max_drawdown_usd = 0.0
    max_drawdown_pct = 0.0
    trades = 0
    wins = 0
    losses = 0
    total_pnl_usd = 0.0
    tp_hits = 0
    resolution_exits = 0
    order_size_sum = 0.0
    trade_results: list[bool] = []

    for win in windows:
        notional_usd = order_size(balance)
        if notional_usd <= 1e-12:
            continue

        traded = False
        pnl_usd = 0.0
        for tick_index in range(min(variant.entry_max_elapsed_sec + 1, len(win.ticks))):
            side = winner_side(win, tick_index)
            if side is None:
                continue
            tick = win.ticks[tick_index]
            observed_px = side_price(tick, side)
            if observed_px < variant.entry_min_price - 1e-12 or observed_px > variant.entry_max_price + 1e-12:
                continue
            btc_move = abs(float(tick.btc_price) - float(win.ticks[0].btc_price))
            if btc_move + 1e-12 < variant.move_from_open_min_usd:
                continue

            traded = True
            trades += 1
            order_size_sum += notional_usd
            fill_px = entry_price(observed_px, variant.buy_price_buffer)
            shares = notional_usd / fill_px

            if variant.take_profit_price is not None:
                hit_tp = False
                for exit_tick in range(tick_index + 1, len(win.ticks)):
                    held_px = side_price(win.ticks[exit_tick], side)
                    if held_px + 1e-12 >= variant.take_profit_price:
                        pnl_usd = shares * variant.take_profit_price - notional_usd
                        tp_hits += 1
                        hit_tp = True
                        break
                if not hit_tp:
                    pnl_usd = settle(win, side, fill_px) * notional_usd
                    resolution_exits += 1
            else:
                pnl_usd = settle(win, side, fill_px) * notional_usd
                resolution_exits += 1
            break

        if not traded:
            continue

        total_pnl_usd += pnl_usd
        balance += pnl_usd
        peak_balance = max(peak_balance, balance)
        drawdown_usd = peak_balance - balance
        drawdown_pct = (100.0 * drawdown_usd / peak_balance) if peak_balance > 0 else 0.0
        max_drawdown_usd = max(max_drawdown_usd, drawdown_usd)
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)

        won = pnl_usd > 1e-12
        trade_results.append(won)
        if won:
            wins += 1
        elif pnl_usd < -1e-12:
            losses += 1

    return Stats(
        windows=len(windows),
        start_balance_usd=float(start_balance_usd),
        end_balance_usd=balance,
        peak_balance_usd=peak_balance,
        max_drawdown_usd=max_drawdown_usd,
        max_drawdown_pct=max_drawdown_pct,
        trades=trades,
        wins=wins,
        losses=losses,
        total_pnl_usd=total_pnl_usd,
        tp_hits=tp_hits,
        resolution_exits=resolution_exits,
        avg_order_size_usd=(order_size_sum / trades) if trades else 0.0,
        best_win_streak=longest_streak(trade_results, True),
        worst_loss_streak=longest_streak(trade_results, False),
    )


def build_variants() -> list[Variant]:
    mins = (0.45, 0.46, 0.47)
    maxes = (0.55, 0.56, 0.57)
    secs = (15, 20, 25)
    moves = (0.5, 1.0, 2.0)
    bufs = (0.03, 0.05, 0.07)
    tps = (None, 0.80, 0.85, 0.90, 0.95)
    out: list[Variant] = []
    seq = 1
    for mn in mins:
        for mx in maxes:
            if mx <= mn:
                continue
            for sec in secs:
                for move in moves:
                    for buf in bufs:
                        for tp in tps:
                            out.append(
                                Variant(
                                    key=f"P{seq:04d}",
                                    entry_min_price=mn,
                                    entry_max_price=mx,
                                    entry_max_elapsed_sec=sec,
                                    move_from_open_min_usd=move,
                                    buy_price_buffer=buf,
                                    take_profit_price=tp,
                                )
                            )
                            seq += 1
    return out


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx(path: Path, sheets: dict[str, list[dict[str, object]]]) -> None:
    try:
        from openpyxl import Workbook
    except Exception:
        return
    wb = Workbook()
    first = True
    for name, rows in sheets.items():
        ws = wb.active if first else wb.create_sheet(name)
        ws.title = name
        first = False
        if not rows:
            continue
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row.get(header, "") for header in headers])
        ws.freeze_panes = "A2"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Sweep current S0184-style params on full-pool rolling-balance replay.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument("--start-balance", type=float, default=START_BALANCE_USD)
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_param_sweep_balance.csv",
    )
    parser.add_argument(
        "--best-csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_param_sweep_balance_best.csv",
    )
    parser.add_argument(
        "--xlsx-out",
        type=Path,
        default=repo_root / "reports" / "s0184_param_sweep_balance.xlsx",
    )
    args = parser.parse_args()

    windows = load_windows(args.input_dir)
    if not windows:
        raise SystemExit(f"no windows found under {args.input_dir}")

    rows: list[dict[str, object]] = []
    variants = build_variants()
    for idx, variant in enumerate(variants, start=1):
        stats = simulate_variant(windows, variant, start_balance_usd=float(args.start_balance))
        win_rate_pct = (100.0 * stats.wins / stats.trades) if stats.trades else 0.0
        trade_rate_pct = (100.0 * stats.trades / stats.windows) if stats.windows else 0.0
        row = {
            "rank_hint": idx,
            "variant_key": variant.key,
            "variant_label": variant.label,
            "entry_min_price": variant.entry_min_price,
            "entry_max_price": variant.entry_max_price,
            "entry_max_elapsed_sec": variant.entry_max_elapsed_sec,
            "move_from_open_min_usd": variant.move_from_open_min_usd,
            "buy_price_buffer": variant.buy_price_buffer,
            "take_profit_price": "" if variant.take_profit_price is None else variant.take_profit_price,
            "start_balance_usd": stats.start_balance_usd,
            "end_balance_usd": round(stats.end_balance_usd, 6),
            "total_pnl_usd": round(stats.total_pnl_usd, 6),
            "roi_pct": round((100.0 * (stats.end_balance_usd - stats.start_balance_usd) / stats.start_balance_usd), 4),
            "trades": stats.trades,
            "trade_rate_pct": round(trade_rate_pct, 4),
            "wins": stats.wins,
            "losses": stats.losses,
            "win_rate_pct": round(win_rate_pct, 4),
            "tp_hits": stats.tp_hits,
            "resolution_exits": stats.resolution_exits,
            "avg_order_size_usd": round(stats.avg_order_size_usd, 6),
            "peak_balance_usd": round(stats.peak_balance_usd, 6),
            "max_drawdown_usd": round(stats.max_drawdown_usd, 6),
            "max_drawdown_pct": round(stats.max_drawdown_pct, 4),
            "best_win_streak": stats.best_win_streak,
            "worst_loss_streak": stats.worst_loss_streak,
            "order_size_rule": "10pct_balance_min1",
        }
        rows.append(row)

    rows.sort(
        key=lambda row: (
            float(row["end_balance_usd"]),
            float(row["total_pnl_usd"]),
            -float(row["max_drawdown_pct"]),
            float(row["win_rate_pct"]),
        ),
        reverse=True,
    )
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank

    best_rows = rows[:50]
    write_csv(args.csv_out, rows)
    write_csv(args.best_csv_out, best_rows)
    write_xlsx(
        args.xlsx_out,
        {
            "best_50": best_rows,
            "all_variants": rows,
        },
    )

    best = best_rows[0]
    print(
        f"best={best['variant_key']} end_balance={best['end_balance_usd']} pnl={best['total_pnl_usd']} "
        f"dd={best['max_drawdown_pct']}% wr={best['win_rate_pct']} trades={best['trades']}"
    )
    print(f"Wrote {args.csv_out}")
    print(f"Wrote {args.best_csv_out}")
    print(f"Wrote {args.xlsx_out}")


if __name__ == "__main__":
    main()
