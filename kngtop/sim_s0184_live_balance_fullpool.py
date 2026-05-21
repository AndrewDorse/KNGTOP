from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from kngtop.live_kilemo2 import (
    BUY_FAK_PRICE,
    ENTRY_MAX_ELAPSED_SEC,
    ENTRY_MAX_PRICE,
    ENTRY_MIN_PRICE,
    EXIT_SELL_PRICE,
    MOVE_FROM_OPEN_MIN_USD,
    ORDER_SIZE_BALANCE_FRACTION,
)
from kngtop.sim_search_start_window_hold import load_windows, settle, side_price, winner_side

MIN_ORDER_NOTIONAL_USD = 1.0
START_BALANCE_USD = 10.0


@dataclass(frozen=True, slots=True)
class TradeResult:
    traded: bool
    side: str | None
    entry_tick: int | None
    entry_px: float | None
    notional_usd: float
    shares: float
    exit_mode: str | None
    exit_tick: int | None
    exit_px: float | None
    pnl_usd: float


def live_order_size(balance_usd: float) -> float:
    raw = max(MIN_ORDER_NOTIONAL_USD, float(balance_usd) * ORDER_SIZE_BALANCE_FRACTION)
    return min(float(balance_usd), raw) if balance_usd > 0 else 0.0


def entry_price_for_side(observed_ask_px: float) -> float:
    del observed_ask_px
    return min(0.99, BUY_FAK_PRICE)


def simulate_window(win, *, balance_usd: float, fixed_order_usd: float | None = None) -> TradeResult:
    if fixed_order_usd is None:
        notional_usd = live_order_size(balance_usd)
    else:
        notional_usd = min(float(balance_usd), max(0.0, float(fixed_order_usd))) if balance_usd > 0 else 0.0
    if notional_usd < 1e-12:
        return TradeResult(False, None, None, None, 0.0, 0.0, None, None, None, 0.0)

    for tick_index in range(min(int(ENTRY_MAX_ELAPSED_SEC) + 1, len(win.ticks))):
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

        entry_px = entry_price_for_side(observed_px)
        shares = notional_usd / entry_px
        if EXIT_SELL_PRICE is not None:
            for exit_tick in range(tick_index + 1, len(win.ticks)):
                held_px = side_price(win.ticks[exit_tick], side)
                if held_px + 1e-12 >= EXIT_SELL_PRICE:
                    pnl_usd = shares * EXIT_SELL_PRICE - notional_usd
                    return TradeResult(
                        True,
                        side,
                        tick_index,
                        entry_px,
                        notional_usd,
                        shares,
                        "tp",
                        exit_tick,
                        EXIT_SELL_PRICE,
                        pnl_usd,
                    )

        pnl_usd = settle(win, side, entry_px) * (notional_usd / 1.0)
        return TradeResult(
            True,
            side,
            tick_index,
            entry_px,
            notional_usd,
            shares,
            "resolution",
            len(win.ticks) - 1,
            None,
            pnl_usd,
        )

    return TradeResult(False, None, None, None, notional_usd, 0.0, None, None, None, 0.0)


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


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx(path: Path, summary_rows: list[dict[str, object]], trade_rows: list[dict[str, object]]) -> None:
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

    ws2 = wb.create_sheet("trades")
    trade_headers = list(trade_rows[0].keys()) if trade_rows else ["note"]
    ws2.append(trade_headers)
    for row in trade_rows:
        ws2.append([row.get(header, "") for header in trade_headers])
    ws2.freeze_panes = "A2"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Replay current live S0184 logic on the full BTC 5m public pool.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument("--start-balance", type=float, default=START_BALANCE_USD)
    parser.add_argument("--fixed-order-usd", type=float, default=None)
    parser.add_argument(
        "--summary-csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_live_balance_fullpool_summary.csv",
    )
    parser.add_argument(
        "--trades-csv-out",
        type=Path,
        default=repo_root / "reports" / "s0184_live_balance_fullpool_trades.csv",
    )
    parser.add_argument(
        "--xlsx-out",
        type=Path,
        default=repo_root / "reports" / "s0184_live_balance_fullpool.xlsx",
    )
    args = parser.parse_args()

    windows = load_windows(args.input_dir)
    if not windows:
        raise SystemExit(f"no windows found under {args.input_dir}")

    balance = float(args.start_balance)
    peak_balance = balance
    max_drawdown_usd = 0.0
    max_drawdown_pct = 0.0
    traded_flags: list[bool] = []
    win_flags: list[bool] = []
    trade_rows: list[dict[str, object]] = []
    tp_hits = 0
    resolution_exits = 0
    total_pnl = 0.0

    for idx, win in enumerate(windows, start=1):
        balance_before = balance
        result = simulate_window(win, balance_usd=balance_before, fixed_order_usd=args.fixed_order_usd)
        pnl = result.pnl_usd if result.traded else 0.0
        balance = balance_before + pnl
        peak_balance = max(peak_balance, balance)
        drawdown_usd = peak_balance - balance
        drawdown_pct = (100.0 * drawdown_usd / peak_balance) if peak_balance > 0 else 0.0
        max_drawdown_usd = max(max_drawdown_usd, drawdown_usd)
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
        if result.traded:
            total_pnl += pnl
            traded_flags.append(True)
            won = pnl > 1e-12
            win_flags.append(won)
            if result.exit_mode == "tp":
                tp_hits += 1
            elif result.exit_mode == "resolution":
                resolution_exits += 1
        else:
            traded_flags.append(False)

        trade_rows.append(
            {
                "index": idx,
                "slug": win.slug,
                "traded": int(result.traded),
                "side": result.side or "",
                "entry_tick": "" if result.entry_tick is None else result.entry_tick,
                "entry_px": "" if result.entry_px is None else round(result.entry_px, 6),
                "order_notional_usd": round(result.notional_usd, 6),
                "shares": round(result.shares, 6),
                "exit_mode": result.exit_mode or "",
                "exit_tick": "" if result.exit_tick is None else result.exit_tick,
                "exit_px": "" if result.exit_px is None else round(result.exit_px, 6),
                "pnl_usd": round(pnl, 6),
                "balance_before_usd": round(balance_before, 6),
                "balance_after_usd": round(balance, 6),
                "peak_balance_usd": round(peak_balance, 6),
                "drawdown_usd": round(drawdown_usd, 6),
                "drawdown_pct": round(drawdown_pct, 4),
            }
        )

    trades = sum(1 for row in trade_rows if row["traded"] == 1)
    wins = sum(1 for row in trade_rows if row["traded"] == 1 and float(row["pnl_usd"]) > 1e-12)
    losses = sum(1 for row in trade_rows if row["traded"] == 1 and float(row["pnl_usd"]) < -1e-12)
    trade_rate_pct = (100.0 * trades / len(windows)) if windows else 0.0
    win_rate_pct = (100.0 * wins / trades) if trades else 0.0
    avg_order_size = (
        sum(float(row["order_notional_usd"]) for row in trade_rows if row["traded"] == 1) / trades if trades else 0.0
    )
    best_win_streak = longest_streak([float(row["pnl_usd"]) > 1e-12 for row in trade_rows if row["traded"] == 1], True)
    worst_loss_streak = longest_streak([float(row["pnl_usd"]) < -1e-12 for row in trade_rows if row["traded"] == 1], True)

    summary_rows = [
        {
            "windows": len(windows),
            "start_balance_usd": round(float(args.start_balance), 6),
            "end_balance_usd": round(balance, 6),
            "total_pnl_usd": round(total_pnl, 6),
            "roi_pct": round((100.0 * (balance - float(args.start_balance)) / float(args.start_balance)), 4),
            "trades": trades,
            "trade_rate_pct": round(trade_rate_pct, 4),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(win_rate_pct, 4),
            "tp_hits": tp_hits,
            "resolution_exits": resolution_exits,
            "avg_order_size_usd": round(avg_order_size, 6),
            "peak_balance_usd": round(peak_balance, 6),
            "max_drawdown_usd": round(max_drawdown_usd, 6),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "best_win_streak": best_win_streak,
            "worst_loss_streak": worst_loss_streak,
            "entry_band": f"{ENTRY_MIN_PRICE:.2f}-{ENTRY_MAX_PRICE:.2f}",
            "entry_max_elapsed_sec": ENTRY_MAX_ELAPSED_SEC,
            "btc_move_min_usd": MOVE_FROM_OPEN_MIN_USD,
            "buy_fak_price": BUY_FAK_PRICE,
            "fixed_order_usd": "" if args.fixed_order_usd is None else round(float(args.fixed_order_usd), 6),
            "exit_sell_price": EXIT_SELL_PRICE,
            "order_size_rule": (
                "max(1, balance*0.10), capped by available balance in replay"
                if args.fixed_order_usd is None
                else f"fixed {float(args.fixed_order_usd):.2f}, capped by available balance in replay"
            ),
            "notes": "Replay of current live S0184 with fixed 0.58 FAK buy price and hold-to-resolution exit.",
        }
    ]

    write_csv(args.summary_csv_out, summary_rows)
    write_csv(args.trades_csv_out, trade_rows)
    write_xlsx(args.xlsx_out, summary_rows, trade_rows)

    print(
        f"windows={len(windows)} trades={trades} win_rate={win_rate_pct:.4f}% "
        f"start={float(args.start_balance):.2f} end={balance:.6f} pnl={total_pnl:.6f}"
    )
    print(
        f"peak={peak_balance:.6f} max_drawdown_usd={max_drawdown_usd:.6f} "
        f"max_drawdown_pct={max_drawdown_pct:.4f}% tp_hits={tp_hits}"
    )
    print(f"Wrote {args.summary_csv_out}")
    print(f"Wrote {args.trades_csv_out}")
    print(f"Wrote {args.xlsx_out}")


if __name__ == "__main__":
    main()
