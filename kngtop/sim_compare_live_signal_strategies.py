from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from kngtop.live_kilemo1 import evaluate_signal as current_strategy_signal

WINDOW_SEC = 300
DEFAULT_SAMPLE_SIZES = ("100", "200", "400", "1000", "full")
ORDER_CUTOFF_SEC = 280
NOTIONAL_USD = 1.0
FILL_DELAY_SEC = 2
CURRENT_LIMIT_PX = 0.25
RECLAIM_MIN_ELAPSED = 30
RECLAIM_LOOKBACK_SEC = 40
RECLAIM_PRICE_MIN = 0.01
RECLAIM_PRICE_MAX = 0.30
RECLAIM_GAP_MIN = 0.05
CWC_MIN_ELAPSED = 20
CWC_PRICE_MIN = 0.01
CWC_PRICE_MAX = 0.25
CWC_MOMENTUM_LOOKBACK_SEC = 5
CWC_MOMENTUM_BPS_MIN = 0.0
_SLUG_EP = re.compile(r"-(\d+)$")


@dataclass(frozen=True, slots=True)
class Tick:
    elapsed_sec: int
    up_price: float
    down_price: float
    btc_price: float
    btc_volume: float
    btc_trade_count: int


@dataclass(frozen=True, slots=True)
class WindowData:
    slug: str
    path: Path
    question: str
    ticks: tuple[Tick, ...]


@dataclass(frozen=True, slots=True)
class TradeResult:
    strategy_key: str
    strategy_label: str
    traded: bool
    side: str
    signal_t: int | None
    entry_px: float | None
    pnl_usd: float
    outcome: str
    reason: str


def slug_epoch(slug: str) -> int:
    m = _SLUG_EP.search((slug or "").strip())
    return int(m.group(1)) if m else 0


def load_windows(input_dir: Path) -> list[WindowData]:
    paths = sorted(input_dir.glob("*_btc-updown-5m-*_prices.csv"))
    out: list[WindowData] = []
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            continue
        rows_by_elapsed: dict[int, dict[str, str]] = {}
        for row in rows:
            try:
                elapsed = int(float(row.get("elapsed_sec") or 0))
            except (TypeError, ValueError):
                continue
            if 0 <= elapsed < WINDOW_SEC:
                rows_by_elapsed[elapsed] = row
        if not rows_by_elapsed:
            continue
        slug = str(rows[0].get("slug") or path.stem)
        question = str(rows[0].get("question") or "")
        last_up = 0.5
        last_down = 0.5
        last_btc = 0.0
        ticks: list[Tick] = []
        for elapsed in range(WINDOW_SEC):
            row = rows_by_elapsed.get(elapsed)
            if row is not None:
                try:
                    raw = row.get("up_price")
                    if raw not in (None, ""):
                        last_up = float(raw)
                    raw = row.get("down_price")
                    if raw not in (None, ""):
                        last_down = float(raw)
                    raw = row.get("btc_price")
                    if raw not in (None, ""):
                        last_btc = float(raw)
                except (TypeError, ValueError):
                    pass
                try:
                    btc_volume = float(row.get("btc_volume") or 0.0)
                except (TypeError, ValueError):
                    btc_volume = 0.0
                try:
                    btc_trade_count = int(float(row.get("btc_trade_count") or 0))
                except (TypeError, ValueError):
                    btc_trade_count = 0
            else:
                btc_volume = 0.0
                btc_trade_count = 0
            ticks.append(
                Tick(
                    elapsed_sec=elapsed,
                    up_price=last_up,
                    down_price=last_down,
                    btc_price=last_btc,
                    btc_volume=max(0.0, btc_volume),
                    btc_trade_count=max(0, btc_trade_count),
                )
            )
        if ticks[0].btc_price <= 0 or any(t.btc_price <= 0 for t in ticks[:5]):
            continue
        out.append(WindowData(slug=slug, path=path, question=question, ticks=tuple(ticks)))
    out.sort(key=lambda win: slug_epoch(win.slug), reverse=True)
    return out


def side_price(tick: Tick, side: str) -> float:
    return tick.up_price if side == "UP" else tick.down_price


def btc_outcome(win: WindowData, side: str, entry_px: float) -> tuple[float, str]:
    start_btc = win.ticks[0].btc_price
    end_btc = win.ticks[-1].btc_price
    shares = NOTIONAL_USD / entry_px
    if abs(end_btc - start_btc) < 1e-12:
        last_px = side_price(win.ticks[-1], side)
        return shares * last_px - NOTIONAL_USD, "tie_last_price"
    won = (end_btc > start_btc and side == "UP") or (end_btc < start_btc and side == "DOWN")
    return (shares - NOTIONAL_USD, "btc_dir_win") if won else (-NOTIONAL_USD, "btc_dir_lose")


def delayed_fill_price(win: WindowData, signal_t: int, side: str) -> tuple[int, float] | None:
    fill_t = int(signal_t) + int(FILL_DELAY_SEC)
    if fill_t >= len(win.ticks):
        return None
    tick = win.ticks[fill_t]
    return fill_t, side_price(tick, side)


def side_vs_open(win: WindowData, tick_index: int, side: str) -> float:
    sign = 1.0 if side == "UP" else -1.0
    return sign * (win.ticks[tick_index].btc_price - win.ticks[0].btc_price)


def side_aligned_move(win: WindowData, tick_index: int, side: str, lookback_sec: int) -> float:
    lookback = max(0, tick_index - int(lookback_sec))
    sign = 1.0 if side == "UP" else -1.0
    return sign * (win.ticks[tick_index].btc_price - win.ticks[lookback].btc_price)


def volume_ratio(win: WindowData, tick_index: int, lookback_sec: int) -> float:
    current = win.ticks[tick_index].btc_volume
    if lookback_sec <= 0:
        return 0.0
    lo = max(0, tick_index - lookback_sec)
    prev = [win.ticks[idx].btc_volume for idx in range(lo, tick_index)]
    if not prev:
        return 0.0
    avg_prev = sum(prev) / len(prev)
    if current <= 0.0 and avg_prev <= 0.0:
        return 0.0
    if avg_prev <= 0.0:
        return float("inf")
    return current / avg_prev


def winner_side(win: WindowData, tick_index: int) -> str | None:
    sv = side_vs_open(win, tick_index, "UP")
    if sv > 1e-12:
        return "UP"
    if sv < -1e-12:
        return "DOWN"
    return None


def current_strategy(win: WindowData) -> TradeResult:
    for idx, tick in enumerate(win.ticks):
        if tick.elapsed_sec > ORDER_CUTOFF_SEC:
            break
        decision = current_strategy_signal(
            window_open_px=win.ticks[0].btc_price,
            spot_px=tick.btc_price,
            mid_up=tick.up_price,
            mid_dn=tick.down_price,
            price_then_now_5s=(tick.btc_price, win.ticks[max(0, idx - 5)].btc_price),
            price_then_now_20s=(tick.btc_price, win.ticks[max(0, idx - 20)].btc_price),
            volume_ratio_20s=volume_ratio(win, idx, 20),
        )
        if decision is None:
            continue
        fill = delayed_fill_price(win, idx, decision.side)
        if fill is None:
            return TradeResult("current", "Current: close<=30 + vol20>=1.4x AND move20>=2", False, decision.side, idx, None, 0.0, "no_fill", "after_window")
        fill_t, entry_px = fill
        if entry_px > CURRENT_LIMIT_PX + 1e-12:
            return TradeResult("current", "Current: close<=30 + vol20>=1.4x AND move20>=2", False, decision.side, fill_t, entry_px, 0.0, "no_fill", "above_25c_tplus2")
        pnl, reason = btc_outcome(win, decision.side, entry_px)
        return TradeResult("current", "Current: close<=30 + vol20>=1.4x AND move20>=2", True, decision.side, fill_t, entry_px, pnl, "trade", reason)
    return TradeResult("current", "Current: close<=30 + vol20>=1.4x AND move20>=2", False, "", None, None, 0.0, "no_trade", "no_signal")


def reclaim_strategy(win: WindowData) -> TradeResult:
    for idx, tick in enumerate(win.ticks):
        if tick.elapsed_sec < RECLAIM_MIN_ELAPSED or tick.elapsed_sec > ORDER_CUTOFF_SEC:
            continue
        gap = abs(tick.up_price - tick.down_price)
        if gap + 1e-12 < RECLAIM_GAP_MIN:
            continue
        sv_open = side_vs_open(win, idx, "UP")
        if sv_open > 1e-12:
            side = "UP"
        elif sv_open < -1e-12:
            side = "DOWN"
        else:
            continue
        px = side_price(tick, side)
        if not (RECLAIM_PRICE_MIN <= px <= RECLAIM_PRICE_MAX):
            continue
        reclaimed = False
        lo = max(0, idx - RECLAIM_LOOKBACK_SEC)
        for j in range(lo, idx):
            past_btc = win.ticks[j].btc_price
            if side == "UP" and past_btc < win.ticks[0].btc_price:
                reclaimed = True
                break
            if side == "DOWN" and past_btc > win.ticks[0].btc_price:
                reclaimed = True
                break
        if not reclaimed:
            continue
        fill = delayed_fill_price(win, idx, side)
        if fill is None:
            return TradeResult("reclaim", "Reclaim: e30 + lookback40 + gap>=5c + band<=30c", False, side, idx, None, 0.0, "no_fill", "after_window")
        fill_t, fill_px = fill
        pnl, reason = btc_outcome(win, side, fill_px)
        return TradeResult("reclaim", "Reclaim: e30 + lookback40 + gap>=5c + band<=30c", True, side, fill_t, fill_px, pnl, "trade", reason)
    return TradeResult("reclaim", "Reclaim: e30 + lookback40 + gap>=5c + band<=30c", False, "", None, None, 0.0, "no_trade", "no_signal")


def cwc_strategy(win: WindowData) -> TradeResult:
    for idx, tick in enumerate(win.ticks):
        if tick.elapsed_sec < CWC_MIN_ELAPSED or tick.elapsed_sec > ORDER_CUTOFF_SEC:
            continue
        side = winner_side(win, idx)
        if side is None:
            continue
        px = side_price(tick, side)
        if not (CWC_PRICE_MIN <= px <= CWC_PRICE_MAX):
            continue
        hist_idx = max(0, idx - CWC_MOMENTUM_LOOKBACK_SEC)
        hist_btc = win.ticks[hist_idx].btc_price
        start_btc = win.ticks[0].btc_price
        momentum_bps = ((tick.btc_price - hist_btc) / start_btc) * 10_000.0 if start_btc > 0 else 0.0
        if side == "UP":
            if tick.btc_price <= start_btc or momentum_bps < CWC_MOMENTUM_BPS_MIN:
                continue
        else:
            if tick.btc_price >= start_btc or (-momentum_bps) < CWC_MOMENTUM_BPS_MIN:
                continue
        fill = delayed_fill_price(win, idx, side)
        if fill is None:
            return TradeResult("cwc", "CWC/CWM: e20 + band<=25c + winner + momentum5>=0bps", False, side, idx, None, 0.0, "no_fill", "after_window")
        fill_t, fill_px = fill
        pnl, reason = btc_outcome(win, side, fill_px)
        return TradeResult("cwc", "CWC/CWM: e20 + band<=25c + winner + momentum5>=0bps", True, side, fill_t, fill_px, pnl, "trade", reason)
    return TradeResult("cwc", "CWC/CWM: e20 + band<=25c + winner + momentum5>=0bps", False, "", None, None, 0.0, "no_trade", "no_signal")


STRATEGIES: tuple[Callable[[WindowData], TradeResult], ...] = (
    current_strategy,
    reclaim_strategy,
    cwc_strategy,
)


def summarize(sample: list[WindowData], strategy_fn: Callable[[WindowData], TradeResult]) -> tuple[dict[str, object], list[dict[str, object]]]:
    trades = wins = 0
    total_pnl = 0.0
    entry_sum = 0.0
    detail_rows: list[dict[str, object]] = []
    label = ""
    key = ""
    for rank, win in enumerate(sample, start=1):
        result = strategy_fn(win)
        label = result.strategy_label
        key = result.strategy_key
        if result.traded:
            trades += 1
            total_pnl += result.pnl_usd
            entry_sum += float(result.entry_px or 0.0)
            if result.pnl_usd > 1e-12:
                wins += 1
        detail_rows.append(
            {
                "strategy_key": result.strategy_key,
                "strategy_label": result.strategy_label,
                "sample_rank_newest": rank,
                "slug": win.slug,
                "prices_csv": win.path.name,
                "question": win.question,
                "traded": int(result.traded),
                "side": result.side,
                "signal_t": "" if result.signal_t is None else result.signal_t,
                "entry_px": "" if result.entry_px is None else round(float(result.entry_px), 6),
                "pnl_usd": round(result.pnl_usd, 6),
                "outcome": result.outcome,
                "reason": result.reason,
            }
        )
    summary_row = {
        "strategy_key": key,
        "strategy_label": label,
        "windows": len(sample),
        "trades": trades,
        "trade_rate_pct": round(100.0 * trades / len(sample), 4) if sample else 0.0,
        "win_rate_pct": round(100.0 * wins / trades, 4) if trades else 0.0,
        "total_pnl_usd": round(total_pnl, 6),
        "avg_pnl_per_trade": round(total_pnl / trades, 6) if trades else 0.0,
        "avg_pnl_per_window": round(total_pnl / len(sample), 6) if sample else 0.0,
        "avg_entry_px": round(entry_sum / trades, 6) if trades else 0.0,
        "notes": "Approximate fills from displayed side prices with a 2s delayed fill model; current keeps the 25c cap at t+2. Reclaim and CWC reconstructed from archived KNGTOP live rules.",
    }
    return summary_row, detail_rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx_if_available(summary_rows: list[dict[str, object]], detail_rows: list[dict[str, object]], summary_path: Path) -> Path | None:
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        return None
    xlsx_path = summary_path.with_suffix(".xlsx")
    wb = Workbook()
    ws = wb.active
    ws.title = "summary"
    ws.append(list(summary_rows[0].keys()))
    for row in summary_rows:
        ws.append([row[key] for key in summary_rows[0].keys()])
    ws2 = wb.create_sheet("detail_full")
    ws2.append(list(detail_rows[0].keys()))
    for row in detail_rows:
        ws2.append([row[key] for key in detail_rows[0].keys()])
    wb.save(xlsx_path)
    return xlsx_path


def parse_sample_sizes(raw: str, full_n: int) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for token in (part.strip().lower() for part in raw.split(",") if part.strip()):
        if token == "full":
            out.append(("full", full_n))
        else:
            n = int(token)
            if n <= full_n:
                out.append((str(n), n))
    seen: set[str] = set()
    deduped: list[tuple[str, int]] = []
    for label, n in out:
        if label in seen:
            continue
        seen.add(label)
        deduped.append((label, n))
    return deduped


def run_comparison(input_dir: Path, out_summary: Path, out_detail: Path, sample_sizes_raw: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    wins = load_windows(input_dir)
    if not wins:
        raise RuntimeError(f"No usable windows in {input_dir}")
    sample_specs = parse_sample_sizes(sample_sizes_raw, len(wins))
    if not sample_specs:
        sample_specs = [(label, int(label)) for label in DEFAULT_SAMPLE_SIZES[:-1] if int(label) <= len(wins)]
        sample_specs.append(("full", len(wins)))
    summary_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []
    for label, count in sample_specs:
        sample = wins[:count]
        for strategy_fn in STRATEGIES:
            summary_row, details = summarize(sample, strategy_fn)
            summary_row["sample_size"] = label
            summary_rows.append(summary_row)
            if label == "full":
                detail_rows.extend(details)
    write_csv(out_summary, summary_rows)
    write_csv(out_detail, detail_rows)
    write_xlsx_if_available(summary_rows, detail_rows, out_summary)
    return summary_rows, detail_rows


def print_rankings(summary_rows: list[dict[str, object]]) -> None:
    for sample_size in dict.fromkeys(row["sample_size"] for row in summary_rows):
        bucket = [row for row in summary_rows if row["sample_size"] == sample_size]
        bucket.sort(key=lambda row: (float(row["total_pnl_usd"]), float(row["win_rate_pct"])), reverse=True)
        print(f"Sample {sample_size}:")
        for row in bucket:
            print(
                f"  {row['strategy_key']}: pnl={float(row['total_pnl_usd']):+.2f} "
                f"trades={int(row['trades'])} trade_rate={float(row['trade_rate_pct']):.2f}% "
                f"wr={float(row['win_rate_pct']):.2f}% avg_entry={float(row['avg_entry_px']):.4f}"
            )
        print()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    git_root = repo_root.parent
    parser = argparse.ArgumentParser(description="Compare current KILEMO_1 live strategy vs reclaim and CWC/CWM on BTC 5m public windows.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=git_root / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument(
        "--out-summary",
        type=Path,
        default=repo_root / "reports" / "signal_strategy_compare_delay2_sheet.csv",
    )
    parser.add_argument(
        "--out-detail",
        type=Path,
        default=repo_root / "reports" / "signal_strategy_compare_delay2_detail.csv",
    )
    parser.add_argument("--sample-sizes", default="100,200,400,1000,full")
    args = parser.parse_args()
    summary_rows, _ = run_comparison(args.input_dir, args.out_summary, args.out_detail, args.sample_sizes)
    print_rankings(summary_rows)


if __name__ == "__main__":
    main()
