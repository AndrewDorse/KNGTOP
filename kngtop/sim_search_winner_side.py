from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path

WINDOW_SEC = 300
FILL_DELAY_SEC = 2
ORDER_CUTOFF_SEC = 280
NOTIONAL_USD = 1.0
ENTRY_LIMIT_PX = 0.25
DEFAULT_SAMPLE_SIZES = ("100", "200", "400", "1000", "full")
_SLUG_EP = re.compile(r"-(\d+)$")


@dataclass(frozen=True, slots=True)
class Tick:
    elapsed_sec: int
    up_price: float
    down_price: float
    btc_price: float
    btc_volume: float


@dataclass(frozen=True, slots=True)
class WindowData:
    slug: str
    path: Path
    question: str
    ticks: tuple[Tick, ...]


@dataclass(frozen=True, slots=True)
class Variant:
    key: str
    label: str
    price_max: float
    close_max_usd: float | None
    volume_ratio_min: float | None
    volume_lookback_sec: int
    move_min_usd: float | None
    move_lookback_sec: int
    logic: str


def slug_epoch(slug: str) -> int:
    m = _SLUG_EP.search((slug or "").strip())
    return int(m.group(1)) if m else 0


def load_windows(input_dir: Path) -> list[WindowData]:
    out: list[WindowData] = []
    for path in sorted(input_dir.glob("*_btc-updown-5m-*_prices.csv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            continue
        by_elapsed: dict[int, dict[str, str]] = {}
        for row in rows:
            try:
                elapsed = int(float(row.get("elapsed_sec") or 0))
            except (TypeError, ValueError):
                continue
            if 0 <= elapsed < WINDOW_SEC:
                by_elapsed[elapsed] = row
        if not by_elapsed:
            continue
        slug = str(rows[0].get("slug") or path.stem)
        question = str(rows[0].get("question") or "")
        last_up = 0.5
        last_dn = 0.5
        last_btc = 0.0
        ticks: list[Tick] = []
        for elapsed in range(WINDOW_SEC):
            row = by_elapsed.get(elapsed)
            if row is not None:
                try:
                    raw = row.get("up_price")
                    if raw not in (None, ""):
                        last_up = float(raw)
                    raw = row.get("down_price")
                    if raw not in (None, ""):
                        last_dn = float(raw)
                    raw = row.get("btc_price")
                    if raw not in (None, ""):
                        last_btc = float(raw)
                except (TypeError, ValueError):
                    pass
                try:
                    btc_volume = float(row.get("btc_volume") or 0.0)
                except (TypeError, ValueError):
                    btc_volume = 0.0
            else:
                btc_volume = 0.0
            ticks.append(
                Tick(
                    elapsed_sec=elapsed,
                    up_price=last_up,
                    down_price=last_dn,
                    btc_price=last_btc,
                    btc_volume=max(0.0, btc_volume),
                )
            )
        if ticks[0].btc_price <= 0 or any(t.btc_price <= 0 for t in ticks[:5]):
            continue
        out.append(WindowData(slug=slug, path=path, question=question, ticks=tuple(ticks)))
    out.sort(key=lambda win: slug_epoch(win.slug), reverse=True)
    return out


def winner_side(win: WindowData, tick_index: int) -> str | None:
    delta = win.ticks[tick_index].btc_price - win.ticks[0].btc_price
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def side_price(tick: Tick, side: str) -> float:
    return tick.up_price if side == "UP" else tick.down_price


def side_sign(side: str) -> float:
    return 1.0 if side == "UP" else -1.0


def side_move(win: WindowData, tick_index: int, side: str, lookback_sec: int) -> float:
    j = max(0, tick_index - int(lookback_sec))
    return side_sign(side) * (win.ticks[tick_index].btc_price - win.ticks[j].btc_price)


def volume_ratio(win: WindowData, tick_index: int, lookback_sec: int) -> float:
    current = win.ticks[tick_index].btc_volume
    lo = max(0, tick_index - int(lookback_sec))
    prev = [win.ticks[i].btc_volume for i in range(lo, tick_index)]
    if not prev:
        return 0.0
    mean_prev = sum(prev) / len(prev)
    if current <= 0.0 and mean_prev <= 0.0:
        return 0.0
    if mean_prev <= 0.0:
        return float("inf")
    return current / mean_prev


def delayed_fill(win: WindowData, tick_index: int, side: str) -> tuple[int, float] | None:
    fill_t = tick_index + FILL_DELAY_SEC
    if fill_t >= len(win.ticks):
        return None
    return fill_t, side_price(win.ticks[fill_t], side)


def settle(win: WindowData, side: str, entry_px: float) -> tuple[float, str]:
    shares = NOTIONAL_USD / entry_px
    start_btc = win.ticks[0].btc_price
    end_btc = win.ticks[-1].btc_price
    if abs(end_btc - start_btc) < 1e-12:
        last_px = side_price(win.ticks[-1], side)
        return shares * last_px - NOTIONAL_USD, "tie_last_price"
    won = (end_btc > start_btc and side == "UP") or (end_btc < start_btc and side == "DOWN")
    return (shares - NOTIONAL_USD, "btc_dir_win") if won else (-NOTIONAL_USD, "btc_dir_lose")


def variant_passes(win: WindowData, tick_index: int, variant: Variant) -> tuple[bool, str | None]:
    side = winner_side(win, tick_index)
    if side is None:
        return False, None
    px = side_price(win.ticks[tick_index], side)
    if px < 0.01 - 1e-12 or px > variant.price_max + 1e-12:
        return False, side
    if variant.close_max_usd is not None:
        if abs(win.ticks[tick_index].btc_price - win.ticks[0].btc_price) > variant.close_max_usd + 1e-12:
            return False, side
    vol_ok = True
    move_ok = True
    if variant.volume_ratio_min is not None:
        vol_ok = volume_ratio(win, tick_index, variant.volume_lookback_sec) + 1e-12 >= variant.volume_ratio_min
    if variant.move_min_usd is not None:
        move_ok = side_move(win, tick_index, side, variant.move_lookback_sec) + 1e-12 >= variant.move_min_usd
    if variant.logic == "vol":
        return vol_ok, side
    if variant.logic == "move":
        return move_ok, side
    if variant.logic == "or":
        return (vol_ok or move_ok), side
    if variant.logic == "and":
        return (vol_ok and move_ok), side
    return True, side


def generate_variants() -> list[Variant]:
    price_maxes = (0.20, 0.25)
    close_caps = (None, 20.0, 50.0)
    vol_ratios = (1.3, 1.8)
    vol_lookbacks = (10, 20, 30)
    move_mins = (0.5, 2.0)
    move_lookbacks = (10, 20, 30)
    out: list[Variant] = []
    seq = 1
    for price_max in price_maxes:
        for close_cap in close_caps:
            out.append(
                Variant(
                    key=f"W{seq:04d}",
                    label=f"winner px<={price_max:.2f}" + ("" if close_cap is None else f" close<={close_cap:.0f}"),
                    price_max=price_max,
                    close_max_usd=close_cap,
                    volume_ratio_min=None,
                    volume_lookback_sec=0,
                    move_min_usd=None,
                    move_lookback_sec=0,
                    logic="base",
                )
            )
            seq += 1
            for vr in vol_ratios:
                for vlb in vol_lookbacks:
                    out.append(
                        Variant(
                            key=f"W{seq:04d}",
                            label=f"winner px<={price_max:.2f} vol{vlb}>={vr:.1f}x" + ("" if close_cap is None else f" close<={close_cap:.0f}"),
                            price_max=price_max,
                            close_max_usd=close_cap,
                            volume_ratio_min=vr,
                            volume_lookback_sec=vlb,
                            move_min_usd=None,
                            move_lookback_sec=0,
                            logic="vol",
                        )
                    )
                    seq += 1
            for mm in move_mins:
                for mlb in move_lookbacks:
                    out.append(
                        Variant(
                            key=f"W{seq:04d}",
                            label=f"winner px<={price_max:.2f} move{mlb}>={mm:.1f}" + ("" if close_cap is None else f" close<={close_cap:.0f}"),
                            price_max=price_max,
                            close_max_usd=close_cap,
                            volume_ratio_min=None,
                            volume_lookback_sec=0,
                            move_min_usd=mm,
                            move_lookback_sec=mlb,
                            logic="move",
                        )
                    )
                    seq += 1
            for vr in vol_ratios:
                for vlb in vol_lookbacks:
                    for mm in move_mins:
                        for mlb in move_lookbacks:
                            for logic in ("or", "and"):
                                out.append(
                                    Variant(
                                        key=f"W{seq:04d}",
                                        label=f"winner px<={price_max:.2f} vol{vlb}>={vr:.1f}x {logic.upper()} move{mlb}>={mm:.1f}" + ("" if close_cap is None else f" close<={close_cap:.0f}"),
                                        price_max=price_max,
                                        close_max_usd=close_cap,
                                        volume_ratio_min=vr,
                                        volume_lookback_sec=vlb,
                                        move_min_usd=mm,
                                        move_lookback_sec=mlb,
                                        logic=logic,
                                    )
                                )
                                seq += 1
    return out


def summarize_variant(sample: list[WindowData], variant: Variant) -> tuple[dict[str, object], list[dict[str, object]]]:
    trades = wins = 0
    total_pnl = 0.0
    entry_sum = 0.0
    detail_rows: list[dict[str, object]] = []
    for rank, win in enumerate(sample, start=1):
        traded = False
        entry_px = None
        side = ""
        signal_t = None
        reason = "no_signal"
        pnl = 0.0
        for idx, tick in enumerate(win.ticks):
            if tick.elapsed_sec > ORDER_CUTOFF_SEC:
                break
            passed, chosen_side = variant_passes(win, idx, variant)
            if not passed or chosen_side is None:
                continue
            side = chosen_side
            signal_t = idx
            fill = delayed_fill(win, idx, side)
            if fill is None:
                reason = "after_window"
                break
            fill_t, fill_px = fill
            entry_px = fill_px
            signal_t = fill_t
            if fill_px > ENTRY_LIMIT_PX + 1e-12:
                reason = "above_25c_tplus2"
                break
            traded = True
            pnl, reason = settle(win, side, fill_px)
            break
        if traded:
            trades += 1
            total_pnl += pnl
            entry_sum += float(entry_px or 0.0)
            if pnl > 1e-12:
                wins += 1
        detail_rows.append(
            {
                "variant_key": variant.key,
                "variant_label": variant.label,
                "sample_rank_newest": rank,
                "slug": win.slug,
                "prices_csv": win.path.name,
                "question": win.question,
                "traded": int(traded),
                "side": side,
                "fill_t": "" if signal_t is None else signal_t,
                "entry_px": "" if entry_px is None else round(float(entry_px), 6),
                "pnl_usd": round(pnl, 6),
                "reason": reason,
            }
        )
    row = {
        "variant_key": variant.key,
        "variant_label": variant.label,
        "windows": len(sample),
        "trades": trades,
        "trade_rate_pct": round(100.0 * trades / len(sample), 4) if sample else 0.0,
        "win_rate_pct": round(100.0 * wins / trades, 4) if trades else 0.0,
        "total_pnl_usd": round(total_pnl, 6),
        "avg_pnl_per_trade": round(total_pnl / trades, 6) if trades else 0.0,
        "avg_pnl_per_window": round(total_pnl / len(sample), 6) if sample else 0.0,
        "avg_entry_px": round(entry_sum / trades, 6) if trades else 0.0,
        "notes": "Fresh winner-side search from scratch, $1 notional, 25c cap, t+2 delayed fill.",
    }
    return row, detail_rows


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


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx_if_available(summary_rows: list[dict[str, object]], detail_rows: list[dict[str, object]], out_summary: Path) -> None:
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        return
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
    wb.save(out_summary.with_suffix(".xlsx"))


def run_search(input_dir: Path, out_summary: Path, out_detail: Path, sample_sizes_raw: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    wins = load_windows(input_dir)
    if not wins:
        raise RuntimeError(f"No usable windows in {input_dir}")
    sample_specs = parse_sample_sizes(sample_sizes_raw, len(wins))
    if not sample_specs:
        sample_specs = [(x, int(x)) for x in DEFAULT_SAMPLE_SIZES[:-1] if int(x) <= len(wins)]
        sample_specs.append(("full", len(wins)))
    variants = generate_variants()
    summary_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []
    for label, n in sample_specs:
        sample = wins[:n]
        bucket: list[dict[str, object]] = []
        for variant in variants:
            row, details = summarize_variant(sample, variant)
            row["sample_size"] = label
            summary_rows.append(row)
            bucket.append(row)
            if label == "full":
                detail_rows.extend(details)
        bucket.sort(
            key=lambda row: (
                float(row["total_pnl_usd"]),
                1 if float(row["trade_rate_pct"]) >= 10.0 else 0,
                float(row["trade_rate_pct"]),
                float(row["win_rate_pct"]),
            ),
            reverse=True,
        )
        print(f"Top on sample {label}:")
        for row in bucket[:10]:
            print(
                f"  {row['variant_key']}: pnl={float(row['total_pnl_usd']):+.2f} "
                f"trades={int(row['trades'])} rate={float(row['trade_rate_pct']):.2f}% "
                f"wr={float(row['win_rate_pct']):.2f}% avg_entry={float(row['avg_entry_px']):.4f} "
                f"{row['variant_label']}"
            )
        print()
    write_csv(out_summary, summary_rows)
    write_csv(out_detail, detail_rows)
    write_xlsx_if_available(summary_rows, detail_rows, out_summary)
    return summary_rows, detail_rows


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    git_root = repo_root.parent
    parser = argparse.ArgumentParser(description="Fresh winner-side signal search on BTC 5m public windows.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=git_root / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument(
        "--out-summary",
        type=Path,
        default=repo_root / "reports" / "winner_side_search_sheet.csv",
    )
    parser.add_argument(
        "--out-detail",
        type=Path,
        default=repo_root / "reports" / "winner_side_search_detail.csv",
    )
    parser.add_argument("--sample-sizes", default="100,200,400,1000,full")
    args = parser.parse_args()
    run_search(args.input_dir, args.out_summary, args.out_detail, args.sample_sizes)


if __name__ == "__main__":
    main()
