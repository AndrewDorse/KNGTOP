from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests

WINDOW_SEC = 300
FILL_DELAY_SEC = 2
NOTIONAL_USD = 1.0
DEFAULT_SAMPLE_SIZES = ("100", "200", "400", "1000", "full")
_SLUG_EP = re.compile(r"-(\d+)$")
_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


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
    start_ms: int
    ticks: tuple[Tick, ...]


@dataclass(frozen=True, slots=True)
class CandleContext:
    prev1_side: str | None
    prev1_body_usd: float
    prev5_side: str | None
    prev5_body_usd: float


@dataclass(frozen=True, slots=True)
class Variant:
    key: str
    family: str
    label: str
    price_min: float
    price_max: float
    entry_max_sec: int
    move_from_open_min_usd: float
    volume_ratio_min: float | None
    volume_lookback_sec: int
    require_prewindow_alignment: bool
    prev1_body_min_usd: float
    prev5_body_min_usd: float


@dataclass(slots=True)
class SampleStats:
    windows: int
    trades: int = 0
    wins: int = 0
    total_pnl_usd: float = 0.0
    entry_sum: float = 0.0

    def record(self, *, traded: bool, pnl_usd: float, entry_px: float | None) -> None:
        if not traded or entry_px is None:
            return
        self.trades += 1
        self.total_pnl_usd += pnl_usd
        self.entry_sum += entry_px
        if pnl_usd > 1e-12:
            self.wins += 1

    @property
    def trade_rate_pct(self) -> float:
        return (100.0 * self.trades / self.windows) if self.windows else 0.0

    @property
    def win_rate_pct(self) -> float:
        return (100.0 * self.wins / self.trades) if self.trades else 0.0

    @property
    def avg_entry_px(self) -> float:
        return (self.entry_sum / self.trades) if self.trades else 0.0

    @property
    def avg_pnl_per_trade(self) -> float:
        return (self.total_pnl_usd / self.trades) if self.trades else 0.0

    @property
    def avg_pnl_per_window(self) -> float:
        return (self.total_pnl_usd / self.windows) if self.windows else 0.0


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
        out.append(
            WindowData(
                slug=slug,
                path=path,
                question=question,
                start_ms=slug_epoch(slug) * 1000,
                ticks=tuple(ticks),
            )
        )
    out.sort(key=lambda win: win.start_ms, reverse=True)
    return out


def fetch_binance_1m_klines(*, start_ms: int, end_ms: int, symbol: str = "BTCUSDT") -> dict[int, tuple[float, float]]:
    cursor = start_ms
    out: dict[int, tuple[float, float]] = {}
    while cursor < end_ms:
        response = requests.get(
            _BINANCE_KLINES,
            params={
                "symbol": symbol,
                "interval": "1m",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            try:
                open_time = int(row[0])
                open_px = float(row[1])
                close_px = float(row[4])
            except (TypeError, ValueError, IndexError):
                continue
            out[open_time] = (open_px, close_px)
        last_open_time = int(rows[-1][0])
        if last_open_time <= cursor:
            break
        cursor = last_open_time + 1
        if len(rows) < 1000:
            break
    return out


def candle_side_and_body(open_px: float, close_px: float) -> tuple[str | None, float]:
    if close_px > open_px:
        return "UP", close_px - open_px
    if close_px < open_px:
        return "DOWN", open_px - close_px
    return None, 0.0


def build_candle_contexts(
    windows: Iterable[WindowData],
    one_minute_candles: dict[int, tuple[float, float]],
) -> dict[int, CandleContext]:
    contexts: dict[int, CandleContext] = {}
    for win in windows:
        prev1 = one_minute_candles.get(win.start_ms - 60_000)
        prev5_rows = [
            one_minute_candles.get(win.start_ms - (offset * 60_000))
            for offset in range(5, 0, -1)
        ]
        if prev1 is None or any(row is None for row in prev5_rows):
            contexts[win.start_ms] = CandleContext(None, 0.0, None, 0.0)
            continue
        prev1_side, prev1_body = candle_side_and_body(prev1[0], prev1[1])
        prev5_open = prev5_rows[0][0]  # type: ignore[index]
        prev5_close = prev5_rows[-1][1]  # type: ignore[index]
        prev5_side, prev5_body = candle_side_and_body(prev5_open, prev5_close)
        contexts[win.start_ms] = CandleContext(prev1_side, prev1_body, prev5_side, prev5_body)
    return contexts


def winner_side(win: WindowData, tick_index: int) -> str | None:
    delta = win.ticks[tick_index].btc_price - win.ticks[0].btc_price
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def side_price(tick: Tick, side: str) -> float:
    return tick.up_price if side == "UP" else tick.down_price


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


def settle(win: WindowData, side: str, entry_px: float) -> float:
    shares = NOTIONAL_USD / entry_px
    start_btc = win.ticks[0].btc_price
    end_btc = win.ticks[-1].btc_price
    if abs(end_btc - start_btc) < 1e-12:
        last_px = side_price(win.ticks[-1], side)
        return shares * last_px - NOTIONAL_USD
    won = (end_btc > start_btc and side == "UP") or (end_btc < start_btc and side == "DOWN")
    return (shares - NOTIONAL_USD) if won else (-NOTIONAL_USD)


def variant_passes(win: WindowData, tick_index: int, variant: Variant, context: CandleContext | None) -> tuple[bool, str | None]:
    side = winner_side(win, tick_index)
    if side is None:
        return False, None
    tick = win.ticks[tick_index]
    px = side_price(tick, side)
    if px < variant.price_min - 1e-12 or px > variant.price_max + 1e-12:
        return False, side
    if abs(tick.btc_price - win.ticks[0].btc_price) + 1e-12 < variant.move_from_open_min_usd:
        return False, side
    if variant.volume_ratio_min is not None:
        if volume_ratio(win, tick_index, variant.volume_lookback_sec) + 1e-12 < variant.volume_ratio_min:
            return False, side
    if variant.require_prewindow_alignment:
        if context is None:
            return False, side
        if context.prev1_side != side or context.prev5_side != side:
            return False, side
        if context.prev1_body_usd + 1e-12 < variant.prev1_body_min_usd:
            return False, side
        if context.prev5_body_usd + 1e-12 < variant.prev5_body_min_usd:
            return False, side
    return True, side


def generate_variants() -> list[Variant]:
    out: list[Variant] = []
    seq = 1

    open_bands = (
        (0.42, 0.52),
        (0.44, 0.54),
        (0.45, 0.55),
        (0.46, 0.56),
        (0.48, 0.58),
    )
    for price_min, price_max in open_bands:
        for entry_max_sec in (5, 10, 15, 20):
            for move_min in (0.5, 1.0, 2.0, 4.0):
                out.append(
                    Variant(
                        key=f"S{seq:04d}",
                        family="open_follow",
                        label=f"winner {price_min:.2f}-{price_max:.2f} first{entry_max_sec}s move>={move_min:.1f}",
                        price_min=price_min,
                        price_max=price_max,
                        entry_max_sec=entry_max_sec,
                        move_from_open_min_usd=move_min,
                        volume_ratio_min=None,
                        volume_lookback_sec=0,
                        require_prewindow_alignment=False,
                        prev1_body_min_usd=0.0,
                        prev5_body_min_usd=0.0,
                    )
                )
                seq += 1
                for vol_min in (1.2, 1.5):
                    out.append(
                        Variant(
                            key=f"S{seq:04d}",
                            family="open_follow",
                            label=(
                                f"winner {price_min:.2f}-{price_max:.2f} first{entry_max_sec}s "
                                f"move>={move_min:.1f} vol10>={vol_min:.1f}x"
                            ),
                            price_min=price_min,
                            price_max=price_max,
                            entry_max_sec=entry_max_sec,
                            move_from_open_min_usd=move_min,
                            volume_ratio_min=vol_min,
                            volume_lookback_sec=10,
                            require_prewindow_alignment=False,
                            prev1_body_min_usd=0.0,
                            prev5_body_min_usd=0.0,
                        )
                    )
                    seq += 1

    kline_bands = (
        (0.44, 0.54),
        (0.45, 0.55),
        (0.46, 0.56),
    )
    for price_min, price_max in kline_bands:
        for entry_max_sec in (15, 20, 45):
            for move_min in (1.0, 2.0, 4.0):
                for prev1_body_min in (2.0, 5.0):
                    for prev5_body_min in (10.0, 20.0):
                        out.append(
                            Variant(
                                key=f"S{seq:04d}",
                                family="prewindow_kline_follow",
                                label=(
                                    f"winner {price_min:.2f}-{price_max:.2f} first{entry_max_sec}s "
                                    f"move>={move_min:.1f} prev1>={prev1_body_min:.0f} prev5>={prev5_body_min:.0f}"
                                ),
                                price_min=price_min,
                                price_max=price_max,
                                entry_max_sec=entry_max_sec,
                                move_from_open_min_usd=move_min,
                                volume_ratio_min=None,
                                volume_lookback_sec=0,
                                require_prewindow_alignment=True,
                                prev1_body_min_usd=prev1_body_min,
                                prev5_body_min_usd=prev5_body_min,
                            )
                        )
                        seq += 1
                        for vol_min in (1.2, 1.5, 2.0):
                            out.append(
                                Variant(
                                    key=f"S{seq:04d}",
                                    family="prewindow_kline_follow",
                                    label=(
                                        f"winner {price_min:.2f}-{price_max:.2f} first{entry_max_sec}s "
                                        f"move>={move_min:.1f} vol10>={vol_min:.1f}x "
                                        f"prev1>={prev1_body_min:.0f} prev5>={prev5_body_min:.0f}"
                                    ),
                                    price_min=price_min,
                                    price_max=price_max,
                                    entry_max_sec=entry_max_sec,
                                    move_from_open_min_usd=move_min,
                                    volume_ratio_min=vol_min,
                                    volume_lookback_sec=10,
                                    require_prewindow_alignment=True,
                                    prev1_body_min_usd=prev1_body_min,
                                    prev5_body_min_usd=prev5_body_min,
                                )
                            )
                            seq += 1
    return out


def parse_sample_sizes(raw: str, full_n: int) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for token in (part.strip().lower() for part in raw.split(",") if part.strip()):
        if token == "full":
            out.append(("full", full_n))
        else:
            n = int(token)
            if n <= full_n:
                out.append((str(n), n))
    if not out:
        out = [(label, full_n if label == "full" else int(label)) for label in DEFAULT_SAMPLE_SIZES if label == "full" or int(label) <= full_n]
    seen: set[str] = set()
    deduped: list[tuple[str, int]] = []
    for label, n in out:
        if label in seen:
            continue
        seen.add(label)
        deduped.append((label, n))
    return deduped


def gate_thresholds(label: str, full_n: int) -> tuple[int, float]:
    if label == "100":
        return 10, 55.0
    if label == "200":
        return 20, 55.0
    if label == "400":
        return 40, 55.0
    if label == "1000":
        return 100, 55.0
    if label == "full":
        return max(250, min(1000, full_n // 3)), 55.0
    return 0, 0.0


def evaluate_variant(
    windows: list[WindowData],
    variant: Variant,
    sample_specs: list[tuple[str, int]],
    contexts: dict[int, CandleContext],
) -> dict[str, object]:
    stats = {label: SampleStats(windows=n) for label, n in sample_specs}
    for window_index, win in enumerate(windows):
        traded = False
        entry_px: float | None = None
        pnl_usd = 0.0
        context = contexts.get(win.start_ms)
        for tick_index, tick in enumerate(win.ticks):
            if tick.elapsed_sec > variant.entry_max_sec:
                break
            passed, side = variant_passes(win, tick_index, variant, context)
            if not passed or side is None:
                continue
            fill = delayed_fill(win, tick_index, side)
            if fill is None:
                break
            _, fill_px = fill
            if fill_px < variant.price_min - 1e-12 or fill_px > variant.price_max + 1e-12:
                break
            traded = True
            entry_px = fill_px
            pnl_usd = settle(win, side, fill_px)
            break
        for label, n in sample_specs:
            if window_index < n:
                stats[label].record(traded=traded, pnl_usd=pnl_usd, entry_px=entry_px)

    row: dict[str, object] = {
        "variant_key": variant.key,
        "family": variant.family,
        "variant_label": variant.label,
        "notes": (
            "Single $1 buy on one side only, fill booked at t+2 from the public snapshot, "
            "hold to 5m expiry. Kline family reconstructs pre-window context from Binance 1m candles."
        ),
    }
    for label, n in sample_specs:
        sample = stats[label]
        row[f"windows_{label}"] = n
        row[f"trades_{label}"] = sample.trades
        row[f"trade_rate_pct_{label}"] = round(sample.trade_rate_pct, 4)
        row[f"win_rate_pct_{label}"] = round(sample.win_rate_pct, 4)
        row[f"total_pnl_usd_{label}"] = round(sample.total_pnl_usd, 6)
        row[f"avg_pnl_per_trade_{label}"] = round(sample.avg_pnl_per_trade, 6)
        row[f"avg_pnl_per_window_{label}"] = round(sample.avg_pnl_per_window, 6)
        row[f"avg_entry_px_{label}"] = round(sample.avg_entry_px, 6)
        min_trades, min_wr = gate_thresholds(label, len(windows))
        row[f"passes_gate_{label}"] = (
            sample.trades >= min_trades
            and sample.win_rate_pct + 1e-12 >= min_wr
            and sample.total_pnl_usd > 0.0
        )
    gate_labels = [label for label, _ in sample_specs if label in {"100", "200", "400", "1000"}]
    row["passes_progressive_1000_gate"] = all(bool(row[f"passes_gate_{label}"]) for label in gate_labels)
    row["passes_full_sanity"] = bool(row.get("passes_progressive_1000_gate")) and bool(row.get("passes_gate_full", False))
    return row


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx_if_available(
    summary_rows: list[dict[str, object]],
    best_rows: list[dict[str, object]],
    output_csv: Path,
) -> None:
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "best_realistic"
    ws.append(list(best_rows[0].keys()))
    for row in best_rows:
        ws.append([row[key] for key in best_rows[0].keys()])
    ws2 = wb.create_sheet("summary_all")
    ws2.append(list(summary_rows[0].keys()))
    for row in summary_rows:
        ws2.append([row[key] for key in summary_rows[0].keys()])
    wb.save(output_csv.with_suffix(".xlsx"))


def run_search(
    input_dir: Path,
    out_summary: Path,
    out_best: Path,
    sample_sizes_raw: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    windows = load_windows(input_dir)
    if not windows:
        raise RuntimeError(f"No usable windows in {input_dir}")
    sample_specs = parse_sample_sizes(sample_sizes_raw, len(windows))
    min_start_ms = min(win.start_ms for win in windows) - (6 * 60_000)
    max_start_ms = max(win.start_ms for win in windows) + 60_000
    candles_1m = fetch_binance_1m_klines(start_ms=min_start_ms, end_ms=max_start_ms)
    contexts = build_candle_contexts(windows, candles_1m)
    variants = generate_variants()
    summary_rows = [evaluate_variant(windows, variant, sample_specs, contexts) for variant in variants]
    summary_rows.sort(
        key=lambda row: (
            bool(row["passes_full_sanity"]),
            bool(row["passes_progressive_1000_gate"]),
            float(row.get("total_pnl_usd_1000", 0.0)),
            float(row.get("win_rate_pct_1000", 0.0)),
            float(row.get("trades_1000", 0.0)),
        ),
        reverse=True,
    )
    best_rows = [row for row in summary_rows if bool(row["passes_progressive_1000_gate"])]
    write_csv(out_summary, summary_rows)
    write_csv(out_best, best_rows)
    write_xlsx_if_available(summary_rows, best_rows, out_best)
    return summary_rows, best_rows


def print_rankings(rows: list[dict[str, object]], *, title: str, limit: int = 12) -> None:
    print(title)
    for row in rows[:limit]:
        print(
            f"  {row['variant_key']} | {row['family']} | "
            f"pnl1000={float(row.get('total_pnl_usd_1000', 0.0)):+.2f} "
            f"wr1000={float(row.get('win_rate_pct_1000', 0.0)):.2f}% "
            f"trades1000={int(row.get('trades_1000', 0))} "
            f"full_ok={bool(row.get('passes_full_sanity'))} | {row['variant_label']}"
        )
    print()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    git_root = repo_root.parent
    parser = argparse.ArgumentParser(description="Fresh BTC 5m start-window one-side hold search.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=git_root / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument(
        "--out-summary",
        type=Path,
        default=repo_root / "reports" / "start_window_hold_search_summary.csv",
    )
    parser.add_argument(
        "--out-best",
        type=Path,
        default=repo_root / "reports" / "start_window_hold_best_realistic.csv",
    )
    parser.add_argument("--sample-sizes", default="100,200,400,1000,full")
    args = parser.parse_args()

    summary_rows, best_rows = run_search(
        args.input_dir,
        args.out_summary,
        args.out_best,
        args.sample_sizes,
    )
    print_rankings(best_rows, title="Best realistic variants:")
    kline_recent_only = [
        row
        for row in summary_rows
        if row["family"] == "prewindow_kline_follow"
        and bool(row["passes_progressive_1000_gate"])
        and not bool(row["passes_full_sanity"])
    ]
    if kline_recent_only:
        print_rankings(kline_recent_only, title="Recent-only kline variants that failed the full sanity pass:", limit=8)


if __name__ == "__main__":
    main()
