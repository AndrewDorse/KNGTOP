from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path

WINDOW_SEC = 300
ORDER_CUTOFF_SEC = 280
MAX_BUDGET_USD = 30.0
MIN_ORDER_USD = 1.0
MAX_ORDERS_PER_SIDE = 5
DEFAULT_SAMPLE_SIZE = 100
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
    seed_mode: str
    seed_price_min: float
    seed_price_max: float
    seed_move_min: float
    seed_persist_sec: int
    seed_order_usd: float
    both_sum_cap: float | None
    pair_order_usd: float
    hedge_price_cap: float
    target_roi: float
    rebalance_mult: float
    max_order_usd: float
    imbalance_slack_usd: float


@dataclass(slots=True)
class PositionState:
    spent_up: float = 0.0
    spent_down: float = 0.0
    shares_up: float = 0.0
    shares_down: float = 0.0
    orders_up: int = 0
    orders_down: int = 0
    action_count: int = 0

    @property
    def spent_total(self) -> float:
        return self.spent_up + self.spent_down

    def pnl_if_up(self) -> float:
        return self.shares_up - self.spent_total

    def pnl_if_down(self) -> float:
        return self.shares_down - self.spent_total


def slug_epoch(slug: str) -> int:
    m = _SLUG_EP.search((slug or "").strip())
    return int(m.group(1)) if m else 0


def load_windows(input_dir: Path, sample_size: int) -> list[WindowData]:
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
        last_down = 0.5
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
            else:
                btc_volume = 0.0
            ticks.append(
                Tick(
                    elapsed_sec=elapsed,
                    up_price=last_up,
                    down_price=last_down,
                    btc_price=last_btc,
                    btc_volume=max(0.0, btc_volume),
                )
            )
        if ticks[0].btc_price <= 0 or any(t.btc_price <= 0 for t in ticks[:5]):
            continue
        out.append(WindowData(slug=slug, path=path, question=question, ticks=tuple(ticks)))
    out.sort(key=lambda win: slug_epoch(win.slug), reverse=True)
    return out[:sample_size]


def winner_side(win: WindowData, tick_index: int) -> str | None:
    delta = win.ticks[tick_index].btc_price - win.ticks[0].btc_price
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def opposite_side(side: str) -> str:
    return "DOWN" if side == "UP" else "UP"


def side_price(tick: Tick, side: str) -> float:
    return tick.up_price if side == "UP" else tick.down_price


def side_sign(side: str) -> float:
    return 1.0 if side == "UP" else -1.0


def side_move(win: WindowData, tick_index: int, side: str, lookback_sec: int = 10) -> float:
    j = max(0, tick_index - int(lookback_sec))
    return side_sign(side) * (win.ticks[tick_index].btc_price - win.ticks[j].btc_price)


def persist_winner_ok(win: WindowData, tick_index: int, side: str, persist_sec: int) -> bool:
    if persist_sec <= 1:
        return True
    start = max(0, tick_index - persist_sec + 1)
    for i in range(start, tick_index + 1):
        if winner_side(win, i) != side:
            return False
    return True


def can_buy(state: PositionState, side: str, amount_usd: float) -> bool:
    if amount_usd + 1e-12 < MIN_ORDER_USD:
        return False
    if state.spent_total + amount_usd > MAX_BUDGET_USD + 1e-12:
        return False
    if side == "UP":
        return state.orders_up < MAX_ORDERS_PER_SIDE
    return state.orders_down < MAX_ORDERS_PER_SIDE


def apply_buy(state: PositionState, side: str, price: float, amount_usd: float) -> None:
    if side == "UP":
        state.spent_up += amount_usd
        state.shares_up += amount_usd / price
        state.orders_up += 1
    else:
        state.spent_down += amount_usd
        state.shares_down += amount_usd / price
        state.orders_down += 1
    state.action_count += 1


def target_amount_for_side(
    state: PositionState,
    side: str,
    price: float,
    target_roi: float,
    rebalance_mult: float,
    max_order_usd: float,
    imbalance_slack_usd: float,
) -> float:
    spent = state.spent_total
    pnl_side = state.pnl_if_up() if side == "UP" else state.pnl_if_down()
    pnl_other = state.pnl_if_down() if side == "UP" else state.pnl_if_up()
    denom = (1.0 / price) - 1.0 - target_roi
    if denom <= 1e-12:
        return 0.0
    need_to_target = max(0.0, (target_roi * spent - pnl_side) / denom)
    equalize_raw = max(0.0, price * (pnl_other - pnl_side))
    desired = max(need_to_target, equalize_raw * rebalance_mult)
    if desired < MIN_ORDER_USD - 1e-12:
        desired = 0.0
    cap_by_other = max(0.0, (pnl_other - target_roi * spent + imbalance_slack_usd) / (1.0 + target_roi))
    desired = min(desired, max_order_usd, cap_by_other, MAX_BUDGET_USD - spent)
    if desired + 1e-12 < MIN_ORDER_USD:
        return 0.0
    return desired


def realized_pnl(state: PositionState, up_won: bool) -> float:
    return state.pnl_if_up() if up_won else state.pnl_if_down()


def maybe_pair_buy(state: PositionState, tick: Tick, variant: Variant) -> bool:
    if variant.both_sum_cap is None:
        return False
    if tick.up_price + tick.down_price > variant.both_sum_cap + 1e-12:
        return False
    amount = min(variant.pair_order_usd, MAX_BUDGET_USD - state.spent_total)
    if amount + 1e-12 < MIN_ORDER_USD:
        return False
    if not can_buy(state, "UP", amount) or not can_buy(state, "DOWN", amount):
        return False
    apply_buy(state, "UP", tick.up_price, amount)
    apply_buy(state, "DOWN", tick.down_price, amount)
    return True


def maybe_seed_buy(state: PositionState, win: WindowData, tick_index: int, variant: Variant) -> bool:
    if state.orders_up + state.orders_down > 0:
        return False
    tick = win.ticks[tick_index]
    if maybe_pair_buy(state, tick, variant):
        return True
    if variant.seed_mode == "both":
        return False
    side = winner_side(win, tick_index)
    if side is None:
        return False
    price = side_price(tick, side)
    if price < variant.seed_price_min - 1e-12 or price > variant.seed_price_max + 1e-12:
        return False
    if side_move(win, tick_index, side, 10) + 1e-12 < variant.seed_move_min:
        return False
    if not persist_winner_ok(win, tick_index, side, variant.seed_persist_sec):
        return False
    amount = min(variant.seed_order_usd, MAX_BUDGET_USD - state.spent_total)
    if not can_buy(state, side, amount):
        return False
    apply_buy(state, side, price, amount)
    return True


def maybe_hedge_buy(state: PositionState, tick: Tick, variant: Variant) -> bool:
    if state.spent_total + 1e-12 < MIN_ORDER_USD:
        return False
    pnl_up = state.pnl_if_up()
    pnl_down = state.pnl_if_down()
    target_side = "UP" if pnl_up < pnl_down else "DOWN"
    price = side_price(tick, target_side)
    if price > variant.hedge_price_cap + 1e-12:
        return False
    amount = target_amount_for_side(
        state=state,
        side=target_side,
        price=price,
        target_roi=variant.target_roi,
        rebalance_mult=variant.rebalance_mult,
        max_order_usd=variant.max_order_usd,
        imbalance_slack_usd=variant.imbalance_slack_usd,
    )
    if not can_buy(state, target_side, amount):
        return False
    apply_buy(state, target_side, price, amount)
    return True


def simulate_window(win: WindowData, variant: Variant) -> dict[str, object]:
    state = PositionState()
    for idx, tick in enumerate(win.ticks):
        if tick.elapsed_sec > ORDER_CUTOFF_SEC:
            break
        acted = maybe_seed_buy(state, win, idx, variant)
        if not acted:
            if maybe_pair_buy(state, tick, variant):
                acted = True
        if not acted:
            maybe_hedge_buy(state, tick, variant)
    up_realized = state.pnl_if_up()
    down_realized = state.pnl_if_down()
    spent = state.spent_total
    realized = realized_pnl(state, win.ticks[-1].btc_price > win.ticks[0].btc_price)
    guaranteed = min(up_realized, down_realized)
    guaranteed_roi = guaranteed / spent if spent > 1e-12 else 0.0
    return {
        "slug": win.slug,
        "question": win.question,
        "spent_total": round(spent, 6),
        "orders_up": state.orders_up,
        "orders_down": state.orders_down,
        "shares_up": round(state.shares_up, 6),
        "shares_down": round(state.shares_down, 6),
        "pnl_if_up": round(up_realized, 6),
        "pnl_if_down": round(down_realized, 6),
        "realized_pnl": round(realized, 6),
        "guaranteed_pnl": round(guaranteed, 6),
        "guaranteed_roi_pct": round(100.0 * guaranteed_roi, 4) if spent > 1e-12 else 0.0,
        "both_positive": int(up_realized > 1e-12 and down_realized > 1e-12),
        "both_10roi": int(spent > 1e-12 and up_realized >= 0.10 * spent - 1e-12 and down_realized >= 0.10 * spent - 1e-12),
        "traded": int(spent > 1e-12),
    }


def summarize_variant(wins: list[WindowData], variant: Variant) -> tuple[dict[str, object], list[dict[str, object]]]:
    details: list[dict[str, object]] = []
    total_realized = 0.0
    total_guaranteed = 0.0
    total_spent = 0.0
    traded = 0
    both_positive = 0
    both_10roi = 0
    for win in wins:
        detail = simulate_window(win, variant)
        details.append(detail)
        total_realized += float(detail["realized_pnl"])
        total_guaranteed += float(detail["guaranteed_pnl"])
        total_spent += float(detail["spent_total"])
        traded += int(detail["traded"])
        both_positive += int(detail["both_positive"])
        both_10roi += int(detail["both_10roi"])
    row = {
        "variant_key": variant.key,
        "variant_label": variant.label,
        "windows": len(wins),
        "traded_windows": traded,
        "trade_rate_pct": round(100.0 * traded / len(wins), 4) if wins else 0.0,
        "total_spent_usd": round(total_spent, 6),
        "avg_spent_per_window": round(total_spent / len(wins), 6) if wins else 0.0,
        "realized_total_pnl_usd": round(total_realized, 6),
        "realized_roi_pct": round(100.0 * total_realized / total_spent, 4) if total_spent > 1e-12 else 0.0,
        "guaranteed_total_pnl_usd": round(total_guaranteed, 6),
        "guaranteed_roi_pct": round(100.0 * total_guaranteed / total_spent, 4) if total_spent > 1e-12 else 0.0,
        "both_positive_windows": both_positive,
        "both_10roi_windows": both_10roi,
        "seed_mode": variant.seed_mode,
        "seed_band": f"{variant.seed_price_min:.2f}-{variant.seed_price_max:.2f}",
        "seed_move_min": variant.seed_move_min,
        "seed_persist_sec": variant.seed_persist_sec,
        "seed_order_usd": variant.seed_order_usd,
        "both_sum_cap": "" if variant.both_sum_cap is None else variant.both_sum_cap,
        "pair_order_usd": variant.pair_order_usd,
        "hedge_price_cap": variant.hedge_price_cap,
        "target_roi_pct": round(100.0 * variant.target_roi, 4),
        "rebalance_mult": variant.rebalance_mult,
        "max_order_usd": variant.max_order_usd,
        "imbalance_slack_usd": variant.imbalance_slack_usd,
        "notes": "KILEMO_2 dynamic hedge search on last 100 BTC 5m windows. Uses current public PM prices only; CSV has no separate bid ladder.",
    }
    return row, details


def generate_variants() -> list[Variant]:
    seed_modes = ("winner", "both", "winner_or_both")
    seed_bands = ((0.30, 0.40), (0.32, 0.45), (0.35, 0.50))
    seed_moves = (0.5, 2.0)
    seed_persists = (1, 3)
    seed_orders = (1.0, 2.0)
    both_sum_caps = (None, 0.96, 0.99)
    pair_orders = (1.0, 2.0)
    hedge_caps = (0.22, 0.28, 0.35, 0.42)
    target_rois = (0.0, 0.10)
    rebalance_mults = (0.6, 1.0)
    max_orders = (2.0, 4.0)
    slack_values = (0.5, 1.5)
    out: list[Variant] = []
    seq = 1
    for seed_mode in seed_modes:
        for seed_min, seed_max in seed_bands:
            for seed_move in seed_moves:
                for seed_persist in seed_persists:
                    for seed_order in seed_orders:
                        for both_sum_cap in both_sum_caps:
                            if seed_mode == "both" and both_sum_cap is None:
                                continue
                            if seed_mode == "winner" and both_sum_cap is not None:
                                continue
                            for pair_order in pair_orders:
                                for hedge_cap in hedge_caps:
                                    for target_roi in target_rois:
                                        for rebalance_mult in rebalance_mults:
                                            for max_order_usd in max_orders:
                                                for slack in slack_values:
                                                    label_parts = [
                                                        f"seed={seed_mode}",
                                                        f"band={seed_min:.2f}-{seed_max:.2f}",
                                                        f"move10>={seed_move:.1f}",
                                                        f"persist={seed_persist}",
                                                        f"seed${seed_order:.0f}",
                                                        f"hedge<={hedge_cap:.2f}",
                                                        f"target={int(target_roi * 100)}%",
                                                        f"reb={rebalance_mult:.1f}",
                                                        f"max${max_order_usd:.0f}",
                                                        f"slack${slack:.1f}",
                                                    ]
                                                    if both_sum_cap is not None:
                                                        label_parts.append(f"sum<={both_sum_cap:.2f}")
                                                        label_parts.append(f"pair${pair_order:.0f}")
                                                    out.append(
                                                        Variant(
                                                            key=f"H{seq:04d}",
                                                            label=" ".join(label_parts),
                                                            seed_mode=seed_mode,
                                                            seed_price_min=seed_min,
                                                            seed_price_max=seed_max,
                                                            seed_move_min=seed_move,
                                                            seed_persist_sec=seed_persist,
                                                            seed_order_usd=seed_order,
                                                            both_sum_cap=both_sum_cap,
                                                            pair_order_usd=pair_order,
                                                            hedge_price_cap=hedge_cap,
                                                            target_roi=target_roi,
                                                            rebalance_mult=rebalance_mult,
                                                            max_order_usd=max_order_usd,
                                                            imbalance_slack_usd=slack,
                                                        )
                                                    )
                                                    seq += 1
    return out


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx_if_available(summary_rows: list[dict[str, object]], detail_rows: list[dict[str, object]], out_path: Path) -> None:
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
    ws2 = wb.create_sheet("detail_top")
    ws2.append(list(detail_rows[0].keys()))
    for row in detail_rows:
        ws2.append([row[key] for key in detail_rows[0].keys()])
    wb.save(out_path.with_suffix(".xlsx"))


def run_search(input_dir: Path, sample_size: int, out_summary: Path, out_detail: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    wins = load_windows(input_dir, sample_size=sample_size)
    if len(wins) < sample_size:
        raise RuntimeError(f"Need {sample_size} usable windows, found {len(wins)}")
    variants = generate_variants()
    summary_rows: list[dict[str, object]] = []
    top_variant_details: list[dict[str, object]] = []
    positive_details_cache: dict[str, list[dict[str, object]]] = {}
    for variant in variants:
        row, details = summarize_variant(wins, variant)
        if float(row["realized_total_pnl_usd"]) > 0.0:
            summary_rows.append(row)
            positive_details_cache[str(row["variant_key"])] = details
    if not summary_rows:
        raise RuntimeError("No positive realized PnL variants found.")
    summary_rows.sort(
        key=lambda row: (
            float(row["guaranteed_total_pnl_usd"]),
            float(row["realized_total_pnl_usd"]),
            float(row["both_10roi_windows"]),
            float(row["trade_rate_pct"]),
        ),
        reverse=True,
    )
    top_keys = [str(row["variant_key"]) for row in summary_rows[:20]]
    for key in top_keys:
        for rank, detail in enumerate(positive_details_cache[key], start=1):
            top_variant_details.append({"variant_key": key, "window_rank": rank, **detail})
    write_csv(out_summary, summary_rows)
    write_csv(out_detail, top_variant_details)
    write_xlsx_if_available(summary_rows, top_variant_details, out_summary)
    print("Top positive variants:")
    for row in summary_rows[:15]:
        print(
            f"  {row['variant_key']}: guaranteed={float(row['guaranteed_total_pnl_usd']):+.2f} "
            f"realized={float(row['realized_total_pnl_usd']):+.2f} both10={int(row['both_10roi_windows'])} "
            f"trade_rate={float(row['trade_rate_pct']):.2f}% {row['variant_label']}"
        )
    return summary_rows, top_variant_details


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    git_root = repo_root.parent
    parser = argparse.ArgumentParser(description="Search KILEMO_2 hedge variants on last 100 BTC 5m windows.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=git_root / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m",
    )
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument(
        "--out-summary",
        type=Path,
        default=repo_root / "reports" / "kilemo2_hedge_search_positive_sheet.csv",
    )
    parser.add_argument(
        "--out-detail",
        type=Path,
        default=repo_root / "reports" / "kilemo2_hedge_search_positive_detail.csv",
    )
    args = parser.parse_args()
    run_search(args.input_dir, args.sample_size, args.out_summary, args.out_detail)


if __name__ == "__main__":
    main()
