from __future__ import annotations

import argparse
import csv
import glob
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import Callable

FEE_BUFFER_PER_TRADE = 0.018
DEFAULT_BUDGET = 20.0
DEFAULT_SHARE_SIZES = (5, 3, 2, 1)
CANCEL_AFTER_SEC = 240
WINDOW_END_SEC = 300
MIN_PRICE = 0.25
MAX_PRICE = 0.65
APPROXIMATE_NOTE = "approximate fills from displayed side prices; no historical bid/ask ladder in source dataset"


@dataclass(frozen=True, slots=True)
class Tick:
    timestamp: str
    elapsed_sec: int
    remaining_sec: int
    up_price: float
    down_price: float
    btc_price: float
    pm_volume: float
    btc_volume: float
    btc_quote_volume: float
    btc_trade_count: int


@dataclass(frozen=True, slots=True)
class WindowData:
    window_id: str
    start_time: str
    end_time: str
    ticks: tuple[Tick, ...]
    final_result: str


@dataclass(slots=True)
class Order:
    side: str
    price: float
    shares: int
    placed_time: int
    filled_time: int | None = None
    cancelled_time: int | None = None
    filled: bool = False


@dataclass(slots=True)
class Fill:
    side: str
    price: float
    shares: int
    elapsed_sec: int
    btc_move_10s: float | None
    btc_move_30s: float | None
    pm_volume: float


@dataclass(slots=True)
class WindowResult:
    window_id: str
    strategy_name: str
    variant: str
    share_size: int
    start_time: str
    end_time: str
    final_result: str
    orders_placed: int
    orders_filled: int
    up_shares: int
    down_shares: int
    avg_up: float | None
    avg_down: float | None
    avg_sum: float | None
    cost_gross: float
    gross_pnl: float
    net_pnl: float
    max_imbalance: int
    stopped_reason: str
    first_fill_time: int | None
    last_fill_time: int | None
    btc_move_10s_at_entry: float | None
    btc_move_30s_at_entry: float | None
    pm_volume_at_entry: float | None
    hold_time_seconds: int | None
    late_fills_count: int
    balanced: bool
    ideal_basket: bool
    good_basket: bool


@dataclass(slots=True)
class WindowState:
    budget: float
    share_size: int
    orders: list[Order] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    stopped_reason: str = ""
    trading_stopped: bool = False
    max_imbalance: int = 0
    notes: set[str] = field(default_factory=set)

    def filled_shares(self, side: str) -> int:
        return sum(fill.shares for fill in self.fills if fill.side == side)

    def avg_entry(self, side: str) -> float | None:
        fills = [fill for fill in self.fills if fill.side == side]
        if not fills:
            return None
        shares = sum(fill.shares for fill in fills)
        return sum(fill.price * fill.shares for fill in fills) / shares

    def avg_sum(self) -> float | None:
        avg_up = self.avg_entry("UP")
        avg_down = self.avg_entry("DOWN")
        if avg_up is None or avg_down is None:
            return None
        return avg_up + avg_down

    def open_orders(self, side: str | None = None) -> list[Order]:
        rows = [order for order in self.orders if not order.filled and order.cancelled_time is None]
        if side is None:
            return rows
        return [order for order in rows if order.side == side]

    def gross_cost(self) -> float:
        return sum(fill.price * fill.shares for fill in self.fills)

    def fee_cost(self) -> float:
        return sum(fill.shares * FEE_BUFFER_PER_TRADE for fill in self.fills)

    def reserved_budget(self) -> float:
        return self.gross_cost() + sum(order.price * order.shares for order in self.open_orders())

    def can_place(self, price: float, shares: int) -> bool:
        return (self.reserved_budget() + price * shares) <= (self.budget + 1e-9)

    def place_order(self, side: str, price: float, elapsed_sec: int) -> bool:
        if price <= 0 or price >= 1:
            return False
        if not self.can_place(price, self.share_size):
            return False
        self.orders.append(Order(side=side, price=price, shares=self.share_size, placed_time=elapsed_sec))
        return True

    def cancel_open_orders(self, elapsed_sec: int, reason: str) -> None:
        for order in self.open_orders():
            order.cancelled_time = elapsed_sec
        if reason and not self.stopped_reason:
            self.stopped_reason = reason

    def update_imbalance(self) -> None:
        imbalance = abs(self.filled_shares("UP") - self.filled_shares("DOWN"))
        self.max_imbalance = max(self.max_imbalance, imbalance)

    def projected_avg_sum_with(self, side: str, price: float, shares: int) -> float | None:
        current_shares = self.filled_shares(side)
        current_avg = self.avg_entry(side)
        other_side = "DOWN" if side == "UP" else "UP"
        other_avg = self.avg_entry(other_side)
        if current_avg is None:
            new_avg = price
        else:
            total_cost = current_avg * current_shares + price * shares
            new_avg = total_cost / (current_shares + shares)
        if other_avg is None:
            return None
        return new_avg + other_avg


def clamp_price(price: float, floor: float, ceiling: float) -> float:
    return round(max(floor, min(ceiling, price)), 2)


def load_windows(input_path: str) -> list[WindowData]:
    root = Path(input_path)
    files: list[Path]
    if root.is_file():
        files = [root]
    else:
        files = sorted(Path(p) for p in glob.glob(str(root / "*.csv")))
    windows: list[WindowData] = []
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        if not rows:
            continue
        rows.sort(key=lambda row: int(float(row.get("elapsed_sec") or 0)))
        ticks: list[Tick] = []
        for row in rows:
            try:
                up_price = float(row["up_price"])
                down_price = float(row["down_price"])
                btc_price = float(row.get("btc_price") or 0.0)
            except (KeyError, TypeError, ValueError):
                continue
            ticks.append(
                Tick(
                    timestamp=str(row.get("recorded_at") or row.get("timestamp") or ""),
                    elapsed_sec=int(float(row.get("elapsed_sec") or 0)),
                    remaining_sec=int(float(row.get("remaining_sec") or 0)),
                    up_price=up_price,
                    down_price=down_price,
                    btc_price=btc_price,
                    pm_volume=float(row.get("volume") or row.get("pm_volume") or row.get("btc_trade_count") or 0.0),
                    btc_volume=float(row.get("btc_volume") or 0.0),
                    btc_quote_volume=float(row.get("btc_quote_volume") or 0.0),
                    btc_trade_count=int(float(row.get("btc_trade_count") or 0)),
                )
            )
        if not ticks:
            continue
        slug = str(rows[0].get("slug") or path.stem)
        final_result = infer_final_result(ticks)
        windows.append(
            WindowData(
                window_id=slug,
                start_time=ticks[0].timestamp,
                end_time=ticks[-1].timestamp,
                ticks=tuple(ticks),
                final_result=final_result,
            )
        )
    return windows


def infer_final_result(ticks: tuple[Tick, ...]) -> str:
    last = ticks[-1]
    if last.up_price > last.down_price:
        return "UP"
    if last.down_price > last.up_price:
        return "DOWN"
    first = ticks[0]
    return "UP" if last.btc_price >= first.btc_price else "DOWN"


def btc_move(ticks: tuple[Tick, ...], current_index: int, lookback_sec: int) -> float | None:
    current = ticks[current_index]
    target_elapsed = current.elapsed_sec - lookback_sec
    if target_elapsed < 0:
        return None
    prior: Tick | None = None
    for idx in range(current_index, -1, -1):
        if ticks[idx].elapsed_sec <= target_elapsed:
            prior = ticks[idx]
            break
    if prior is None or prior.btc_price <= 0:
        return None
    return (current.btc_price - prior.btc_price) / prior.btc_price


def side_price(tick: Tick, side: str) -> float:
    return tick.up_price if side == "UP" else tick.down_price


def first_future_fill_time(window: WindowData, start_index: int, side: str, limit_price: float) -> int | None:
    for idx in range(start_index + 1, len(window.ticks)):
        tick = window.ticks[idx]
        if tick.elapsed_sec > CANCEL_AFTER_SEC:
            return None
        if tick.btc_volume <= 0 and tick.btc_quote_volume <= 0 and tick.btc_trade_count <= 0:
            continue
        if side_price(tick, side) <= limit_price + 1e-9:
            return tick.elapsed_sec
    return None


def process_fills(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    for order in state.open_orders():
        if tick.elapsed_sec <= order.placed_time:
            continue
        if tick.elapsed_sec > CANCEL_AFTER_SEC:
            continue
        if tick.btc_volume <= 0 and tick.btc_quote_volume <= 0 and tick.btc_trade_count <= 0:
            continue
        if side_price(tick, order.side) <= order.price + 1e-9:
            order.filled = True
            order.filled_time = tick.elapsed_sec
            state.fills.append(
                Fill(
                    side=order.side,
                    price=order.price,
                    shares=order.shares,
                    elapsed_sec=tick.elapsed_sec,
                    btc_move_10s=btc_move(window.ticks, tick_index, 10),
                    btc_move_30s=btc_move(window.ticks, tick_index, 30),
                    pm_volume=tick.pm_volume,
                )
            )
    state.update_imbalance()


def current_mid(tick: Tick) -> float:
    return (tick.up_price + (1.0 - tick.down_price)) / 2.0


def place_ladder(state: WindowState, tick: Tick, side: str, offsets: tuple[float, ...], floor: float = 0.30, ceiling: float = 0.65) -> None:
    price_now = side_price(tick, side)
    existing = {(order.side, order.price) for order in state.open_orders(side)}
    filled_levels = {(fill.side, fill.price) for fill in state.fills}
    for offset in offsets:
        price = clamp_price(price_now - offset, floor, ceiling)
        key = (side, price)
        if key in existing or key in filled_levels:
            continue
        if not state.place_order(side, price, tick.elapsed_sec):
            return


def strategy_deep_ladder(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    if tick.elapsed_sec == 0:
        place_ladder(state, tick, "UP", (0.03, 0.06))
        place_ladder(state, tick, "DOWN", (0.03, 0.06))
    if tick.elapsed_sec >= CANCEL_AFTER_SEC:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")


def strategy_rebalance_only_cheap(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    if tick.elapsed_sec == 0:
        place_ladder(state, tick, "UP", (0.03, 0.06))
        place_ladder(state, tick, "DOWN", (0.03, 0.06))
    up_shares = state.filled_shares("UP")
    down_shares = state.filled_shares("DOWN")
    imbalance_orders = abs(up_shares - down_shares) // state.share_size
    if imbalance_orders > 0 and imbalance_orders <= 2 and tick.elapsed_sec < CANCEL_AFTER_SEC:
        bigger = "UP" if up_shares > down_shares else "DOWN"
        smaller = "DOWN" if bigger == "UP" else "UP"
        bigger_avg = state.avg_entry(bigger)
        smaller_price = side_price(tick, smaller)
        cheap = (bigger_avg is not None and smaller_price <= bigger_avg - 0.02) or smaller_price <= 0.47
        if cheap:
            price = clamp_price(smaller_price - 0.01, 0.30, 0.65)
            if not state.open_orders(smaller):
                state.place_order(smaller, price, tick.elapsed_sec)
        for order in state.open_orders(bigger):
            order.cancelled_time = tick.elapsed_sec
    if tick.elapsed_sec >= CANCEL_AFTER_SEC:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")


def strategy_momentum_discount(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    move10 = btc_move(window.ticks, tick_index, 10)
    move30 = btc_move(window.ticks, tick_index, 30)
    strong = move10 is not None and abs(move10) >= 0.0008
    if tick.elapsed_sec < CANCEL_AFTER_SEC:
        if strong and move10 is not None and move10 > 0:
            primary, delayed = "DOWN", "UP"
        elif strong and move10 is not None and move10 < 0:
            primary, delayed = "UP", "DOWN"
        else:
            primary, delayed = "UP", "DOWN"
        primary_price = side_price(tick, primary)
        if primary_price <= 0.48 and not state.open_orders(primary):
            state.place_order(primary, clamp_price(primary_price - 0.01, 0.30, 0.65), tick.elapsed_sec)
        delayed_price = side_price(tick, delayed)
        avg_primary = state.avg_entry(primary)
        projected = None if avg_primary is None else avg_primary + max(0.0, delayed_price - 0.01)
        pullback = move30 is not None and ((primary == "DOWN" and move30 <= 0) or (primary == "UP" and move30 >= 0))
        if (pullback or delayed_price <= 0.48) and projected is not None and projected <= 0.99 and not state.open_orders(delayed):
            state.place_order(delayed, clamp_price(delayed_price - 0.01, 0.30, 0.65), tick.elapsed_sec)
    if tick.elapsed_sec >= CANCEL_AFTER_SEC:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")


def strategy_two_stage(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    if tick.elapsed_sec < 120:
        if tick.elapsed_sec == 0:
            place_ladder(state, tick, "UP", (0.04, 0.07))
            place_ladder(state, tick, "DOWN", (0.04, 0.07))
    elif tick.elapsed_sec < 240:
        for side in ("UP", "DOWN"):
            price = clamp_price(side_price(tick, side) - 0.01, 0.30, 0.65)
            projected = state.projected_avg_sum_with(side, price, state.share_size)
            if projected is None or (projected <= 0.98 and projected <= 1.00):
                if not state.open_orders(side):
                    state.place_order(side, price, tick.elapsed_sec)
    else:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")


def make_strategy_avg_sum(target_sum: float) -> Callable[[WindowState, WindowData, int], None]:
    def _strategy(state: WindowState, window: WindowData, tick_index: int) -> None:
        tick = window.ticks[tick_index]
        if tick.elapsed_sec >= CANCEL_AFTER_SEC:
            state.cancel_open_orders(tick.elapsed_sec, "cancel_240")
            return
        for side in ("UP", "DOWN"):
            price = clamp_price(side_price(tick, side) - 0.01, 0.30, 0.65)
            projected = state.projected_avg_sum_with(side, price, state.share_size)
            if projected is None:
                other = "DOWN" if side == "UP" else "UP"
                if state.avg_entry(other) is not None and state.avg_entry(other) + price <= target_sum and not state.open_orders(side):
                    state.place_order(side, price, tick.elapsed_sec)
            elif projected <= target_sum and not state.open_orders(side):
                state.place_order(side, price, tick.elapsed_sec)
    return _strategy


def strategy_panic_wick(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    if tick.elapsed_sec >= CANCEL_AFTER_SEC:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")
        return
    for side in ("UP", "DOWN"):
        if len(state.open_orders(side)) >= 2:
            continue
        current = side_price(tick, side)
        drop5 = None
        drop15 = None
        for lookback in (5, 15):
            for idx in range(tick_index - 1, -1, -1):
                if window.ticks[idx].elapsed_sec <= tick.elapsed_sec - lookback:
                    prev = side_price(window.ticks[idx], side)
                    drop = prev - current
                    if lookback == 5:
                        drop5 = drop
                    else:
                        drop15 = drop
                    break
        if drop15 is not None and drop15 >= 0.05:
            state.place_order(side, clamp_price(current - 0.01, 0.25, 0.60), tick.elapsed_sec)
        if drop15 is not None and drop15 >= 0.08:
            state.place_order(side, clamp_price(current - 0.03, 0.25, 0.60), tick.elapsed_sec)


def strategy_stop_after_good_basket(state: WindowState, window: WindowData, tick_index: int) -> None:
    tick = window.ticks[tick_index]
    if tick.elapsed_sec == 0:
        place_ladder(state, tick, "UP", (0.03, 0.06))
        place_ladder(state, tick, "DOWN", (0.03, 0.06))
    avg_sum = state.avg_sum()
    if avg_sum is not None:
        if avg_sum <= 0.97:
            state.trading_stopped = True
            state.cancel_open_orders(tick.elapsed_sec, "good_basket_stop")
            if avg_sum <= 0.95:
                state.notes.add("ideal_basket")
        elif avg_sum > 0.95:
            state.trading_stopped = True
            state.cancel_open_orders(tick.elapsed_sec, "basket_not_good_enough")
    if tick.elapsed_sec >= CANCEL_AFTER_SEC:
        state.cancel_open_orders(tick.elapsed_sec, "cancel_240")


STRATEGIES: tuple[tuple[str, str, Callable[[WindowState, WindowData, int], None]], ...] = (
    ("Deep Ladder Both Sides", "base", strategy_deep_ladder),
    ("Imbalance Allowed, Rebalance Only Cheap", "base", strategy_rebalance_only_cheap),
    ("Momentum-Discount Hedge", "base", strategy_momentum_discount),
    ("Two-Stage Window Plan", "base", strategy_two_stage),
    ("Avg-Sum Target Bot", "AvgSum_095", make_strategy_avg_sum(0.95)),
    ("Avg-Sum Target Bot", "AvgSum_097", make_strategy_avg_sum(0.97)),
    ("Avg-Sum Target Bot", "AvgSum_099", make_strategy_avg_sum(0.99)),
    ("Panic Wick Catcher", "base", strategy_panic_wick),
    ("Stop-After-Good-Basket", "base", strategy_stop_after_good_basket),
)


def simulate_window(window: WindowData, strategy_name: str, variant: str, share_size: int, budget: float, strategy_fn: Callable[[WindowState, WindowData, int], None]) -> WindowResult:
    state = WindowState(budget=budget, share_size=share_size)
    for tick_index, tick in enumerate(window.ticks):
        process_fills(state, window, tick_index)
        if tick.elapsed_sec >= WINDOW_END_SEC:
            break
        if tick.elapsed_sec >= CANCEL_AFTER_SEC:
            state.cancel_open_orders(tick.elapsed_sec, "cancel_240")
        if state.trading_stopped:
            continue
        strategy_fn(state, window, tick_index)
        state.update_imbalance()
    state.cancel_open_orders(CANCEL_AFTER_SEC, state.stopped_reason or "window_end")
    up_shares = state.filled_shares("UP")
    down_shares = state.filled_shares("DOWN")
    avg_up = state.avg_entry("UP")
    avg_down = state.avg_entry("DOWN")
    avg_sum = state.avg_sum()
    gross_cost = state.gross_cost()
    payout = float(up_shares if window.final_result == "UP" else down_shares)
    gross_pnl = payout - gross_cost
    net_pnl = gross_pnl - state.fee_cost()
    first_fill = min((fill.elapsed_sec for fill in state.fills), default=None)
    last_fill = max((fill.elapsed_sec for fill in state.fills), default=None)
    entry_fill = min(state.fills, key=lambda fill: fill.elapsed_sec) if state.fills else None
    hold_time = None if first_fill is None else WINDOW_END_SEC - first_fill
    late_fills = sum(1 for fill in state.fills if fill.elapsed_sec >= 220)
    return WindowResult(
        window_id=window.window_id,
        strategy_name=strategy_name,
        variant=variant,
        share_size=share_size,
        start_time=window.start_time,
        end_time=window.end_time,
        final_result=window.final_result,
        orders_placed=len(state.orders),
        orders_filled=len(state.fills),
        up_shares=up_shares,
        down_shares=down_shares,
        avg_up=avg_up,
        avg_down=avg_down,
        avg_sum=avg_sum,
        cost_gross=gross_cost,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        max_imbalance=state.max_imbalance,
        stopped_reason=state.stopped_reason or "",
        first_fill_time=first_fill,
        last_fill_time=last_fill,
        btc_move_10s_at_entry=None if entry_fill is None else entry_fill.btc_move_10s,
        btc_move_30s_at_entry=None if entry_fill is None else entry_fill.btc_move_30s,
        pm_volume_at_entry=None if entry_fill is None else entry_fill.pm_volume,
        hold_time_seconds=hold_time,
        late_fills_count=late_fills,
        balanced=up_shares > 0 and down_shares > 0,
        ideal_basket=avg_sum is not None and avg_sum <= 0.95,
        good_basket=avg_sum is not None and avg_sum <= 0.97,
    )


def profit_factor(rows: list[WindowResult], attr: str) -> float:
    gains = sum(getattr(row, attr) for row in rows if getattr(row, attr) > 0)
    losses = sum(-getattr(row, attr) for row in rows if getattr(row, attr) < 0)
    if losses <= 0:
        return gains if gains > 0 else 0.0
    return gains / losses


def max_drawdown(rows: list[WindowResult], attr: str) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for row in rows:
        equity += getattr(row, attr)
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def summarize_group(rows: list[WindowResult], notes: str) -> dict[str, object]:
    traded = [row for row in rows if row.orders_filled > 0]
    balanced = [row for row in rows if row.balanced]
    gross_total = sum(row.gross_pnl for row in rows)
    net_total = sum(row.net_pnl for row in rows)
    total_cost = sum(row.cost_gross for row in rows)
    up_entries = [row.avg_up for row in traded if row.avg_up is not None]
    down_entries = [row.avg_down for row in traded if row.avg_down is not None]
    pair_sums = [row.avg_sum for row in balanced if row.avg_sum is not None]
    avg_costs = [row.cost_gross for row in rows]
    traded_costs = [row.cost_gross for row in traded]
    hold_times = [row.hold_time_seconds for row in traded if row.hold_time_seconds is not None]
    entry_times = [row.first_fill_time for row in traded if row.first_fill_time is not None]
    win_windows = sum(1 for row in rows if row.net_pnl > 0)
    loss_windows = sum(1 for row in rows if row.net_pnl < 0)
    return {
        "strategy_name": rows[0].strategy_name,
        "variant": rows[0].variant,
        "share_size": rows[0].share_size,
        "windows_tested": len(rows),
        "windows_traded": len(traded),
        "skipped_windows": len(rows) - len(traded),
        "trade_rate_pct": (100.0 * len(traded) / len(rows)) if rows else 0.0,
        "total_orders_placed": sum(row.orders_placed for row in rows),
        "total_orders_filled": sum(row.orders_filled for row in rows),
        "fill_rate_pct": (100.0 * sum(row.orders_filled for row in rows) / sum(row.orders_placed for row in rows)) if sum(row.orders_placed for row in rows) else 0.0,
        "total_up_shares": sum(row.up_shares for row in rows),
        "total_down_shares": sum(row.down_shares for row in rows),
        "avg_up_shares_per_traded_window": mean([row.up_shares for row in traded]) if traded else 0.0,
        "avg_down_shares_per_traded_window": mean([row.down_shares for row in traded]) if traded else 0.0,
        "avg_imbalance_shares": mean([row.max_imbalance for row in rows]) if rows else 0.0,
        "max_imbalance_shares": max((row.max_imbalance for row in rows), default=0),
        "avg_cost_per_window": mean(avg_costs) if avg_costs else 0.0,
        "avg_cost_per_traded_window": mean(traded_costs) if traded_costs else 0.0,
        "avg_up_entry": mean(up_entries) if up_entries else 0.0,
        "avg_down_entry": mean(down_entries) if down_entries else 0.0,
        "avg_pair_sum_when_balanced": mean(pair_sums) if pair_sums else 0.0,
        "balanced_windows": len(balanced),
        "balanced_rate_pct": (100.0 * len(balanced) / len(rows)) if rows else 0.0,
        "ideal_basket_windows_avg_sum_le_095": sum(1 for row in rows if row.ideal_basket),
        "good_basket_windows_avg_sum_le_097": sum(1 for row in rows if row.good_basket),
        "gross_total_pnl": gross_total,
        "gross_roi_pct": (100.0 * gross_total / total_cost) if total_cost else 0.0,
        "gross_avg_pnl_per_window": gross_total / len(rows) if rows else 0.0,
        "gross_avg_pnl_per_traded_window": gross_total / len(traded) if traded else 0.0,
        "net_total_pnl_after_fee_buffer": net_total,
        "net_roi_pct": (100.0 * net_total / total_cost) if total_cost else 0.0,
        "net_avg_pnl_per_window": net_total / len(rows) if rows else 0.0,
        "net_avg_pnl_per_traded_window": net_total / len(traded) if traded else 0.0,
        "win_windows": win_windows,
        "loss_windows": loss_windows,
        "win_rate_pct": (100.0 * win_windows / len(rows)) if rows else 0.0,
        "profit_factor": profit_factor(rows, "net_pnl"),
        "max_drawdown": max_drawdown(rows, "net_pnl"),
        "best_window_pnl": max((row.net_pnl for row in rows), default=0.0),
        "worst_window_pnl": min((row.net_pnl for row in rows), default=0.0),
        "avg_orders_per_window": mean([row.orders_placed for row in rows]) if rows else 0.0,
        "avg_hold_time_seconds": mean(hold_times) if hold_times else 0.0,
        "avg_entry_time_seconds": mean(entry_times) if entry_times else 0.0,
        "late_fills_count": sum(row.late_fills_count for row in rows),
        "notes": notes,
    }


def write_csv(path: str, rows: list[dict[str, object]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx_if_available(summary_rows: list[dict[str, object]], detailed_rows: list[dict[str, object]], output_csv: str) -> str | None:
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        return None
    xlsx_path = str(Path(output_csv).with_suffix(".xlsx"))
    wb = Workbook()
    ws = wb.active
    ws.title = "summary"
    ws.append(list(summary_rows[0].keys()))
    for row in summary_rows:
        ws.append([row[key] for key in summary_rows[0].keys()])
    ws2 = wb.create_sheet("detailed")
    ws2.append(list(detailed_rows[0].keys()))
    for row in detailed_rows:
        ws2.append([row[key] for key in detailed_rows[0].keys()])
    wb.save(xlsx_path)
    return xlsx_path


def run_simulation(input_path: str, output_csv: str, budget: float, share_sizes: tuple[int, ...]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    windows = load_windows(input_path)
    detailed_results: list[WindowResult] = []
    for strategy_name, variant, strategy_fn in STRATEGIES:
        for share_size in share_sizes:
            for window in windows:
                detailed_results.append(
                    simulate_window(window, strategy_name, variant, share_size, budget, strategy_fn)
                )
    detailed_rows = [
        {
            "window_id": row.window_id,
            "strategy_name": row.strategy_name,
            "variant": row.variant,
            "share_size": row.share_size,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "final_result": row.final_result,
            "orders_placed": row.orders_placed,
            "orders_filled": row.orders_filled,
            "up_shares": row.up_shares,
            "down_shares": row.down_shares,
            "avg_up": row.avg_up,
            "avg_down": row.avg_down,
            "avg_sum": row.avg_sum,
            "cost": row.cost_gross,
            "gross_pnl": row.gross_pnl,
            "net_pnl": row.net_pnl,
            "max_imbalance": row.max_imbalance,
            "stopped_reason": row.stopped_reason,
            "first_fill_time": row.first_fill_time,
            "last_fill_time": row.last_fill_time,
            "btc_move_10s_at_entry": row.btc_move_10s_at_entry,
            "btc_move_30s_at_entry": row.btc_move_30s_at_entry,
            "pm_volume_at_entry": row.pm_volume_at_entry,
        }
        for row in detailed_results
    ]
    grouped: dict[tuple[str, str, int], list[WindowResult]] = {}
    for row in detailed_results:
        grouped.setdefault((row.strategy_name, row.variant, row.share_size), []).append(row)
    summary_rows = [
        summarize_group(rows, APPROXIMATE_NOTE)
        for _, rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1], item[0][2]))
    ]
    write_csv(output_csv, summary_rows)
    detailed_path = str(Path(output_csv).with_name(Path(output_csv).stem + "_detailed.csv"))
    write_csv(detailed_path, detailed_rows)
    write_xlsx_if_available(summary_rows, detailed_rows, output_csv)
    return summary_rows, detailed_rows


def print_rankings(summary_rows: list[dict[str, object]]) -> None:
    ranked = sorted(
        summary_rows,
        key=lambda row: (
            float(row["net_total_pnl_after_fee_buffer"]),
            float(row["net_avg_pnl_per_traded_window"]),
            float(row["max_drawdown"]),
            float(row["win_rate_pct"]),
        ),
        reverse=True,
    )
    print("strategy | variant | share_size | traded | net_pnl | avg_net | win_rate | max_dd | avg_pair_sum")
    for row in ranked[:12]:
        print(
            f"{row['strategy_name']} | {row['variant']} | {row['share_size']} | {row['windows_traded']} | "
            f"{float(row['net_total_pnl_after_fee_buffer']):.2f} | {float(row['net_avg_pnl_per_traded_window']):.4f} | "
            f"{float(row['win_rate_pct']):.2f}% | {float(row['max_drawdown']):.2f} | {float(row['avg_pair_sum_when_balanced']):.4f}"
        )


def parse_share_sizes(raw: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in raw.split(",") if part.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate BTC 5m hedge strategies on recorded datasets.")
    parser.add_argument("--input", required=True, help="Path to recorder dataset directory or CSV file.")
    parser.add_argument("--output", required=True, help="Summary CSV output path.")
    parser.add_argument("--budget", type=float, default=DEFAULT_BUDGET, help="Max budget per window.")
    parser.add_argument("--share-sizes", default="5,3,2,1", help="Comma-separated share sizes.")
    args = parser.parse_args()
    summary_rows, _ = run_simulation(args.input, args.output, args.budget, parse_share_sizes(args.share_sizes))
    print_rankings(summary_rows)


if __name__ == "__main__":
    main()
