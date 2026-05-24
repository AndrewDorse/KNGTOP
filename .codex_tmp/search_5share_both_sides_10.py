from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean

from kngtop.simulate_strategies import FEE_BUFFER_PER_TRADE, WindowData, load_windows

DATA = Path(r"C:\Users\Lenovo\Documents\Git\kng_bot3\exports\window_price_snapshots_public\btc_5m")
ORDER_SHARES = 5.0
MAX_SIDE_SHARES = 15.0
WINDOW_END = 300


@dataclass
class Order:
    side: str
    price: float
    placed: int
    filled: bool = False


@dataclass
class Fill:
    side: str
    price: float
    elapsed: int


@dataclass
class State:
    orders: list[Order] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)

    def shares(self, side: str) -> float:
        return ORDER_SHARES * sum(1 for f in self.fills if f.side == side)

    def avg(self, side: str) -> float | None:
        fills = [f for f in self.fills if f.side == side]
        return mean([f.price for f in fills]) if fills else None

    def avg_sum(self) -> float | None:
        up = self.avg("UP")
        down = self.avg("DOWN")
        if up is None or down is None:
            return None
        return up + down

    def last_fill(self, side: str) -> float | None:
        fills = [f for f in self.fills if f.side == side]
        return fills[-1].price if fills else None

    def open_orders(self, side: str | None = None) -> list[Order]:
        rows = [o for o in self.orders if not o.filled]
        if side is not None:
            rows = [o for o in rows if o.side == side]
        return rows

    def place(self, side: str, price: float, elapsed: int) -> bool:
        if self.shares(side) + ORDER_SHARES * (len(self.open_orders(side)) + 1) > MAX_SIDE_SHARES + 1e-12:
            return False
        if len(self.open_orders(side)) > 0:
            return False
        self.orders.append(Order(side=side, price=round(max(0.01, min(0.99, price)), 2), placed=int(elapsed)))
        return True

    def cancel_side(self, side: str) -> None:
        self.orders = [o for o in self.orders if o.filled or o.side != side]


def side_price(tick, side: str) -> float:
    return float(tick.up_price if side == "UP" else tick.down_price)


def process_fills(state: State, win: WindowData, idx: int) -> None:
    tick = win.ticks[idx]
    for order in state.open_orders():
        if side_price(tick, order.side) <= order.price + 1e-12:
            order.filled = True
            state.fills.append(Fill(order.side, order.price, int(tick.elapsed_sec)))


def pnl(state: State, win: WindowData) -> float:
    up = state.shares("UP")
    down = state.shares("DOWN")
    cost = sum(f.price * ORDER_SHARES for f in state.fills)
    fee = len(state.fills) * ORDER_SHARES * FEE_BUFFER_PER_TRADE
    payout = up if win.final_result == "UP" else down
    return payout - cost - fee


def balanced_attempt_ok(state: State) -> bool:
    return bool([o for o in state.orders if o.side == "UP"]) and bool([o for o in state.orders if o.side == "DOWN"])


def family_fixed_pair(win: WindowData, initial: float, next_price: float, max_pairs: int) -> State:
    st = State()
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < ORDER_SHARES * max_pairs:
            if not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", next_price, tick.elapsed_sec)
                st.place("DOWN", next_price, tick.elapsed_sec)
    return st


def family_ladder_pair(win: WindowData, initial: float, offsets: tuple[float, ...]) -> State:
    st = State()
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < MAX_SIDE_SHARES:
            level = int(st.shares("UP") // ORDER_SHARES)
            if level < len(offsets) and not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", side_price(tick, "UP") - offsets[level], tick.elapsed_sec)
                st.place("DOWN", side_price(tick, "DOWN") - offsets[level], tick.elapsed_sec)
    return st


def family_repair_only(win: WindowData, initial: float, hedge_cap: float, pair_cap: float) -> State:
    st = State()
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        up, down = st.shares("UP"), st.shares("DOWN")
        if up == down and up == ORDER_SHARES:
            avg_sum = st.avg_sum()
            if avg_sum is not None and avg_sum <= pair_cap:
                # Stop after one good basket.
                continue
        if up != down:
            smaller = "UP" if up < down else "DOWN"
            if not st.open_orders(smaller):
                st.place(smaller, min(side_price(tick, smaller), hedge_cap), tick.elapsed_sec)
    return st


def family_dynamic_pair(win: WindowData, initial: float, pair_cap: float, discount: float) -> State:
    st = State()
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < MAX_SIDE_SHARES:
            up_px = round(max(0.01, side_price(tick, "UP") - discount), 2)
            down_px = round(max(0.01, side_price(tick, "DOWN") - discount), 2)
            up_avg = st.avg("UP")
            down_avg = st.avg("DOWN")
            if up_avg is None or down_avg is None:
                continue
            new_sum = ((up_avg * st.shares("UP") + up_px * ORDER_SHARES) / (st.shares("UP") + ORDER_SHARES)) + (
                (down_avg * st.shares("DOWN") + down_px * ORDER_SHARES) / (st.shares("DOWN") + ORDER_SHARES)
            )
            if new_sum <= pair_cap and not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", up_px, tick.elapsed_sec)
                st.place("DOWN", down_px, tick.elapsed_sec)
    return st


def _replace_side_order(state: State, side: str, price: float, elapsed: int) -> bool:
    state.cancel_side(side)
    return state.place(side, price, elapsed)


def family_winner_hedge_ladder(
    win: WindowData,
    *,
    entry_threshold: float,
    hedge_buffer: float,
    dip_trigger: float,
    replace_hedge: bool,
    hedge_basis: str,
    replace_min_improvement: float,
) -> State:
    st = State()
    started = False
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue

        if not started:
            up_px = side_price(tick, "UP")
            down_px = side_price(tick, "DOWN")
            if max(up_px, down_px) < entry_threshold:
                continue
            first = "UP" if up_px >= down_px else "DOWN"
            second = "DOWN" if first == "UP" else "UP"
            first_px = side_price(tick, first)
            if st.place(first, first_px, tick.elapsed_sec):
                hedge_px = 1.0 - first_px - hedge_buffer
                st.place(second, hedge_px, tick.elapsed_sec)
                started = True
            continue

        up_shares = st.shares("UP")
        down_shares = st.shares("DOWN")
        if up_shares != down_shares:
            larger = "UP" if up_shares > down_shares else "DOWN"
            smaller = "DOWN" if larger == "UP" else "UP"
            if st.shares(smaller) < MAX_SIDE_SHARES:
                basis_price = st.last_fill(larger) if hedge_basis == "last_fill" else st.avg(larger)
                if basis_price is not None:
                    hedge_px = 1.0 - basis_price - hedge_buffer
                    existing = st.open_orders(smaller)
                    if replace_hedge and existing:
                        best_existing = max(o.price for o in existing)
                        if hedge_px >= best_existing + replace_min_improvement:
                            _replace_side_order(st, smaller, hedge_px, tick.elapsed_sec)
                    elif not existing:
                        st.place(smaller, hedge_px, tick.elapsed_sec)

        # If a held side gets cheap relative to its own average, add once more
        # and then hedge the opposite side from the updated average.
        candidates = []
        for side in ("UP", "DOWN"):
            avg = st.avg(side)
            if avg is None or st.shares(side) >= MAX_SIDE_SHARES:
                continue
            current = side_price(tick, side)
            if current <= avg - dip_trigger:
                candidates.append((avg - current, side, current))
        if not candidates:
            continue
        _edge, side, current = max(candidates)
        opposite = "DOWN" if side == "UP" else "UP"
        if st.open_orders(side):
            continue
        st.place(side, current, tick.elapsed_sec)
    return st


def eval_variant(name: str, params: dict, fn, windows: list[WindowData]) -> dict:
    rows = []
    for win in windows:
        st = fn(win)
        rows.append((st, pnl(st, win)))
    total = sum(v for _s, v in rows)
    both_attempt = sum(1 for st, _v in rows if balanced_attempt_ok(st))
    both_filled = sum(1 for st, _v in rows if st.shares("UP") > 0 and st.shares("DOWN") > 0)
    balanced_end = sum(1 for st, _v in rows if st.shares("UP") == st.shares("DOWN") and st.shares("UP") > 0)
    return {
        "family": name,
        "params": params,
        "windows": len(windows),
        "net": total,
        "avg": total / len(windows),
        "win_rate": 100 * sum(v > 0 for _s, v in rows) / len(windows),
        "both_attempt": both_attempt,
        "both_filled": both_filled,
        "balanced_end": balanced_end,
        "orders": sum(len(st.orders) for st, _v in rows),
        "fills": sum(len(st.fills) for st, _v in rows),
        "worst": min(v for _s, v in rows),
        "best": max(v for _s, v in rows),
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--windows", type=int, default=10)
    args = parser.parse_args()
    windows = load_windows(str(DATA))[: args.windows]
    variants = []
    for initial in (0.35, 0.39, 0.42, 0.45, 0.47):
        for next_price in (0.25, 0.30, 0.35, 0.39, 0.42):
            for max_pairs in (1, 2, 3):
                variants.append(eval_variant("fixed_pair", {"initial": initial, "next": next_price, "max_pairs": max_pairs}, lambda w, a=initial, b=next_price, c=max_pairs: family_fixed_pair(w, a, b, c), windows))
        for hedge_cap in (0.30, 0.35, 0.39, 0.42, 0.47):
            for pair_cap in (0.90, 0.92, 0.95):
                variants.append(eval_variant("repair_only", {"initial": initial, "hedge_cap": hedge_cap, "pair_cap": pair_cap}, lambda w, a=initial, b=hedge_cap, c=pair_cap: family_repair_only(w, a, b, c), windows))
        for pair_cap in (0.90, 0.92, 0.95):
            for discount in (0.01, 0.03, 0.05, 0.08, 0.12):
                variants.append(eval_variant("dynamic_pair", {"initial": initial, "pair_cap": pair_cap, "discount": discount}, lambda w, a=initial, b=pair_cap, c=discount: family_dynamic_pair(w, a, b, c), windows))
    for initial in (0.39, 0.42, 0.45, 0.47):
        for offsets in ((0.03, 0.06), (0.05, 0.10), (0.08, 0.14), (0.10, 0.18)):
            variants.append(eval_variant("ladder_pair", {"initial": initial, "offsets": offsets}, lambda w, a=initial, b=offsets: family_ladder_pair(w, a, b), windows))
    for entry_threshold in (0.51, 0.53, 0.55, 0.57):
        for hedge_buffer in (0.03, 0.05, 0.07, 0.10):
            for dip_trigger in (0.05, 0.08, 0.10, 0.12, 0.15):
                for replace_hedge in (False, True):
                    for hedge_basis in ("avg", "last_fill"):
                        for replace_min_improvement in (0.01, 0.03, 0.05):
                            variants.append(
                                eval_variant(
                                    "winner_hedge_ladder",
                                    {
                                        "entry_threshold": entry_threshold,
                                        "hedge_buffer": hedge_buffer,
                                        "dip_trigger": dip_trigger,
                                        "replace_hedge": replace_hedge,
                                        "hedge_basis": hedge_basis,
                                        "replace_min_improvement": replace_min_improvement,
                                    },
                                    lambda w, a=entry_threshold, b=hedge_buffer, c=dip_trigger, d=replace_hedge, e=hedge_basis, f=replace_min_improvement: family_winner_hedge_ladder(
                                        w,
                                        entry_threshold=a,
                                        hedge_buffer=b,
                                        dip_trigger=c,
                                        replace_hedge=d,
                                        hedge_basis=e,
                                        replace_min_improvement=f,
                                    ),
                                    windows,
                                )
                            )

    print("Best by family, constrained: 5-share orders, 15 max per side, both sides attempted every window")
    for fam in sorted(set(v["family"] for v in variants)):
        rows = [v for v in variants if v["family"] == fam and v["both_attempt"] == len(windows)]
        rows.sort(key=lambda r: (r["net"], r["balanced_end"], -r["fills"]), reverse=True)
        print("\\n" + fam)
        for r in rows[:8]:
            print(r)


if __name__ == "__main__":
    main()
