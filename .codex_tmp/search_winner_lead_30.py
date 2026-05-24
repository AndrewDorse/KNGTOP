from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean

from kngtop.simulate_strategies import FEE_BUFFER_PER_TRADE, WindowData, load_windows

DATA = Path(r"C:\Users\Lenovo\Documents\Git\kng_bot3\exports\window_price_snapshots_public\btc_5m")
ORDER_SHARES = 5.0
MAX_SIDE_SHARES = 30.0


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

    def open_orders(self, side: str | None = None) -> list[Order]:
        rows = [o for o in self.orders if not o.filled]
        if side is not None:
            rows = [o for o in rows if o.side == side]
        return rows

    def place(self, side: str, price: float, elapsed: int, *, allow_multiple: bool = False) -> bool:
        if self.shares(side) + ORDER_SHARES * (len(self.open_orders(side)) + 1) > MAX_SIDE_SHARES + 1e-12:
            return False
        if not allow_multiple and self.open_orders(side):
            return False
        self.orders.append(Order(side=side, price=round(max(0.01, min(0.99, price)), 2), placed=int(elapsed)))
        return True

    def cancel_side(self, side: str) -> None:
        self.orders = [o for o in self.orders if o.filled or o.side != side]


def side_price(tick, side: str) -> float:
    return float(tick.up_price if side == "UP" else tick.down_price)


def other(side: str) -> str:
    return "DOWN" if side == "UP" else "UP"


def process_fills(st: State, win: WindowData, idx: int) -> None:
    tick = win.ticks[idx]
    for order in st.open_orders():
        if side_price(tick, order.side) <= order.price + 1e-12:
            order.filled = True
            st.fills.append(Fill(order.side, order.price, int(tick.elapsed_sec)))


def pnl(st: State, win: WindowData) -> float:
    up = st.shares("UP")
    down = st.shares("DOWN")
    cost = sum(f.price * ORDER_SHARES for f in st.fills)
    fee = len(st.fills) * ORDER_SHARES * FEE_BUFFER_PER_TRADE
    payout = up if win.final_result == "UP" else down
    return payout - cost - fee


def winner_side(tick) -> str:
    return "UP" if side_price(tick, "UP") >= side_price(tick, "DOWN") else "DOWN"


def both_attempted(st: State) -> bool:
    return bool([o for o in st.orders if o.side == "UP"]) and bool([o for o in st.orders if o.side == "DOWN"])


def place_hedge(st: State, side: str, tick, mode: str, cap: float, buffer: float) -> None:
    avg_leader = st.avg(other(side))
    current = side_price(tick, side)
    if mode == "current":
        price = current
    elif mode == "cap":
        price = min(current, cap)
    elif mode == "formula":
        price = min(current, (1.0 - (avg_leader or side_price(tick, other(side))) - buffer))
    else:
        price = cap
    if st.open_orders(side):
        return
    st.place(side, price, tick.elapsed_sec)


def sim_lead_with_hedge(
    win: WindowData,
    *,
    threshold: float,
    hedge_mode: str,
    hedge_cap: float,
    hedge_buffer: float,
    lead_gap: float,
    max_pairs: int,
    force_after: int | None,
) -> State:
    st = State()
    imbalance_since: int | None = None
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue

        lead = winner_side(tick)
        trail = other(lead)
        lead_px = side_price(tick, lead)
        up = st.shares("UP")
        down = st.shares("DOWN")

        if not st.orders and lead_px >= threshold:
            st.place(lead, lead_px, tick.elapsed_sec)
            place_hedge(st, trail, tick, hedge_mode, hedge_cap, hedge_buffer)
            continue

        if up != down:
            imbalance_since = int(tick.elapsed_sec) if imbalance_since is None else imbalance_since
        else:
            imbalance_since = None

        # Emergency catch-up for the trailing side, but only up to the desired
        # winner + lead_gap shape.
        if force_after is not None and imbalance_since is not None and int(tick.elapsed_sec) - imbalance_since >= force_after:
            smaller = "UP" if up < down else "DOWN"
            larger = other(smaller)
            if st.shares(larger) - st.shares(smaller) > lead_gap:
                st.cancel_side(smaller)
                st.place(smaller, min(side_price(tick, smaller), hedge_cap), tick.elapsed_sec)
                imbalance_since = int(tick.elapsed_sec)

        # Keep the current winner one 5-share block larger when strong enough.
        if lead_px >= threshold and st.shares(lead) < MAX_SIDE_SHARES:
            if st.shares(lead) < st.shares(trail) + lead_gap and not st.open_orders(lead):
                st.place(lead, lead_px, tick.elapsed_sec)

        # Keep hedges working, but do not force full balance. Target is winner
        # side ahead by lead_gap.
        up = st.shares("UP")
        down = st.shares("DOWN")
        if max(up, down) >= ORDER_SHARES and min(up, down) < max(up, down) - lead_gap:
            smaller = "UP" if up < down else "DOWN"
            place_hedge(st, smaller, tick, hedge_mode, hedge_cap, hedge_buffer)

        # Add cheap paired depth only when the leader is already ahead by lead_gap.
        up = st.shares("UP")
        down = st.shares("DOWN")
        if max(up, down) // ORDER_SHARES < max_pairs and abs(up - down) == lead_gap:
            if not st.open_orders("UP") and not st.open_orders("DOWN"):
                offset = 0.20 if max(up, down) < 15 else 0.30
                st.place("UP", side_price(tick, "UP") - offset, tick.elapsed_sec)
                st.place("DOWN", side_price(tick, "DOWN") - offset, tick.elapsed_sec)
    return st


def sim_rotating_momentum(
    win: WindowData,
    *,
    threshold: float,
    add_discount: float,
    hedge_discount: float,
    lead_gap: float,
    force_after: int | None,
) -> State:
    st = State()
    imbalance_since: int | None = None
    for i, tick in enumerate(win.ticks):
        process_fills(st, win, i)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        up = st.shares("UP")
        down = st.shares("DOWN")
        imbalance_since = int(tick.elapsed_sec) if up != down and imbalance_since is None else (None if up == down else imbalance_since)

        lead = winner_side(tick)
        trail = other(lead)
        if not st.orders and side_price(tick, lead) >= threshold:
            st.place(lead, side_price(tick, lead) - add_discount, tick.elapsed_sec)
            st.place(trail, side_price(tick, trail) - hedge_discount, tick.elapsed_sec)
            continue
        if side_price(tick, lead) >= threshold and st.shares(lead) <= st.shares(trail) and not st.open_orders(lead):
            st.place(lead, side_price(tick, lead) - add_discount, tick.elapsed_sec)

        up = st.shares("UP")
        down = st.shares("DOWN")
        if max(up, down) - min(up, down) > lead_gap:
            smaller = "UP" if up < down else "DOWN"
            if not st.open_orders(smaller):
                st.place(smaller, side_price(tick, smaller) - hedge_discount, tick.elapsed_sec)

        if force_after is not None and imbalance_since is not None and int(tick.elapsed_sec) - imbalance_since >= force_after:
            smaller = "UP" if st.shares("UP") < st.shares("DOWN") else "DOWN"
            st.cancel_side(smaller)
            st.place(smaller, min(side_price(tick, smaller), 0.70), tick.elapsed_sec)
            imbalance_since = int(tick.elapsed_sec)
    return st


def eval_variant(name: str, params: dict, fn, windows: list[WindowData]) -> dict:
    rows = []
    for win in windows:
        st = fn(win)
        rows.append((st, pnl(st, win)))
    total = sum(v for _s, v in rows)
    return {
        "family": name,
        "params": params,
        "windows": len(windows),
        "net": total,
        "avg": total / len(windows),
        "win_rate": 100 * sum(v > 0 for _s, v in rows) / len(windows),
        "both_attempt": sum(1 for st, _v in rows if both_attempted(st)),
        "both_filled": sum(1 for st, _v in rows if st.shares("UP") > 0 and st.shares("DOWN") > 0),
        "leader_plus_5_end": sum(1 for st, _v in rows if abs(st.shares("UP") - st.shares("DOWN")) == ORDER_SHARES),
        "balanced_end": sum(1 for st, _v in rows if st.shares("UP") == st.shares("DOWN") and st.shares("UP") > 0),
        "orders": sum(len(st.orders) for st, _v in rows),
        "fills": sum(len(st.fills) for st, _v in rows),
        "worst": min(v for _s, v in rows),
        "best": max(v for _s, v in rows),
        "avg_max_side": mean(max(st.shares("UP"), st.shares("DOWN")) for st, _v in rows),
    }


def main() -> None:
    windows = load_windows(str(DATA))[:100]
    rows = []
    for threshold in (0.60, 0.70, 0.80):
        for hedge_mode in ("cap", "formula", "current"):
            for hedge_cap in (0.45, 0.55, 0.70):
                for hedge_buffer in (0.03, 0.05, 0.10):
                    for force_after in (None, 45, 60):
                        params = {
                            "threshold": threshold,
                            "hedge_mode": hedge_mode,
                            "hedge_cap": hedge_cap,
                            "hedge_buffer": hedge_buffer,
                            "lead_gap": 5.0,
                            "max_pairs": 6,
                            "force_after": force_after,
                        }
                        rows.append(eval_variant("lead_with_hedge", params, lambda w, p=params: sim_lead_with_hedge(w, **p), windows))
        for add_discount in (0.00, 0.05, 0.10):
            for hedge_discount in (0.05, 0.10, 0.20):
                for force_after in (None, 45, 60):
                    params = {
                        "threshold": threshold,
                        "add_discount": add_discount,
                        "hedge_discount": hedge_discount,
                        "lead_gap": 5.0,
                        "force_after": force_after,
                    }
                    rows.append(eval_variant("rotating_momentum", params, lambda w, p=params: sim_rotating_momentum(w, **p), windows))

    print("Best by family, winner side allowed +5, max side 30")
    for fam in sorted({r["family"] for r in rows}):
        fam_rows = [r for r in rows if r["family"] == fam and r["both_attempt"] == 100]
        fam_rows.sort(key=lambda r: (r["net"], r["leader_plus_5_end"], -r["fills"]), reverse=True)
        print("\n" + fam)
        for row in fam_rows[:20]:
            print(row)
    rows = [r for r in rows if r["both_attempt"] == 100]
    rows.sort(key=lambda r: (r["net"], r["leader_plus_5_end"], -r["fills"]), reverse=True)
    print("\nOverall")
    for row in rows[:30]:
        print(row)


if __name__ == "__main__":
    main()
