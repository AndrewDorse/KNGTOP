from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(r"C:\Users\Lenovo\Documents\Git\KNGTOP")
BASE = ROOT / ".codex_tmp" / "search_5share_both_sides_10.py"

sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("base_search", BASE)
base = importlib.util.module_from_spec(spec)
sys.modules["base_search"] = base
assert spec.loader is not None
spec.loader.exec_module(base)


def force_hedge_if_due(st, tick, imbalance_since: int | None, force_after: int | None, force_cap: float) -> int | None:
    up = st.shares("UP")
    down = st.shares("DOWN")
    if up == down:
        return None
    if imbalance_since is None:
        imbalance_since = int(tick.elapsed_sec)
    if force_after is None:
        return imbalance_since
    if int(tick.elapsed_sec) - imbalance_since < force_after:
        return imbalance_since
    smaller = "UP" if up < down else "DOWN"
    if st.shares(smaller) >= base.MAX_SIDE_SHARES:
        return imbalance_since
    price = min(base.side_price(tick, smaller), force_cap)
    st.cancel_side(smaller)
    st.place(smaller, price, tick.elapsed_sec)
    return int(tick.elapsed_sec)


def sim_fixed_pair(win, *, initial, next_price, max_pairs, force_after, force_cap):
    st = base.State()
    imbalance_since = None
    for i, tick in enumerate(win.ticks):
        base.process_fills(st, win, i)
        imbalance_since = force_hedge_if_due(st, tick, imbalance_since, force_after, force_cap)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < base.ORDER_SHARES * max_pairs:
            if not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", next_price, tick.elapsed_sec)
                st.place("DOWN", next_price, tick.elapsed_sec)
    return st


def sim_ladder_pair(win, *, initial, offsets, force_after, force_cap):
    st = base.State()
    imbalance_since = None
    for i, tick in enumerate(win.ticks):
        base.process_fills(st, win, i)
        imbalance_since = force_hedge_if_due(st, tick, imbalance_since, force_after, force_cap)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < base.MAX_SIDE_SHARES:
            level = int(st.shares("UP") // base.ORDER_SHARES)
            if level < len(offsets) and not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", base.side_price(tick, "UP") - offsets[level], tick.elapsed_sec)
                st.place("DOWN", base.side_price(tick, "DOWN") - offsets[level], tick.elapsed_sec)
    return st


def sim_dynamic_pair(win, *, initial, pair_cap, discount, force_after, force_cap):
    st = base.State()
    imbalance_since = None
    for i, tick in enumerate(win.ticks):
        base.process_fills(st, win, i)
        imbalance_since = force_hedge_if_due(st, tick, imbalance_since, force_after, force_cap)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        if st.shares("UP") == st.shares("DOWN") and st.shares("UP") < base.MAX_SIDE_SHARES:
            up_px = round(max(0.01, base.side_price(tick, "UP") - discount), 2)
            down_px = round(max(0.01, base.side_price(tick, "DOWN") - discount), 2)
            up_avg = st.avg("UP")
            down_avg = st.avg("DOWN")
            if up_avg is None or down_avg is None:
                continue
            new_sum = ((up_avg * st.shares("UP") + up_px * base.ORDER_SHARES) / (st.shares("UP") + base.ORDER_SHARES)) + (
                (down_avg * st.shares("DOWN") + down_px * base.ORDER_SHARES) / (st.shares("DOWN") + base.ORDER_SHARES)
            )
            if new_sum <= pair_cap and not st.open_orders("UP") and not st.open_orders("DOWN"):
                st.place("UP", up_px, tick.elapsed_sec)
                st.place("DOWN", down_px, tick.elapsed_sec)
    return st


def sim_repair_only(win, *, initial, hedge_cap, pair_cap, force_after, force_cap):
    st = base.State()
    imbalance_since = None
    del pair_cap
    for i, tick in enumerate(win.ticks):
        base.process_fills(st, win, i)
        imbalance_since = force_hedge_if_due(st, tick, imbalance_since, force_after, force_cap)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue
        if tick.elapsed_sec <= 2 and not st.orders:
            st.place("UP", initial, tick.elapsed_sec)
            st.place("DOWN", initial, tick.elapsed_sec)
        up, down = st.shares("UP"), st.shares("DOWN")
        if up != down:
            smaller = "UP" if up < down else "DOWN"
            if not st.open_orders(smaller):
                st.place(smaller, min(base.side_price(tick, smaller), hedge_cap), tick.elapsed_sec)
    return st


def sim_winner_hedge_ladder(win, *, entry_threshold, hedge_buffer, dip_trigger, replace_hedge, hedge_basis, force_after, force_cap):
    st = base.State()
    started = False
    imbalance_since = None
    for i, tick in enumerate(win.ticks):
        base.process_fills(st, win, i)
        imbalance_since = force_hedge_if_due(st, tick, imbalance_since, force_after, force_cap)
        if tick.elapsed_sec < -20 or tick.elapsed_sec >= 240:
            continue

        if not started:
            up_px = base.side_price(tick, "UP")
            down_px = base.side_price(tick, "DOWN")
            if max(up_px, down_px) < entry_threshold:
                continue
            first = "UP" if up_px >= down_px else "DOWN"
            second = "DOWN" if first == "UP" else "UP"
            first_px = base.side_price(tick, first)
            if st.place(first, first_px, tick.elapsed_sec):
                st.place(second, 1.0 - first_px - hedge_buffer, tick.elapsed_sec)
                started = True
            continue

        up = st.shares("UP")
        down = st.shares("DOWN")
        if up != down:
            larger = "UP" if up > down else "DOWN"
            smaller = "DOWN" if larger == "UP" else "UP"
            if not st.open_orders(smaller):
                basis = st.last_fill(larger) if hedge_basis == "last_fill" else st.avg(larger)
                if basis is not None:
                    st.place(smaller, 1.0 - basis - hedge_buffer, tick.elapsed_sec)
            continue

        candidates = []
        for side in ("UP", "DOWN"):
            avg = st.avg(side)
            if avg is None or st.shares(side) >= base.MAX_SIDE_SHARES or st.open_orders(side):
                continue
            current = base.side_price(tick, side)
            if current <= avg - dip_trigger:
                candidates.append((avg - current, side, current))
        if candidates:
            _edge, side, current = max(candidates)
            st.place(side, current, tick.elapsed_sec)
            if replace_hedge:
                other = "DOWN" if side == "UP" else "UP"
                basis = st.last_fill(side) if hedge_basis == "last_fill" else st.avg(side)
                if basis is not None and st.shares(other) < st.shares(side):
                    st.cancel_side(other)
                    st.place(other, 1.0 - basis - hedge_buffer, tick.elapsed_sec)
    return st


def run_search(windows_n: int = 100) -> list[dict]:
    windows = base.load_windows(str(base.DATA))[:windows_n]
    rows = []
    force_afters = (None, 15, 30, 45, 60)
    force_caps = (0.70, 0.80, 0.99)

    for force_after in force_afters:
        for force_cap in force_caps:
            for initial in (0.35, 0.39, 0.42, 0.45, 0.47):
                for next_price in (0.25, 0.30, 0.35):
                    rows.append(base.eval_variant("fixed_pair_force", {"initial": initial, "next": next_price, "max_pairs": 3, "force_after": force_after, "force_cap": force_cap}, lambda w, a=initial, b=next_price, fa=force_after, fc=force_cap: sim_fixed_pair(w, initial=a, next_price=b, max_pairs=3, force_after=fa, force_cap=fc), windows))
                for hedge_cap in (0.25, 0.30, 0.35, 0.39):
                    rows.append(base.eval_variant("repair_only_force", {"initial": initial, "hedge_cap": hedge_cap, "pair_cap": 0.95, "force_after": force_after, "force_cap": force_cap}, lambda w, a=initial, h=hedge_cap, fa=force_after, fc=force_cap: sim_repair_only(w, initial=a, hedge_cap=h, pair_cap=0.95, force_after=fa, force_cap=fc), windows))
                for discount in (0.08, 0.12, 0.15):
                    rows.append(base.eval_variant("dynamic_pair_force", {"initial": initial, "pair_cap": 0.95, "discount": discount, "force_after": force_after, "force_cap": force_cap}, lambda w, a=initial, d=discount, fa=force_after, fc=force_cap: sim_dynamic_pair(w, initial=a, pair_cap=0.95, discount=d, force_after=fa, force_cap=fc), windows))

            for initial in (0.39, 0.42, 0.45, 0.47):
                for offsets in ((0.10, 0.18), (0.15, 0.25), (0.20, 0.30)):
                    rows.append(base.eval_variant("ladder_pair_force", {"initial": initial, "offsets": offsets, "force_after": force_after, "force_cap": force_cap}, lambda w, a=initial, o=offsets, fa=force_after, fc=force_cap: sim_ladder_pair(w, initial=a, offsets=o, force_after=fa, force_cap=fc), windows))

            for entry_threshold in (0.53, 0.55, 0.57):
                for hedge_buffer in (0.05, 0.07, 0.10):
                    for dip_trigger in (0.10, 0.12, 0.15):
                        rows.append(base.eval_variant("winner_hedge_ladder_force", {"entry_threshold": entry_threshold, "hedge_buffer": hedge_buffer, "dip_trigger": dip_trigger, "replace_hedge": False, "hedge_basis": "avg", "force_after": force_after, "force_cap": force_cap}, lambda w, a=entry_threshold, b=hedge_buffer, c=dip_trigger, fa=force_after, fc=force_cap: sim_winner_hedge_ladder(w, entry_threshold=a, hedge_buffer=b, dip_trigger=c, replace_hedge=False, hedge_basis="avg", force_after=fa, force_cap=fc), windows))
    return rows


if __name__ == "__main__":
    rows = run_search(100)
    print("Best by family with force hedge timing")
    for family in sorted({r["family"] for r in rows}):
        fam_rows = [r for r in rows if r["family"] == family and r["both_attempt"] == 100]
        fam_rows.sort(key=lambda r: (r["net"], r["balanced_end"], -r["fills"]), reverse=True)
        print("\n" + family)
        for row in fam_rows[:10]:
            print(row)
    rows.sort(key=lambda r: (r["net"], r["balanced_end"], -r["fills"]), reverse=True)
    print("\nOverall")
    for row in rows[:20]:
        print(row)
