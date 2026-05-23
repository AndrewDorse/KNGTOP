"""Strict order cycle: send primary → confirm on CLOB → send hedge once → 5× PM confirm → idle."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.pm_data import fetch_user_positions

if TYPE_CHECKING:
    from kngtop.live_kilemo2 import PositionState, WindowRunner

LOGGER = logging.getLogger("kngtop")

RECONCILE_INTERVAL_SEC = 1.0
PM_STABLE_CONFIRMS = 5
PM_STABLE_INTERVAL_SEC = 1.0

PHASE_IDLE = "idle"
PHASE_WAIT_PRIMARY = "wait_primary"
PHASE_WAIT_HEDGE = "wait_hedge"
PHASE_WAIT_PM = "wait_pm"

LEG_PRIMARY = "primary"
LEG_HEDGE = "hedge"


@dataclass(slots=True)
class OpenOrderView:
    order_id: str
    side: str
    price: float
    remaining_shares: float


@dataclass(slots=True)
class LiveOrder:
    """Compat view of the in-memory cycle leg."""

    client_id: str
    side: str
    token_id: str
    price: float
    shares: float
    reason: str
    order_id: str | None = None
    status: str = "open"
    sent_ts: float = 0.0
    pre_shares: float = 0.0
    remaining_shares: float = 0.0
    matched_shares: float = 0.0

    def is_active(self) -> bool:
        return True

    def reserved_shares(self) -> float:
        return self.remaining_shares if self.remaining_shares > 1e-12 else self.shares


@dataclass(slots=True)
class OrderCycle:
    phase: str = PHASE_IDLE
    cycle_n: int = 0
    primary_side: str = ""
    primary_price: float = 0.0
    primary_shares: float = 0.0
    primary_order_id: str | None = None
    primary_reason: str = ""
    primary_sent: bool = False
    hedge_side: str = ""
    hedge_price: float = 0.0
    hedge_shares: float = 0.0
    hedge_order_id: str | None = None
    hedge_sent: bool = False
    hedge_needed: bool = False
    pm_up: float = -1.0
    pm_down: float = -1.0
    pm_up_start: float = -1.0
    pm_down_start: float = -1.0
    pm_stable_streak: int = 0
    pm_checks: int = 0
    last_pm_check_ts: float = 0.0
    sends_this_cycle: int = 0


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


def log_cycle(runner: WindowRunner) -> None:
    c = runner.cycle
    if cycle_is_idle(runner):
        _log_tag("CYCLE", slug=runner.contract.slug, phase="IDLE", cycles=str(c.cycle_n))
        return
    _log_tag(
        "CYCLE",
        slug=runner.contract.slug,
        phase=c.phase,
        cycle_n=str(c.cycle_n),
        sends=str(c.sends_this_cycle),
        primary_id=c.primary_order_id,
        hedge_id=c.hedge_order_id,
        pm_streak=str(c.pm_stable_streak),
    )


def cycle_is_idle(runner: WindowRunner) -> bool:
    return runner.cycle.phase == PHASE_IDLE


def cycle_is_busy(runner: WindowRunner) -> bool:
    return not cycle_is_idle(runner)


def has_active_order(runner: WindowRunner) -> bool:
    return cycle_is_busy(runner)


def active_order(runner: WindowRunner) -> LiveOrder | None:
    c = runner.cycle
    if cycle_is_idle(runner):
        return None
    if c.phase in {PHASE_WAIT_HEDGE, PHASE_WAIT_PM} and c.hedge_sent:
        side, price, shares = c.hedge_side, c.hedge_price, c.hedge_shares
        oid, reason = c.hedge_order_id, "hedge_limit"
    else:
        side, price, shares = c.primary_side, c.primary_price, c.primary_shares
        oid, reason = c.primary_order_id, c.primary_reason
    token_id = runner.contract.up.token_id if side == "UP" else runner.contract.down.token_id
    return LiveOrder(
        client_id=f"cycle-{c.cycle_n}-{side}",
        side=side,
        token_id=token_id,
        price=price,
        shares=shares,
        reason=reason,
        order_id=oid,
        status="open",
        remaining_shares=shares,
    )


def runner_open_order_views(runner: WindowRunner) -> list[OpenOrderView]:
    return list(runner.open_orders.get("UP", [])) + list(runner.open_orders.get("DOWN", []))


def cycle_reset(runner: WindowRunner) -> None:
    runner.cycle = OrderCycle(cycle_n=runner.cycle.cycle_n)


def clear_window_orders(runner: WindowRunner) -> None:
    runner.cycle = OrderCycle()
    runner.open_orders = {"UP": [], "DOWN": []}
    runner.orders_sent = 0
    runner.orders.clear()


def cycle_begin_primary(
    runner: WindowRunner,
    *,
    side: str,
    price: float,
    shares: float,
    reason: str,
    pm_up: float = -1.0,
    pm_down: float = -1.0,
) -> bool:
    c = runner.cycle
    if not cycle_is_idle(runner) or c.primary_sent:
        return False
    c.cycle_n += 1
    c.phase = PHASE_WAIT_PRIMARY
    c.primary_side = str(side).upper()
    c.primary_price = float(price)
    c.primary_shares = float(shares)
    c.primary_reason = str(reason)
    c.primary_sent = True
    c.sends_this_cycle = 1
    c.hedge_sent = False
    c.hedge_needed = False
    c.pm_stable_streak = 0
    c.pm_checks = 0
    c.pm_up_start = float(pm_up)
    c.pm_down_start = float(pm_down)
    runner.orders_sent = int(getattr(runner, "orders_sent", 0)) + 1
    _log_tag(
        "CYCLE PRIMARY SENT",
        slug=runner.contract.slug,
        cycle_n=str(c.cycle_n),
        side=c.primary_side,
        price=f"{c.primary_price:.4f}",
        shares=f"{c.primary_shares:.6f}",
        send_n=str(runner.orders_sent),
    )
    return True


def cycle_mark_primary_id(runner: WindowRunner, order_id: str | None) -> None:
    if order_id:
        runner.cycle.primary_order_id = str(order_id)


def cycle_begin_hedge(runner: WindowRunner, *, side: str, price: float, shares: float) -> bool:
    c = runner.cycle
    if c.hedge_sent or c.sends_this_cycle >= 2:
        return False
    c.hedge_side = str(side).upper()
    c.hedge_price = float(price)
    c.hedge_shares = float(shares)
    c.hedge_sent = True
    c.hedge_needed = True
    c.sends_this_cycle = 2
    c.phase = PHASE_WAIT_HEDGE
    runner.orders_sent = int(getattr(runner, "orders_sent", 0)) + 1
    _log_tag(
        "CYCLE HEDGE SENT",
        slug=runner.contract.slug,
        cycle_n=str(c.cycle_n),
        side=c.hedge_side,
        price=f"{c.hedge_price:.4f}",
        shares=f"{c.hedge_shares:.6f}",
        send_n=str(runner.orders_sent),
    )
    return True


def cycle_mark_hedge_id(runner: WindowRunner, order_id: str | None) -> None:
    if order_id:
        runner.cycle.hedge_order_id = str(order_id)


def cycle_start_pm_wait(runner: WindowRunner, *, up_shares: float, down_shares: float, now_ts: float | None = None) -> None:
    c = runner.cycle
    c.phase = PHASE_WAIT_PM
    c.pm_up = float(up_shares)
    c.pm_down = float(down_shares)
    c.pm_stable_streak = 1
    c.pm_checks = 1
    c.last_pm_check_ts = 0.0 if now_ts is None else float(now_ts)
    _log_tag(
        "CYCLE PM WAIT",
        slug=runner.contract.slug,
        cycle_n=str(c.cycle_n),
        up_shares=f"{c.pm_up:.6f}",
        down_shares=f"{c.pm_down:.6f}",
        need=str(PM_STABLE_CONFIRMS),
    )


def _extract_order_id(payload: dict[str, object]) -> str | None:
    for key in ("orderID", "orderId", "id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _filtered_open_orders_for_runner(runner: WindowRunner, open_order_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    token_ids = {runner.contract.up.token_id, runner.contract.down.token_id}
    out: list[dict[str, Any]] = []
    for row in open_order_rows:
        asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
        if asset_id and asset_id in token_ids:
            out.append(row)
    return out


def _parse_open_orders_for_runner(runner: WindowRunner, open_order_rows: list[dict[str, Any]]) -> dict[str, list[OpenOrderView]]:
    parsed: dict[str, list[OpenOrderView]] = {"UP": [], "DOWN": []}
    for side, token in (("UP", runner.contract.up), ("DOWN", runner.contract.down)):
        for row in open_order_rows:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
            if asset_id and asset_id != token.token_id:
                continue
            raw_side = str(row.get("side") or "").strip().upper()
            if raw_side and raw_side != "BUY":
                continue
            oid = _extract_order_id(row)
            try:
                price = float(row.get("price") or 0.0)
            except (TypeError, ValueError):
                continue
            if not oid or price <= 0.0:
                continue
            remaining = row.get("size_left") or row.get("remaining") or row.get("original_size") or row.get("size")
            try:
                rem = max(0.0, float(remaining or 0.0))
            except (TypeError, ValueError):
                rem = 0.0
            if rem <= 1e-12:
                continue
            parsed[side].append(OpenOrderView(order_id=oid, side=side, price=price, remaining_shares=rem))
    return parsed


def sync_clob_open_orders(runner: WindowRunner, *, clob: KngtopClob | None, open_rows: list[dict[str, Any]] | None = None) -> dict[str, list[OpenOrderView]]:
    if clob is None and open_rows is None:
        return runner.open_orders
    rows = open_rows if open_rows is not None else (clob.get_open_orders() if clob is not None else [])
    scoped = _filtered_open_orders_for_runner(runner, list(rows))
    parsed = _parse_open_orders_for_runner(runner, scoped)
    runner.open_orders = parsed
    return parsed


def order_on_clob(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    order_id: str | None,
    side: str,
) -> bool:
    del side
    sync_clob_open_orders(runner, clob=clob)
    if not order_id:
        return False
    for view in runner_open_order_views(runner):
        if view.order_id == order_id:
            return True
    if clob is not None:
        try:
            payload = clob.get_order(order_id)
        except Exception:  # noqa: BLE001
            payload = {}
        if isinstance(payload, dict) and payload:
            remaining = payload.get("size_left") or payload.get("remaining") or payload.get("original_size")
            try:
                if float(remaining or 0.0) > 1e-12:
                    return True
            except (TypeError, ValueError):
                pass
    return False


def tick_pm_stable(runner: WindowRunner, *, up_shares: float, down_shares: float, now_ts: float) -> bool:
    """One PM check. Returns True when 5 stable confirms reached."""
    c = runner.cycle
    if c.last_pm_check_ts > 1e-12 and now_ts - c.last_pm_check_ts + 1e-12 < PM_STABLE_INTERVAL_SEC:
        return False
    c.last_pm_check_ts = now_ts
    c.pm_checks += 1
    if abs(up_shares - c.pm_up) <= 1e-4 and abs(down_shares - c.pm_down) <= 1e-4:
        c.pm_stable_streak += 1
    else:
        c.pm_up = float(up_shares)
        c.pm_down = float(down_shares)
        c.pm_stable_streak = 1
    _log_tag(
        "CYCLE PM CHECK",
        slug=runner.contract.slug,
        cycle_n=str(c.cycle_n),
        streak=str(c.pm_stable_streak),
        need=str(PM_STABLE_CONFIRMS),
        up=f"{up_shares:.6f}",
        down=f"{down_shares:.6f}",
    )
    return c.pm_stable_streak >= PM_STABLE_CONFIRMS


def cancel_open_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    view: OpenOrderView,
    reason: str,
) -> bool:
    if clob is None:
        return False
    keep_ids = {runner.cycle.primary_order_id, runner.cycle.hedge_order_id} - {None}
    if view.order_id in keep_ids:
        return False
    try:
        clob.cancel_order_by_id(view.order_id)
    except Exception as exc:  # noqa: BLE001
        _log_tag("LIMIT CANCEL", slug=runner.contract.slug, order_id=view.order_id, error=str(exc))
        return False
    _log_tag("LIMIT CANCEL", slug=runner.contract.slug, order_id=view.order_id, reason=reason)
    return True


def enforce_single_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    required_side: str | None,
) -> bool:
    if clob is None:
        return cycle_is_busy(runner)
    sync_clob_open_orders(runner, clob=clob)
    keep_ids = {runner.cycle.primary_order_id, runner.cycle.hedge_order_id} - {None}
    views = [v for v in runner_open_order_views(runner) if v.order_id not in keep_ids]
    if not views:
        return cycle_is_busy(runner)
    keep_one: OpenOrderView | None = None
    for view in views:
        if required_side is not None and view.side != required_side:
            cancel_open_order(runner, clob=clob, view=view, reason="wrong_side")
            continue
        if keep_one is None:
            keep_one = view
            continue
        cancel_open_order(runner, clob=clob, view=view, reason="duplicate_open")
    if keep_one is not None and cycle_is_idle(runner):
        from kngtop.live_kilemo2 import _confirmed_position_state

        pos = _confirmed_position_state(runner)
        adopt_open_order(runner, view=keep_one, pm_up=pos.shares_up, pm_down=pos.shares_down)
    return bool(runner_open_order_views(runner)) or cycle_is_busy(runner)


def adopt_open_order(runner: WindowRunner, *, view: OpenOrderView, pm_up: float = -1.0, pm_down: float = -1.0) -> None:
    """Track an existing CLOB order in memory — no new send."""
    c = runner.cycle
    if not cycle_is_idle(runner):
        return
    c.cycle_n += 1
    c.phase = PHASE_WAIT_PRIMARY
    c.primary_side = view.side
    c.primary_price = view.price
    c.primary_shares = view.remaining_shares
    c.primary_order_id = view.order_id
    c.primary_reason = "adopted_open"
    c.primary_sent = True
    c.sends_this_cycle = 0
    c.pm_up_start = float(pm_up)
    c.pm_down_start = float(pm_down)
    _log_tag(
        "CYCLE ADOPT",
        slug=runner.contract.slug,
        cycle_n=str(c.cycle_n),
        side=c.primary_side,
        order_id=c.primary_order_id,
        price=f"{c.primary_price:.4f}",
    )


def projected_positions(runner: WindowRunner, base: PositionState) -> PositionState:
    from kngtop.live_kilemo2 import _copy_position_state

    state = _copy_position_state(base)
    order = active_order(runner)
    if order is None:
        return state
    reserved = order.reserved_shares()
    cost = reserved * order.price
    if order.side == "UP":
        state.shares_up += reserved
        state.spent_up += cost
    else:
        state.shares_down += reserved
        state.spent_down += cost
    return state


def _filtered_positions_for_runner(runner: WindowRunner, position_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    token_ids = {runner.contract.up.token_id, runner.contract.down.token_id}
    out: list[dict[str, Any]] = []
    for row in position_rows:
        slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        asset_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
        if slug and slug == runner.contract.slug:
            out.append(row)
        elif asset_id and asset_id in token_ids:
            out.append(row)
    return out


def reconcile_all(
    *,
    clob: KngtopClob,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any],
    runners: dict[int, WindowRunner],
) -> None:
    position_rows = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
    open_order_rows = clob.get_open_orders()
    now_ts = time.time()
    for runner in runners.values():
        sync_clob_open_orders(runner, clob=clob, open_rows=open_order_rows)
    runtime_state["reconcile_positions"] = position_rows
    runtime_state["reconcile_open_orders"] = open_order_rows
    runtime_state["reconcile_cache_at"] = time.perf_counter()
    runtime_state["reconcile_wall_ts"] = now_ts
    runtime_state["reconcile_seq"] = int(runtime_state.get("reconcile_seq", 0)) + 1


def run_live_reconcile_loop(
    stop: threading.Event,
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any],
    on_update: Callable[[], None] | None = None,
) -> None:
    while not stop.wait(RECONCILE_INTERVAL_SEC):
        if cfg.dry_run or clob is None:
            continue
        runners = runtime_state.get("runners")
        if not isinstance(runners, dict) or not runners:
            continue
        try:
            reconcile_all(clob=clob, cfg=cfg, runtime_state=runtime_state, runners=runners)
            if on_update is not None:
                on_update()
        except Exception as exc:  # noqa: BLE001
            _log_tag("RECONCILE", scope="global", status="error", error=str(exc))


# --- compat aliases ---
ORDER_OPEN = "open"
ORDER_FILLED = "filled"
ORDER_FAILED = "failed"
ORDER_INTENT = "intent"
ORDER_PARTIAL = "partial"
ORDER_POSTING = "posting"
ORDER_CANCELLED = "cancelled"
ACTIVE_STATUSES = frozenset({ORDER_OPEN})
TERMINAL_STATUSES = frozenset({ORDER_FAILED, ORDER_FILLED})


def register_intent(*args: object, **kwargs: object) -> LiveOrder | None:
    del args, kwargs
    return None


def mark_posted(order: LiveOrder | None, *, order_id: str) -> None:
    if order is None:
        return
    order.order_id = str(order_id)
    order.status = ORDER_OPEN


def mark_failed(order: LiveOrder | None, *, error: str) -> None:
    del error
    if order is None:
        return
    order.status = ORDER_FAILED


def mark_filled(order: LiveOrder | None, *, pm_shares: float) -> None:
    del pm_shares
    if order is None:
        return
    order.status = ORDER_FILLED


def mark_posting(order: LiveOrder | None) -> None:
    if order is None:
        return
    order.status = ORDER_POSTING


def parse_open_buy_order_row(row: dict[str, Any]) -> OpenOrderView | None:
    oid = _extract_order_id(row)
    if not oid:
        return None
    try:
        price = float(row.get("price") or 0.0)
    except (TypeError, ValueError):
        return None
    remaining = row.get("size_left") or row.get("remaining") or row.get("original_size") or row.get("size")
    try:
        rem = max(0.0, float(remaining or 0.0))
    except (TypeError, ValueError):
        rem = 0.0
    side = str(row.get("side") or "BUY").upper()
    return OpenOrderView(order_id=oid, side=side, price=price, remaining_shares=rem)


def _open_order_lookup(open_orders: dict[str, list[OpenOrderView]]) -> dict[str, OpenOrderView]:
    out: dict[str, OpenOrderView] = {}
    for views in open_orders.values():
        for view in views:
            out[view.order_id] = view
    return out


def _reconcile_order_statuses(*args: object, **kwargs: object) -> None:
    del args, kwargs


def _side_orders_map() -> dict[str, list[LiveOrder]]:
    return {"UP": [], "DOWN": []}


def reconcile_runner_orders(runner: WindowRunner, *, clob: KngtopClob | None, open_order_rows: list[dict[str, Any]], now_ts: float, **kwargs: object) -> None:
    del now_ts, kwargs
    sync_clob_open_orders(runner, clob=clob, open_rows=open_order_rows)


def log_deal_state(runner: WindowRunner) -> None:
    log_cycle(runner)
