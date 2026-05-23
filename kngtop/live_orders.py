"""Unified live order registry, reconcile, and CLOB I/O."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.pm_data import fetch_user_positions

if TYPE_CHECKING:
    from kngtop.live_kilemo2 import PositionState, WindowRunner

LOGGER = logging.getLogger("kngtop")

RECONCILE_INTERVAL_SEC = 1.0
MAX_ACTIVE_LIMIT_ORDERS = 1

ORDER_INTENT = "intent"
ORDER_POSTING = "posting"
ORDER_OPEN = "open"
ORDER_PARTIAL = "partial"
ORDER_FILLED = "filled"
ORDER_CANCELLED = "cancelled"
ORDER_FAILED = "failed"

ACTIVE_STATUSES = frozenset({ORDER_INTENT, ORDER_POSTING, ORDER_OPEN, ORDER_PARTIAL})
TERMINAL_STATUSES = frozenset({ORDER_FILLED, ORDER_CANCELLED, ORDER_FAILED})


@dataclass(slots=True)
class OpenOrderView:
    order_id: str
    side: str
    price: float
    remaining_shares: float


@dataclass(slots=True)
class LiveOrder:
    client_id: str
    side: str
    token_id: str
    price: float
    shares: float
    reason: str
    order_id: str | None = None
    status: str = ORDER_INTENT
    sent_ts: float = 0.0
    last_checked_ts: float = 0.0
    matched_shares: float = 0.0
    remaining_shares: float = 0.0
    last_error: str | None = None

    def is_active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    def reserved_shares(self) -> float:
        if self.status in TERMINAL_STATUSES:
            return 0.0
        if self.remaining_shares > 1e-12:
            return self.remaining_shares
        return self.shares


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


def _extract_order_id(payload: dict[str, object]) -> str | None:
    for key in ("orderID", "orderId", "id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _extract_numeric(payload: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        try:
            if value is None:
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _side_orders_map() -> dict[str, list[OpenOrderView]]:
    return {"UP": [], "DOWN": []}


def parse_open_buy_order_row(row: dict[str, object], *, token_id: str, side: str) -> OpenOrderView | None:
    asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
    if asset_id and asset_id != token_id:
        return None
    raw_side = str(row.get("side") or row.get("order_side") or "").strip().upper()
    if raw_side and raw_side != "BUY":
        return None
    order_id = _extract_order_id(row)
    price = _extract_numeric(row, "price")
    if not order_id or price is None or price <= 0.0:
        return None
    original_size = _extract_numeric(row, "original_size", "size", "makerAmount", "amount")
    matched_size = _extract_numeric(row, "size_matched", "matched_amount", "filled_amount", "filled", "makerAmountFilled")
    remaining_size = _extract_numeric(row, "size_left", "remaining", "remaining_amount", "size_remaining", "makerAmountRemaining")
    matched = max(0.0, float(matched_size or 0.0))
    if remaining_size is None and original_size is not None:
        remaining_size = max(0.0, float(original_size) - matched)
    if original_size is None and remaining_size is not None:
        original_size = max(0.0, float(remaining_size) + matched)
    if remaining_size is None:
        remaining_size = original_size
    remaining = max(0.0, float(remaining_size or 0.0))
    if remaining <= 1e-12:
        return None
    return OpenOrderView(order_id=order_id, side=side, price=float(price), remaining_shares=remaining)


def _parse_open_orders_for_runner(runner: WindowRunner, open_order_rows: list[dict[str, Any]]) -> dict[str, list[OpenOrderView]]:
    parsed = _side_orders_map()
    for side, token in (("UP", runner.contract.up), ("DOWN", runner.contract.down)):
        for row in open_order_rows:
            if not isinstance(row, dict):
                continue
            order = parse_open_buy_order_row(row, token_id=token.token_id, side=side)
            if order is not None:
                parsed[side].append(order)
    parsed["UP"].sort(key=lambda item: item.order_id)
    parsed["DOWN"].sort(key=lambda item: item.order_id)
    return parsed


def _open_order_lookup(open_orders: dict[str, list[OpenOrderView]]) -> dict[str, OpenOrderView]:
    lookup: dict[str, OpenOrderView] = {}
    for orders in open_orders.values():
        for order in orders:
            lookup[order.order_id] = order
    return lookup


def _filtered_positions_for_runner(runner: WindowRunner, position_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    token_ids = {runner.contract.up.token_id, runner.contract.down.token_id}
    for row in position_rows:
        slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        asset_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
        if slug and slug == runner.contract.slug:
            out.append(row)
            continue
        if asset_id and asset_id in token_ids:
            out.append(row)
    return out


def _filtered_open_orders_for_runner(runner: WindowRunner, open_order_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    token_ids = {runner.contract.up.token_id, runner.contract.down.token_id}
    out: list[dict[str, Any]] = []
    for row in open_order_rows:
        asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
        if asset_id and asset_id in token_ids:
            out.append(row)
    return out


def _find_order_by_clob_id(runner: WindowRunner, order_id: str | None) -> LiveOrder | None:
    if not order_id:
        return None
    for order in runner.orders.values():
        if order.order_id == order_id:
            return order
    return None


def active_order(runner: WindowRunner) -> LiveOrder | None:
    active: LiveOrder | None = None
    for order in runner.orders.values():
        if not order.is_active():
            continue
        if active is None or order.sent_ts >= active.sent_ts:
            active = order
    return active


def has_active_order(runner: WindowRunner) -> bool:
    return active_order(runner) is not None


def runner_open_order_views(runner: WindowRunner) -> list[OpenOrderView]:
    return list(runner.open_orders.get("UP", [])) + list(runner.open_orders.get("DOWN", []))


def _derive_open_orders_from_registry(runner: WindowRunner) -> dict[str, list[OpenOrderView]]:
    parsed = _side_orders_map()
    for order in runner.orders.values():
        if order.status not in {ORDER_OPEN, ORDER_PARTIAL} or not order.order_id:
            continue
        remaining = order.remaining_shares if order.remaining_shares > 1e-12 else max(0.0, order.shares - order.matched_shares)
        if remaining <= 1e-12:
            continue
        parsed[order.side].append(
            OpenOrderView(order_id=order.order_id, side=order.side, price=order.price, remaining_shares=remaining)
        )
    for side in ("UP", "DOWN"):
        parsed[side].sort(key=lambda item: item.order_id)
    return parsed


def _sync_open_orders_cache(runner: WindowRunner, clob_open: dict[str, list[OpenOrderView]]) -> None:
    runner.open_orders = clob_open


def register_intent(
    runner: WindowRunner,
    *,
    side: str,
    token_id: str,
    price: float,
    shares: float,
    reason: str,
    sent_ts: float,
) -> LiveOrder:
    order = LiveOrder(
        client_id=str(uuid.uuid4()),
        side=str(side).upper(),
        token_id=str(token_id),
        price=float(price),
        shares=float(shares),
        reason=str(reason),
        status=ORDER_INTENT,
        sent_ts=float(sent_ts),
        remaining_shares=float(shares),
    )
    runner.orders[order.client_id] = order
    _log_tag(
        "ORDER INTENT",
        slug=runner.contract.slug,
        client_id=order.client_id,
        side=order.side,
        price=f"{order.price:.4f}",
        shares=f"{order.shares:.6f}",
        reason=order.reason,
    )
    return order


def mark_posting(order: LiveOrder) -> None:
    order.status = ORDER_POSTING


def mark_posted(order: LiveOrder, *, order_id: str) -> None:
    order.order_id = str(order_id)
    order.status = ORDER_OPEN
    order.remaining_shares = order.shares
    order.matched_shares = 0.0
    _log_tag("ORDER POSTED", order_id=order.order_id, side=order.side, status=order.status)


def mark_failed(order: LiveOrder, *, error: str) -> None:
    order.status = ORDER_FAILED
    order.last_error = error
    order.remaining_shares = 0.0
    order.last_checked_ts = time.time()


def mark_filled(order: LiveOrder) -> None:
    order.status = ORDER_FILLED
    order.matched_shares = order.shares
    order.remaining_shares = 0.0
    order.last_checked_ts = time.time()


def _update_order_from_open_row(order: LiveOrder, open_row: OpenOrderView, *, now_ts: float) -> None:
    order.last_checked_ts = now_ts
    order.remaining_shares = open_row.remaining_shares
    order.matched_shares = max(0.0, order.shares - open_row.remaining_shares)
    if order.matched_shares > 1e-12 and open_row.remaining_shares > 1e-12:
        order.status = ORDER_PARTIAL
    else:
        order.status = ORDER_OPEN


def _update_order_from_get_order(order: LiveOrder, payload: dict[str, object], *, now_ts: float) -> None:
    matched = _extract_numeric(payload, "size_matched", "matched_amount", "filled", "makerAmountFilled") or 0.0
    original = _extract_numeric(payload, "original_size", "size", "makerAmount", "amount") or order.shares
    remaining = _extract_numeric(payload, "size_left", "remaining", "remaining_amount", "size_remaining")
    if remaining is None:
        remaining = max(0.0, float(original) - float(matched))
    order.matched_shares = max(0.0, float(matched))
    order.remaining_shares = max(0.0, float(remaining))
    order.last_checked_ts = now_ts
    status_text = str(payload.get("status") or payload.get("order_status") or "").strip().lower()
    if order.remaining_shares <= 1e-12 and order.matched_shares + 1e-12 >= order.shares:
        order.status = ORDER_FILLED
    elif status_text in {"canceled", "cancelled", "expired"}:
        order.status = ORDER_CANCELLED
    elif order.matched_shares > 1e-12 and order.remaining_shares > 1e-12:
        order.status = ORDER_PARTIAL
    elif order.matched_shares > 1e-12:
        order.status = ORDER_FILLED
    elif order.remaining_shares > 1e-12:
        order.status = ORDER_OPEN


def _reconcile_order_statuses(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    open_lookup: dict[str, OpenOrderView],
    now_ts: float,
) -> None:
    for order in runner.orders.values():
        if not order.is_active() or not order.order_id:
            continue
        open_row = open_lookup.get(order.order_id)
        if open_row is not None:
            _update_order_from_open_row(order, open_row, now_ts=now_ts)
            continue
        if order.status in {ORDER_INTENT, ORDER_POSTING}:
            continue
        if clob is None:
            continue
        try:
            payload = clob.get_order(order.order_id)
        except Exception as exc:  # noqa: BLE001
            order.last_error = str(exc)
            order.last_checked_ts = now_ts
            continue
        if not isinstance(payload, dict) or not payload:
            continue
        _update_order_from_get_order(order, payload, now_ts=now_ts)
        if order.status in TERMINAL_STATUSES:
            _log_tag(
                "ORDER LIFECYCLE",
                slug=runner.contract.slug,
                order_id=order.order_id,
                side=order.side,
                status=order.status,
                matched=f"{order.matched_shares:.6f}",
            )


def _adopt_orphan_open_orders(runner: WindowRunner, open_lookup: dict[str, OpenOrderView], *, now_ts: float) -> None:
    token_by_side = {"UP": runner.contract.up.token_id, "DOWN": runner.contract.down.token_id}
    for view in open_lookup.values():
        if _find_order_by_clob_id(runner, view.order_id) is not None:
            continue
        order = LiveOrder(
            client_id=str(uuid.uuid4()),
            side=view.side,
            token_id=token_by_side.get(view.side, ""),
            price=view.price,
            shares=view.remaining_shares,
            reason="adopted_open_order",
            order_id=view.order_id,
            status=ORDER_OPEN,
            sent_ts=now_ts,
            remaining_shares=view.remaining_shares,
        )
        runner.orders[order.client_id] = order
        _log_tag(
            "ORDER ADOPTED",
            slug=runner.contract.slug,
            order_id=order.order_id,
            side=order.side,
            price=f"{order.price:.4f}",
            shares=f"{order.remaining_shares:.6f}",
        )


def reconcile_runner_orders(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    open_order_rows: list[dict[str, Any]],
    now_ts: float,
) -> None:
    scoped_open = _filtered_open_orders_for_runner(runner, open_order_rows)
    clob_open = _parse_open_orders_for_runner(runner, scoped_open)
    open_lookup = _open_order_lookup(clob_open)
    _adopt_orphan_open_orders(runner, open_lookup, now_ts=now_ts)
    _reconcile_order_statuses(runner, clob=clob, open_lookup=open_lookup, now_ts=now_ts)
    _sync_open_orders_cache(runner, clob_open)


def projected_positions(runner: WindowRunner, base: PositionState) -> PositionState:
    from kngtop.live_kilemo2 import PositionState as PS, _copy_position_state

    state = _copy_position_state(base)
    order = active_order(runner)
    if order is None:
        return state
    reserved = order.reserved_shares()
    if reserved <= 1e-12:
        return state
    cost = reserved * order.price
    if order.side == "UP":
        state.shares_up += reserved
        state.spent_up += cost
    else:
        state.shares_down += reserved
        state.spent_down += cost
    return state


def cancel_open_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    view: OpenOrderView,
    reason: str,
) -> bool:
    if clob is None:
        return False
    try:
        clob.cancel_order_by_id(view.order_id)
    except Exception as exc:  # noqa: BLE001
        _log_tag(
            "LIMIT CANCEL",
            slug=runner.contract.slug,
            side=view.side,
            order_id=view.order_id,
            reason=reason,
            error=str(exc),
        )
        return False
    record = _find_order_by_clob_id(runner, view.order_id)
    if record is not None:
        record.status = ORDER_CANCELLED
        record.remaining_shares = 0.0
        record.last_checked_ts = time.time()
    runner.open_orders[view.side] = [
        row for row in runner.open_orders.get(view.side, []) if row.order_id != view.order_id
    ]
    _log_tag(
        "LIMIT CANCEL",
        slug=runner.contract.slug,
        side=view.side,
        order_id=view.order_id,
        reason=reason,
        price=f"{view.price:.4f}",
        shares=f"{view.remaining_shares:.6f}",
    )
    return True


def enforce_single_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    required_side: str | None,
) -> bool:
    open_views = runner_open_order_views(runner)
    if required_side is not None:
        wrong = [row for row in open_views if row.side != required_side]
        for row in wrong:
            cancel_open_order(runner, clob=clob, view=row, reason="wrong_balance_side")
        open_views = [row for row in open_views if row.side == required_side]

    if len(open_views) > MAX_ACTIVE_LIMIT_ORDERS:
        keep = open_views[0]
        for extra in open_views[MAX_ACTIVE_LIMIT_ORDERS:]:
            cancel_open_order(runner, clob=clob, view=extra, reason="duplicate_open_order")
        open_views = [keep]
        _log_tag(
            "OPEN_ORDERS",
            slug=runner.contract.slug,
            status="deduped",
            kept=keep.order_id,
            cancelled=str(len(open_views) - 1),
        )

    if open_views:
        _log_tag(
            "OPEN_ORDERS",
            slug=runner.contract.slug,
            status="active",
            side=open_views[0].side,
            order_id=open_views[0].order_id,
            price=f"{open_views[0].price:.4f}",
            shares=f"{open_views[0].remaining_shares:.6f}",
        )
        return True
    return has_active_order(runner)


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
    now_mono = time.perf_counter()

    for runner in runners.values():
        reconcile_runner_orders(runner, clob=clob, open_order_rows=open_order_rows, now_ts=now_ts)

    runtime_state["reconcile_positions"] = position_rows
    runtime_state["reconcile_open_orders"] = open_order_rows
    runtime_state["reconcile_cache_at"] = now_mono
    runtime_state["reconcile_wall_ts"] = now_ts
    runtime_state["reconcile_seq"] = int(runtime_state.get("reconcile_seq", 0)) + 1
    _log_tag(
        "RECONCILE",
        scope="global",
        seq=str(runtime_state["reconcile_seq"]),
        positions=str(len(position_rows)),
        open_orders=str(len(open_order_rows)),
        windows=str(len(runners)),
    )


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


def clear_window_orders(runner: WindowRunner) -> None:
    runner.orders.clear()
    runner.open_orders = {"UP": [], "DOWN": []}
