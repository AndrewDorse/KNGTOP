"""Live trading reconcile: positions, open orders, and sent-order lifecycle."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract
from kngtop.pm_data import fetch_user_positions

if TYPE_CHECKING:
    from kngtop.live_kilemo2 import TrackedLimitOrder, WindowRunner

LOGGER = logging.getLogger("kngtop")

RECONCILE_INTERVAL_SEC = 1.0

SENT_SENT = "sent"
SENT_OPEN = "open"
SENT_PARTIAL = "partial"
SENT_FILLED = "filled"
SENT_CANCELLED = "cancelled"
SENT_FAILED = "failed"

TERMINAL_SENT_STATUSES = frozenset({SENT_FILLED, SENT_CANCELLED, SENT_FAILED})


@dataclass(slots=True)
class SentOrderRecord:
    order_id: str
    side: str
    token_id: str
    price: float
    shares: float
    reason: str
    status: str = SENT_SENT
    sent_ts: float = 0.0
    last_checked_ts: float = 0.0
    matched_shares: float = 0.0
    remaining_shares: float = 0.0
    last_error: str | None = None

    def is_active(self) -> bool:
        return self.status not in TERMINAL_SENT_STATUSES


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


def _side_orders_map() -> dict[str, list[TrackedLimitOrder]]:
    return {"UP": [], "DOWN": []}


def register_sent_order(
    runner: WindowRunner,
    *,
    order_id: str,
    side: str,
    token_id: str,
    price: float,
    shares: float,
    reason: str,
    sent_ts: float,
) -> SentOrderRecord:
    record = SentOrderRecord(
        order_id=str(order_id),
        side=str(side).upper(),
        token_id=str(token_id),
        price=float(price),
        shares=float(shares),
        reason=str(reason),
        status=SENT_SENT,
        sent_ts=float(sent_ts),
        remaining_shares=float(shares),
    )
    runner.sent_orders[record.order_id] = record
    _log_tag(
        "ORDER REGISTERED",
        slug=runner.contract.slug,
        order_id=record.order_id,
        side=record.side,
        price=f"{record.price:.4f}",
        shares=f"{record.shares:.6f}",
        reason=record.reason,
    )
    return record


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


def parse_open_buy_order_row(row: dict[str, object], *, token_id: str, side: str) -> TrackedLimitOrder | None:
    from kngtop.live_kilemo2 import TrackedLimitOrder

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
    return TrackedLimitOrder(order_id=order_id, side=side, price=float(price), remaining_shares=remaining)


def _parse_open_orders_for_runner(runner: WindowRunner, open_order_rows: list[dict[str, Any]]) -> dict[str, list[TrackedLimitOrder]]:
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


def _open_order_lookup(open_orders: dict[str, list[TrackedLimitOrder]]) -> dict[str, TrackedLimitOrder]:
    lookup: dict[str, TrackedLimitOrder] = {}
    for orders in open_orders.values():
        for order in orders:
            lookup[order.order_id] = order
    return lookup


def _update_sent_orders_from_open_lookup(
    runner: WindowRunner,
    *,
    open_lookup: dict[str, TrackedLimitOrder],
    now_ts: float,
) -> None:
    for record in runner.sent_orders.values():
        if not record.is_active():
            continue
        record.last_checked_ts = now_ts
        open_row = open_lookup.get(record.order_id)
        if open_row is not None:
            record.remaining_shares = open_row.remaining_shares
            record.matched_shares = max(0.0, record.shares - open_row.remaining_shares)
            if record.matched_shares > 1e-12 and open_row.remaining_shares > 1e-12:
                record.status = SENT_PARTIAL
            else:
                record.status = SENT_OPEN
            continue
        if record.status == SENT_SENT:
            continue
        record.remaining_shares = 0.0
        if record.matched_shares + 1e-12 >= record.shares:
            record.status = SENT_FILLED
        else:
            record.status = SENT_CANCELLED


def _update_sent_orders_from_get_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob,
    open_lookup: dict[str, TrackedLimitOrder],
    now_ts: float,
) -> None:
    for record in runner.sent_orders.values():
        if not record.is_active():
            continue
        if record.order_id in open_lookup:
            continue
        try:
            payload = clob.get_order(record.order_id)
        except Exception as exc:  # noqa: BLE001
            record.last_error = str(exc)
            record.last_checked_ts = now_ts
            continue
        if not isinstance(payload, dict) or not payload:
            continue
        matched = _extract_numeric(payload, "size_matched", "matched_amount", "filled", "makerAmountFilled") or 0.0
        original = _extract_numeric(payload, "original_size", "size", "makerAmount", "amount") or record.shares
        remaining = _extract_numeric(payload, "size_left", "remaining", "remaining_amount", "size_remaining")
        if remaining is None:
            remaining = max(0.0, float(original) - float(matched))
        record.matched_shares = max(0.0, float(matched))
        record.remaining_shares = max(0.0, float(remaining))
        record.last_checked_ts = now_ts
        status_text = str(payload.get("status") or payload.get("order_status") or "").strip().lower()
        if record.remaining_shares <= 1e-12 and record.matched_shares + 1e-12 >= record.shares:
            record.status = SENT_FILLED
        elif status_text in {"canceled", "cancelled", "expired"}:
            record.status = SENT_CANCELLED
        elif record.matched_shares > 1e-12 and record.remaining_shares > 1e-12:
            record.status = SENT_PARTIAL
        elif record.matched_shares > 1e-12:
            record.status = SENT_FILLED
        elif record.status == SENT_SENT and record.remaining_shares > 1e-12:
            record.status = SENT_OPEN


def mark_sent_order_failed(runner: WindowRunner, order_id: str | None, *, error: str) -> None:
    if not order_id:
        return
    record = runner.sent_orders.get(str(order_id))
    if record is None:
        return
    record.status = SENT_FAILED
    record.last_error = error
    record.remaining_shares = 0.0
    record.last_checked_ts = time.time()


def runner_active_open_orders(runner: WindowRunner) -> list[TrackedLimitOrder]:
    return list(runner.open_orders.get("UP", [])) + list(runner.open_orders.get("DOWN", []))


def runner_has_active_open_order(runner: WindowRunner) -> bool:
    return len(runner_active_open_orders(runner)) > 0


def refresh_live_reconcile_cache(
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
        scoped_open = _filtered_open_orders_for_runner(runner, open_order_rows)
        runner.open_orders = _parse_open_orders_for_runner(runner, scoped_open)
        open_lookup = _open_order_lookup(runner.open_orders)
        _update_sent_orders_from_open_lookup(runner, open_lookup=open_lookup, now_ts=now_ts)
        _update_sent_orders_from_get_order(runner, clob=clob, open_lookup=open_lookup, now_ts=now_ts)

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
    """Background loop: positions + open orders + sent-order status every second."""
    while not stop.wait(RECONCILE_INTERVAL_SEC):
        if cfg.dry_run or clob is None:
            continue
        runners = runtime_state.get("runners")
        if not isinstance(runners, dict) or not runners:
            continue
        try:
            refresh_live_reconcile_cache(clob=clob, cfg=cfg, runtime_state=runtime_state, runners=runners)
            if on_update is not None:
                on_update()
        except Exception as exc:  # noqa: BLE001
            _log_tag("RECONCILE", scope="global", status="error", error=str(exc))
