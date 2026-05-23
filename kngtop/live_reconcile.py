"""Backward-compatible re-exports; use kngtop.live_orders for new code."""

from kngtop.live_orders import (
    ACTIVE_STATUSES,
    LiveOrder,
    OpenOrderView,
    ORDER_CANCELLED,
    ORDER_FAILED,
    ORDER_FILLED,
    ORDER_INTENT,
    ORDER_OPEN,
    ORDER_PARTIAL,
    ORDER_POSTING,
    RECONCILE_INTERVAL_SEC,
    TERMINAL_STATUSES,
    active_order,
    cancel_open_order,
    clear_window_orders,
    enforce_single_order,
    has_active_order,
    parse_open_buy_order_row,
    projected_positions,
    reconcile_all,
    reconcile_runner_orders,
    register_intent,
    run_live_reconcile_loop,
)

# Legacy aliases
SENT_SENT = ORDER_INTENT
SENT_OPEN = ORDER_OPEN
SENT_PARTIAL = ORDER_PARTIAL
SENT_FILLED = ORDER_FILLED
SENT_CANCELLED = ORDER_CANCELLED
SENT_FAILED = ORDER_FAILED
TERMINAL_SENT_STATUSES = TERMINAL_STATUSES

SentOrderRecord = LiveOrder
TrackedLimitOrder = OpenOrderView


def register_sent_order(runner, *, order_id: str, side: str, token_id: str, price: float, shares: float, reason: str, sent_ts: float):
    order = register_intent(runner, side=side, token_id=token_id, price=price, shares=shares, reason=reason, sent_ts=sent_ts)
    if order_id:
        from kngtop.live_orders import mark_posted

        mark_posted(order, order_id=str(order_id))
    return order


def mark_sent_order_failed(runner, order_id: str | None, *, error: str) -> None:
    from kngtop.live_orders import mark_failed

    if not order_id:
        return
    for order in runner.orders.values():
        if order.order_id == str(order_id):
            mark_failed(order, error=error)
            return


def runner_active_open_orders(runner):
    return __import__("kngtop.live_orders", fromlist=["runner_open_order_views"]).runner_open_order_views(runner)


def runner_has_active_open_order(runner) -> bool:
    return has_active_order(runner)


def refresh_live_reconcile_cache(*, clob, cfg, runtime_state, runners):
    reconcile_all(clob=clob, cfg=cfg, runtime_state=runtime_state, runners=runners)


def _filtered_positions_for_runner(runner, position_rows):
    from kngtop.live_orders import _filtered_positions_for_runner as fn

    return fn(runner, position_rows)


def _filtered_open_orders_for_runner(runner, open_order_rows):
    from kngtop.live_orders import _filtered_open_orders_for_runner as fn

    return fn(runner, open_order_rows)


def _parse_open_orders_for_runner(runner, open_order_rows):
    from kngtop.live_orders import _parse_open_orders_for_runner as fn

    return fn(runner, open_order_rows)


def _open_order_lookup(open_orders):
    from kngtop.live_orders import _open_order_lookup as fn

    return fn(open_orders)


def _update_sent_orders_from_open_lookup(runner, *, open_lookup, now_ts):
    from kngtop.live_orders import _reconcile_order_statuses

    _reconcile_order_statuses(runner, clob=None, open_lookup=open_lookup, now_ts=now_ts)


def _update_sent_orders_from_get_order(runner, *, clob, open_lookup, now_ts):
    from kngtop.live_orders import _reconcile_order_statuses

    _reconcile_order_statuses(runner, clob=clob, open_lookup=open_lookup, now_ts=now_ts)


def _side_orders_map():
    from kngtop.live_orders import _side_orders_map as fn

    return fn()
