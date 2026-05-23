"""Backward-compatible re-exports; use kngtop.live_orders for new code."""

from kngtop.live_orders import (
    LiveOrder,
    OpenOrderView,
    RECONCILE_INTERVAL_SEC,
    active_order,
    adopt_open_order,
    clear_window_orders,
    cycle_begin_primary,
    cycle_is_busy,
    cycle_is_idle,
    enforce_single_order,
    has_active_order,
    order_on_clob,
    reconcile_all,
    reconcile_runner_orders,
    run_live_reconcile_loop,
    sync_clob_open_orders,
)

SentOrderRecord = LiveOrder
TrackedLimitOrder = OpenOrderView


def refresh_live_reconcile_cache(*, clob, cfg, runtime_state, runners):
    reconcile_all(clob=clob, cfg=cfg, runtime_state=runtime_state, runners=runners)


def runner_active_open_orders(runner):
    from kngtop.live_orders import runner_open_order_views

    return runner_open_order_views(runner)


def runner_has_active_open_order(runner) -> bool:
    return has_active_order(runner)
