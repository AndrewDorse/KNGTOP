"""BTC 5m live bot for guarded PnL-balance strategy C."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_window_open_px
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.eval_coordinator import EvalCoordinator
from kngtop.gamma import (
    ActiveContract,
    TokenMarket,
    discover_updown_window_by_start,
    window_start_ts_from_slug,
)
from kngtop.pm_data import fetch_user_positions
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")

TRADE_PAIR_KEY = "BTC"
TRADE_WINDOW_MINUTES = 5
WINDOW_SECONDS = TRADE_WINDOW_MINUTES * 60
NEXT_WINDOW_LOOKAHEAD_SEC = 20

MIN_ORDER_USD = 1.0
LARGE_ORDER_USD = 2.0
LIMIT_MIN_SHARES = 5.0
LIMIT_MIN_USD = 1.05
LIMIT_PASSIVE_OFFSET = 0.01
MAX_SHARES_PER_SIDE = 15.0
MAX_SHARE_GAP = 2.0

BOOTSTRAP_CHEAP_CAP = 0.55
ACTIVE_REPAIR_INTERVAL_SEC = 5
IMBALANCE_TRIGGER = 0.20
AVG_SUM_CAP = 0.95
REPAIR_AVG_SUM_CAP = 0.95
WEAK_REPAIR_CHEAP_CAP = 0.45
HIGH_REPAIR_GUARD = 0.60
HIGH_REPAIR_PRE240_CAP = 0.65
FINAL_60_DANGER_CAP = 0.80
LOCKED_PROFIT_IMBALANCE_TRIGGER = 0.25
LOCKED_PROFIT_STOP_AVG_SUM = 0.95
LOCKED_PROFIT_STOP_IMBALANCE = 0.10
LOCKED_PROFIT_STOP_ROI = 0.10
HIGH_REPAIR_WORST_TARGET = -0.25
HIGH_REPAIR_SHARE_GAP_TARGET = 0.10
DANGEROUS_WEAK_PNL = -2.0
REPAIR_PRICE_IMPROVEMENT_BUFFER = 0.02
INITIAL_RETRY_WAIT_SEC = 10.0
INITIAL_RETRY_PRICE_IMPROVEMENT = 0.02
MAX_INITIAL_RETRIES_PER_SIDE = 2
MAX_ORDER_PRICE = 0.99
MAX_ACTIVE_LIMIT_ORDERS = 1
POST_ORDER_RECONCILE_TIMEOUT_SEC = 8.0
POST_ORDER_RECONCILE_POLL_SEC = 0.25
READY = "READY"
ORDER_IN_FLIGHT = "ORDER_IN_FLIGHT"
WAIT_NEXT_DECISION = "WAIT_NEXT_DECISION"


@dataclass(slots=True)
class TrackedLimitOrder:
    order_id: str
    side: str
    price: float
    remaining_shares: float


@dataclass(slots=True)
class BuyAction:
    side: str
    ask_px: float
    amount_usd: float
    reason: str
    enforce_avg_cap: bool = True


@dataclass(slots=True)
class PositionState:
    spent_up: float = 0.0
    spent_down: float = 0.0
    shares_up: float = 0.0
    shares_down: float = 0.0
    orders_up: int = 0
    orders_down: int = 0
    total_deals: int = 0

    def spent_total(self) -> float:
        return self.spent_up + self.spent_down

    def pnl_if_up(self) -> float:
        return self.shares_up - self.spent_total()

    def pnl_if_down(self) -> float:
        return self.shares_down - self.spent_total()

    def avg_up(self) -> float:
        return self.spent_up / self.shares_up if self.shares_up > 1e-12 else 0.0

    def avg_down(self) -> float:
        return self.spent_down / self.shares_down if self.shares_down > 1e-12 else 0.0

    def share_imbalance(self) -> float:
        total = self.shares_up + self.shares_down
        if total <= 1e-12:
            return 0.0
        return abs(self.shares_up - self.shares_down) / total

    def both_sides_traded(self) -> bool:
        return has_real_position(self, "UP") and has_real_position(self, "DOWN")


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    positions: PositionState = field(default_factory=PositionState)
    local_positions: PositionState = field(default_factory=PositionState)
    api_positions: PositionState = field(default_factory=PositionState)
    last_repair_slot: int = -1
    last_missing_wait_log_slot: int = -1
    pending_order: bool = False
    pending_side: str | None = None
    pending_order_id: str | None = None
    pending_reason: str | None = None
    pending_ask_px: float = 0.0
    pending_amount_usd: float = 0.0
    pending_reserved_shares: float = 0.0
    pending_created_ts: float = 0.0
    last_successful_buy_ts: float = -10_000.0
    last_position_refresh_ts: float = 0.0
    execution_state: str = READY
    next_decision_ts: float = 0.0
    stop_reason: str | None = None
    initial_intent_attempted: bool = False
    initial_filled: bool = False
    last_initial_attempt_side: str | None = None
    last_initial_attempt_price: float = 0.0
    last_initial_attempt_elapsed: float = -10_000.0
    initial_failed_up: int = 0
    initial_failed_down: int = 0
    intent_count_up: int = 0
    intent_count_down: int = 0

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lv,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy_name in ("websocket", "urllib3", "httpx", "httpcore", "py_clob_client_v2", "py_clob_client_v2.http_helpers.helpers"):
        noisy = logging.getLogger(noisy_name)
        noisy.setLevel(logging.CRITICAL)
        noisy.propagate = False


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


def _ws_reconnected_event(feed: str, downtime_sec: float) -> None:
    del feed, downtime_sec


def _log_ws_update(runtime_state: dict[str, Any], *, feed: str, symbol: str | None = None) -> None:
    del runtime_state, feed, symbol


def _extract_order_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("orderID", "orderId", "id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _clear_pending_limit_state(runner: WindowRunner) -> None:
    runner.pending_order = False
    runner.pending_order_id = None
    runner.pending_side = None
    runner.pending_reason = None
    runner.pending_ask_px = 0.0
    runner.pending_amount_usd = 0.0
    runner.pending_reserved_shares = 0.0
    runner.pending_created_ts = 0.0


def _adopt_tracked_limit_order(runner: WindowRunner, order: TrackedLimitOrder, *, reason: str) -> None:
    runner.pending_order = True
    runner.pending_order_id = order.order_id
    runner.pending_side = order.side
    runner.pending_reason = reason
    runner.pending_ask_px = order.price
    runner.pending_amount_usd = order.remaining_shares * order.price
    runner.pending_reserved_shares = order.remaining_shares
    runner.execution_state = ORDER_IN_FLIGHT


def _parse_open_buy_order_row(row: dict[str, object], *, token_id: str, side: str) -> TrackedLimitOrder | None:
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


def _fetch_window_open_buy_orders(runner: WindowRunner, clob: KngtopClob | None) -> list[TrackedLimitOrder]:
    if clob is None:
        return []
    orders: list[TrackedLimitOrder] = []
    for side, token in (("UP", runner.contract.up), ("DOWN", runner.contract.down)):
        try:
            rows = clob.get_open_orders_for_asset(token)
        except Exception as exc:  # noqa: BLE001
            _log_tag("OPEN_ORDERS", slug=runner.contract.slug, side=side, status="error", error=str(exc))
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            parsed = _parse_open_buy_order_row(row, token_id=token.token_id, side=side)
            if parsed is not None:
                orders.append(parsed)
    return orders


def _cancel_tracked_limit_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    order: TrackedLimitOrder,
    reason: str,
) -> bool:
    if clob is None:
        return False
    try:
        clob.cancel_order_by_id(order.order_id)
    except Exception as exc:  # noqa: BLE001
        _log_tag(
            "LIMIT CANCEL",
            slug=runner.contract.slug,
            side=order.side,
            order_id=order.order_id,
            reason=reason,
            error=str(exc),
        )
        return False
    if runner.pending_order_id == order.order_id:
        _clear_pending_limit_state(runner)
        runner.execution_state = READY
    _log_tag(
        "LIMIT CANCEL",
        slug=runner.contract.slug,
        side=order.side,
        order_id=order.order_id,
        reason=reason,
        price=f"{order.price:.4f}",
        shares=f"{order.remaining_shares:.6f}",
    )
    return True


def _sync_and_enforce_single_open_limit(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> bool:
    if cfg.dry_run or clob is None:
        return runner.pending_order

    open_orders = _fetch_window_open_buy_orders(runner, clob)
    confirmed = _confirmed_position_state(runner)
    required_side = _required_hedge_side(confirmed, cfg)

    if required_side is not None:
        wrong_side_orders = [order for order in open_orders if order.side != required_side]
        for order in wrong_side_orders:
            _cancel_tracked_limit_order(runner, clob=clob, order=order, reason="wrong_balance_side")
        open_orders = [order for order in open_orders if order.side == required_side]

    if len(open_orders) > MAX_ACTIVE_LIMIT_ORDERS:
        keep = open_orders[0]
        extras = open_orders[MAX_ACTIVE_LIMIT_ORDERS:]
        for extra in extras:
            _cancel_tracked_limit_order(runner, clob=clob, order=extra, reason="duplicate_open_order")
        open_orders = [keep]
        _log_tag(
            "OPEN_ORDERS",
            slug=runner.contract.slug,
            status="deduped",
            kept=keep.order_id,
            cancelled=str(len(extras)),
        )

    if open_orders:
        active = open_orders[0]
        if (
            not runner.pending_order
            or runner.pending_order_id != active.order_id
            or runner.pending_side != active.side
        ):
            _adopt_tracked_limit_order(runner, active, reason="synced_open_order")
        _log_tag(
            "OPEN_ORDERS",
            slug=runner.contract.slug,
            status="active",
            side=active.side,
            order_id=active.order_id,
            price=f"{active.price:.4f}",
            shares=f"{active.remaining_shares:.6f}",
        )
        return True

    if runner.pending_order:
        if runner.pending_order_id:
            _clear_pending_limit_state(runner)
            runner.execution_state = READY
        else:
            return True
    return False


def _drop_tracked_open_order(clob: KngtopClob | None, order_id: str | None) -> None:
    if clob is None or not order_id:
        return
    drop = getattr(clob, "drop_open_order", None)
    if callable(drop):
        drop(str(order_id))


def _needs_urgent_balance_order(runner: WindowRunner, cfg: KngtopConfig) -> bool:
    confirmed = _confirmed_position_state(runner)
    required_side = _required_hedge_side(confirmed, cfg)
    if required_side is None:
        return False
    if runner.pending_order and runner.pending_side == required_side:
        return False
    return True


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


def _extract_filled_shares(payload: object, *, fallback_amount_usd: float = 0.0, fallback_price: float = 0.0) -> float:
    if not isinstance(payload, dict):
        return 0.0
    share_keys = (
        "size_matched",
        "matched_amount",
        "filled_amount",
        "filled",
        "makerAmountFilled",
        "makingAmount",
        "makerAmount",
        "size",
    )
    value = _extract_numeric(payload, *share_keys)
    if value is not None:
        return max(0.0, float(value))
    for nested_key in ("order", "data", "result"):
        nested = payload.get(nested_key)
        if not isinstance(nested, dict):
            continue
        nested_value = _extract_numeric(nested, *share_keys)
        if nested_value is not None:
            return max(0.0, float(nested_value))
    explicit_zero = _extract_numeric(payload, "size_matched", "matched_amount", "filled_amount", "filled") == 0.0
    success = bool(payload.get("success") is True or payload.get("status") in {"success", "matched", "filled"} or _extract_order_id(payload))
    if success and not explicit_zero and fallback_amount_usd > 0.0 and fallback_price > 0.0:
        return fallback_amount_usd / max(fallback_price, 1e-9)
    return 0.0


def _current_window_start_sec(now_ts: int, window_minutes: int) -> int:
    window_sec = max(60, int(window_minutes) * 60)
    return (int(now_ts) // window_sec) * window_sec


def _candidate_window_starts(now_ts: int) -> tuple[int, ...]:
    current_start = _current_window_start_sec(now_ts, TRADE_WINDOW_MINUTES)
    next_start = current_start + WINDOW_SECONDS
    if next_start - int(now_ts) <= NEXT_WINDOW_LOOKAHEAD_SEC:
        return (current_start, next_start)
    return (current_start,)


def _window_elapsed_remaining(runner: WindowRunner, now_ts: float) -> tuple[float | None, float | None]:
    start_sec = runner.start_sec()
    if start_sec is None:
        return None, None
    elapsed = float(now_ts) - float(start_sec)
    remaining = float(runner.window_minutes * 60) - elapsed
    return elapsed, remaining


def _token_for_side(runner: WindowRunner, side: str) -> TokenMarket:
    return runner.contract.up if str(side).upper() == "UP" else runner.contract.down


def has_real_position(state: PositionState, side: str) -> bool:
    shares = state.shares_up if side == "UP" else state.shares_down
    return float(shares) > 0.000001


def _weak_outcome_side(state: PositionState) -> str | None:
    up = state.pnl_if_up()
    down = state.pnl_if_down()
    if up < down - 1e-12:
        return "UP"
    if down < up - 1e-12:
        return "DOWN"
    return None


def _other_side(side: str) -> str:
    return "DOWN" if side == "UP" else "UP"


def _copy_position_state(state: PositionState) -> PositionState:
    return PositionState(
        spent_up=state.spent_up,
        spent_down=state.spent_down,
        shares_up=state.shares_up,
        shares_down=state.shares_down,
        orders_up=state.orders_up,
        orders_down=state.orders_down,
        total_deals=state.total_deals,
    )


def _ensure_local_position_floor(runner: WindowRunner) -> None:
    if runner.local_positions.total_deals > 0 or runner.positions.total_deals <= 0:
        return
    runner.local_positions = _copy_position_state(runner.positions)


def _side_cost_for_shares(state: PositionState, side: str, shares: float) -> float:
    state_shares = state.shares_up if side == "UP" else state.shares_down
    state_spent = state.spent_up if side == "UP" else state.spent_down
    if shares <= 1e-12 or state_shares <= 1e-12 or state_spent <= 1e-12:
        return 0.0
    return (state_spent / state_shares) * shares


def _merge_effective_positions(local: PositionState, api: PositionState) -> PositionState:
    up_shares = max(local.shares_up, api.shares_up)
    down_shares = max(local.shares_down, api.shares_down)
    return PositionState(
        spent_up=max(_side_cost_for_shares(local, "UP", up_shares), _side_cost_for_shares(api, "UP", up_shares)),
        spent_down=max(_side_cost_for_shares(local, "DOWN", down_shares), _side_cost_for_shares(api, "DOWN", down_shares)),
        shares_up=up_shares,
        shares_down=down_shares,
        orders_up=max(local.orders_up, api.orders_up),
        orders_down=max(local.orders_down, api.orders_down),
        total_deals=max(local.orders_up, api.orders_up) + max(local.orders_down, api.orders_down),
    )


def _sync_effective_positions(runner: WindowRunner) -> PositionState:
    _ensure_local_position_floor(runner)
    runner.positions = _merge_effective_positions(runner.local_positions, runner.api_positions)
    return runner.positions


def _shares_for_side(state: PositionState, side: str) -> float:
    return state.shares_up if side == "UP" else state.shares_down


def _avg_for_side(state: PositionState, side: str) -> float:
    return state.avg_up() if side == "UP" else state.avg_down()


def _initial_failures_for_side(runner: WindowRunner, side: str) -> int:
    return runner.initial_failed_up if side == "UP" else runner.initial_failed_down


def _missing_position_side(state: PositionState) -> str | None:
    up = has_real_position(state, "UP")
    down = has_real_position(state, "DOWN")
    if up and not down:
        return "DOWN"
    if down and not up:
        return "UP"
    return None


def _projected_after_buy(state: PositionState, side: str, ask_px: float, amount_usd: float) -> tuple[float, float, float, float, float, float]:
    spent = state.spent_total() + amount_usd
    up_shares = state.shares_up + (amount_usd / max(ask_px, 1e-9) if side == "UP" else 0.0)
    down_shares = state.shares_down + (amount_usd / max(ask_px, 1e-9) if side == "DOWN" else 0.0)
    pnl_up = up_shares - spent
    pnl_down = down_shares - spent
    total_shares = up_shares + down_shares
    projected_share_gap = abs(up_shares - down_shares) / total_shares if total_shares > 1e-12 else 0.0
    projected_avg_up = (
        (state.spent_up + (amount_usd if side == "UP" else 0.0)) / up_shares
        if up_shares > 1e-12
        else 0.0
    )
    projected_avg_down = (
        (state.spent_down + (amount_usd if side == "DOWN" else 0.0)) / down_shares
        if down_shares > 1e-12
        else 0.0
    )
    return (
        pnl_up,
        pnl_down,
        min(pnl_up, pnl_down),
        abs(pnl_up - pnl_down),
        projected_share_gap,
        projected_avg_up + projected_avg_down,
    )


def _candidate_amounts(cfg: KngtopConfig) -> tuple[float, ...]:
    cap = max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))
    start_cents = int(round(MIN_ORDER_USD * 100))
    cap_cents = int(round(cap * 100))
    amounts = tuple(cents / 100.0 for cents in range(start_cents, cap_cents + 1))
    return amounts or (MIN_ORDER_USD,)


def _current_gap(state: PositionState) -> float:
    return abs(state.pnl_if_up() - state.pnl_if_down())


def _configured_max_shares_per_side(cfg: KngtopConfig) -> float:
    return max(1.0, float(getattr(cfg, "max_shares_per_side", MAX_SHARES_PER_SIDE)))


def _configured_max_share_gap(cfg: KngtopConfig) -> float:
    return max(0.0, float(getattr(cfg, "max_share_gap", MAX_SHARE_GAP)))


def _configured_repair_avg_sum_cap(cfg: KngtopConfig) -> float:
    return max(0.0, float(getattr(cfg, "repair_avg_sum_cap", REPAIR_AVG_SUM_CAP)))


def _configured_locked_profit_roi(cfg: KngtopConfig) -> float:
    return max(0.0, float(getattr(cfg, "locked_profit_roi", LOCKED_PROFIT_STOP_ROI)))


def _locked_profit_target_usd(cfg: KngtopConfig) -> float:
    return _configured_max_shares_per_side(cfg) * _configured_locked_profit_roi(cfg)


def _over_cap_side(state: PositionState, cfg: KngtopConfig) -> str | None:
    max_shares = _configured_max_shares_per_side(cfg)
    if state.shares_up > max_shares + 1e-12:
        return "UP"
    if state.shares_down > max_shares + 1e-12:
        return "DOWN"
    return None


def _share_room(state: PositionState, side: str, cfg: KngtopConfig) -> float:
    return max(0.0, _configured_max_shares_per_side(cfg) - _shares_for_side(state, side))


def _pending_reserved_shares_for_side(runner: WindowRunner, side: str) -> float:
    return runner.pending_reserved_shares if runner.pending_order and runner.pending_side == side else 0.0


def _effective_state_with_pending(runner: WindowRunner) -> PositionState:
    state = _copy_position_state(_sync_effective_positions(runner))
    if runner.pending_order and runner.pending_side in {"UP", "DOWN"} and runner.pending_reserved_shares > 1e-12:
        if runner.pending_side == "UP":
            state.shares_up += runner.pending_reserved_shares
            state.spent_up += runner.pending_amount_usd
        else:
            state.shares_down += runner.pending_reserved_shares
            state.spent_down += runner.pending_amount_usd
    return state


def _can_place_limit_buy(runner: WindowRunner, side: str, price: float, cfg: KngtopConfig) -> bool:
    if runner.pending_order:
        return False
    shares = _limit_order_shares(price)
    cost = shares * max(price, 0.0)
    if shares <= 0.0 or cost + 1e-12 < LIMIT_MIN_USD:
        return False
    state = _effective_state_with_pending(runner)
    if not _can_buy(state, side, cost, ask_px=price, cfg=cfg):
        return False
    confirmed = _confirmed_position_state(runner)
    required_side = _required_hedge_side(confirmed, cfg)
    if required_side is not None and side != required_side:
        return False
    if not state.both_sides_traded():
        return True
    if _limit_order_respects_share_gap(state, side, price, cfg):
        return True
    return _balance_required_side(state, cfg) == side and _balance_limit_improves_gap(state, side, price, cfg)


def _cap_amount_to_share_room(state: PositionState, side: str, ask_px: float, amount_usd: float, cfg: KngtopConfig) -> float | None:
    if ask_px <= 0.0:
        return None
    capped = min(float(amount_usd), _share_room(state, side, cfg) * float(ask_px))
    if capped + 1e-12 < MIN_ORDER_USD:
        return None
    return max(MIN_ORDER_USD, min(capped, float(amount_usd)))


def _share_gap_after_buy(state: PositionState, side: str, ask_px: float, amount_usd: float) -> float:
    shares = amount_usd / max(ask_px, 1e-9)
    up_shares = state.shares_up + (shares if side == "UP" else 0.0)
    down_shares = state.shares_down + (shares if side == "DOWN" else 0.0)
    return abs(up_shares - down_shares)


def _repair_buy_keeps_balance(
    state: PositionState,
    side: str,
    ask_px: float,
    amount_usd: float,
    cfg: KngtopConfig,
    preferred_larger_side: str | None,
) -> bool:
    if not state.both_sides_traded():
        return True
    current_side_shares = _shares_for_side(state, side)
    other_shares = _shares_for_side(state, _other_side(side))
    projected_side_shares = current_side_shares + amount_usd / max(ask_px, 1e-9)
    current_abs_share_gap = abs(current_side_shares - other_shares)
    projected_abs_share_gap = abs(projected_side_shares - other_shares)
    if (
        projected_abs_share_gap > _configured_max_share_gap(cfg) + 1e-12
        and projected_abs_share_gap >= current_abs_share_gap - 1e-12
    ):
        return False
    if side != preferred_larger_side and projected_side_shares > other_shares + 1e-12:
        return (
            current_side_shares < other_shares - 1e-12
            and projected_abs_share_gap <= _configured_max_share_gap(cfg) + 1e-12
        )
    return (
        side == preferred_larger_side
        or projected_abs_share_gap < current_abs_share_gap - 1e-12
        or current_side_shares <= other_shares + 1e-12
    )


def _repair_avg_sum_allowed(state: PositionState, projected_avg_sum: float, cfg: KngtopConfig) -> bool:
    cap = _configured_repair_avg_sum_cap(cfg)
    current_avg_sum = _avg_sum(state)
    if current_avg_sum > cap + 1e-12:
        return projected_avg_sum < current_avg_sum - 1e-12
    return projected_avg_sum <= cap + 1e-12


def _repair_price_improves_avg_sum(state: PositionState, side: str, ask_px: float) -> bool:
    if not state.both_sides_traded():
        return True
    current_side_avg = state.avg_up() if side == "UP" else state.avg_down()
    if current_side_avg <= 1e-12:
        return True
    if _shares_for_side(state, side) < _shares_for_side(state, _other_side(side)) - 1e-12:
        return ask_px < current_side_avg - 1e-12
    return ask_px < current_side_avg - REPAIR_PRICE_IMPROVEMENT_BUFFER + 1e-12


def _choose_order_amount_usd(
    *,
    side: str,
    ask_px: float,
    state: PositionState,
    cfg: KngtopConfig,
    enforce_repair_guards: bool = False,
    preferred_larger_side: str | None = None,
) -> float | None:
    current_worst_side = _weak_outcome_side(state)
    current_worst_side_pnl = state.pnl_if_up() if current_worst_side == "UP" else state.pnl_if_down()
    if current_worst_side is not None and current_worst_side_pnl < -1e-12 and side == current_worst_side:
        best_amount: float | None = None
        best_score: tuple[int, int, float, float, float, float] | None = None
        for amount in _candidate_amounts(cfg):
            if not _can_buy(state, side, amount, ask_px=ask_px, cfg=cfg):
                continue
            pnl_up, pnl_down, _projected_worst, _projected_gap, _projected_share_gap, projected_avg_sum = _projected_after_buy(
                state, side, ask_px, amount
            )
            if enforce_repair_guards:
                if not _repair_price_improves_avg_sum(state, side, ask_px):
                    continue
                if not _repair_avg_sum_allowed(state, projected_avg_sum, cfg):
                    continue
                if not _repair_buy_keeps_balance(state, side, ask_px, amount, cfg, preferred_larger_side):
                    continue
            projected_side_pnl = pnl_up if side == "UP" else pnl_down
            improvement = projected_side_pnl - current_worst_side_pnl
            if improvement <= 1e-12:
                continue
            both_profitable = int(pnl_up >= -1e-12 and pnl_down >= -1e-12)
            projected_abs_share_gap = _share_gap_after_buy(state, side, ask_px, amount)
            within_share_gap = int(projected_abs_share_gap <= _configured_max_share_gap(cfg) + 1e-12)
            score = (both_profitable, within_share_gap, -projected_abs_share_gap, improvement, -projected_avg_sum, -amount)
            if best_score is None or score > best_score:
                best_score = score
                best_amount = amount
        return best_amount

    best_amount: float | None = None
    best_score: tuple[int, int, float, float, float] | None = None
    for amount in _candidate_amounts(cfg):
        if not _can_buy(state, side, amount, ask_px=ask_px, cfg=cfg):
            continue
        pnl_up, pnl_down, _projected_worst, _projected_gap, projected_share_gap, projected_avg_sum = _projected_after_buy(
            state, side, ask_px, amount
        )
        projected_abs_share_gap = _share_gap_after_buy(state, side, ask_px, amount)
        if enforce_repair_guards:
            if not _repair_price_improves_avg_sum(state, side, ask_px):
                continue
            if not _repair_avg_sum_allowed(state, projected_avg_sum, cfg):
                continue
            if not _repair_buy_keeps_balance(state, side, ask_px, amount, cfg, preferred_larger_side):
                continue
            current_abs_share_gap = abs(state.shares_up - state.shares_down)
            if (
                projected_abs_share_gap > _configured_max_share_gap(cfg) + 1e-12
                and projected_abs_share_gap >= current_abs_share_gap - 1e-12
            ):
                continue
        both_profitable = int(pnl_up >= -1e-12 and pnl_down >= -1e-12)
        within_share_gap = int(projected_abs_share_gap <= _configured_max_share_gap(cfg) + 1e-12)
        score = (both_profitable, within_share_gap, -projected_abs_share_gap, -projected_avg_sum, -amount)
        if best_score is None or score > best_score:
            best_score = score
            best_amount = amount
    return best_amount


def _repair_candidate_skip_reason(
    *,
    side: str,
    ask_px: float,
    state: PositionState,
    cfg: KngtopConfig,
    preferred_larger_side: str | None = None,
) -> str:
    saw_buyable = False
    saw_price_pass = False
    saw_avg_sum_pass = False
    saw_share_gap_pass = False
    for amount in _candidate_amounts(cfg):
        if not _can_buy(state, side, amount, ask_px=ask_px, cfg=cfg):
            continue
        saw_buyable = True
        if not _repair_price_improves_avg_sum(state, side, ask_px):
            continue
        saw_price_pass = True
        _pnl_up, _pnl_down, _projected_worst, _projected_gap, _projected_share_gap, projected_avg_sum = _projected_after_buy(
            state, side, ask_px, amount
        )
        del _pnl_up, _pnl_down, _projected_worst, _projected_gap, _projected_share_gap
        if _repair_avg_sum_allowed(state, projected_avg_sum, cfg):
            saw_avg_sum_pass = True
        if not _repair_buy_keeps_balance(state, side, ask_px, amount, cfg, preferred_larger_side):
            continue
        current_abs_share_gap = abs(state.shares_up - state.shares_down)
        projected_abs_share_gap = _share_gap_after_buy(state, side, ask_px, amount)
        if (
            projected_abs_share_gap <= _configured_max_share_gap(cfg) + 1e-12
            or projected_abs_share_gap < current_abs_share_gap - 1e-12
        ):
            saw_share_gap_pass = True
    if not saw_buyable:
        return "no_valid_amount"
    if not saw_price_pass:
        return "price_not_tighter"
    if not saw_avg_sum_pass:
        return "avg_sum_guard"
    if not saw_share_gap_pass:
        return "repair_overshoot_guard"
    return "repair_candidate_guard"


def _bootstrap_amount_usd(cfg: KngtopConfig) -> float:
    return max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))


def _limit_order_shares(price: float) -> float:
    if price <= 0.0:
        return 0.0
    return max(LIMIT_MIN_SHARES, LIMIT_MIN_USD / max(price, 1e-9))


def _passive_limit_buy_price(ask_px: float) -> float:
    return max(0.01, min(MAX_ORDER_PRICE, float(ask_px) - LIMIT_PASSIVE_OFFSET))


def _limit_order_cost(price: float) -> float:
    return _limit_order_shares(price) * max(price, 0.0)


def _abs_share_gap(state: PositionState) -> float:
    return abs(state.shares_up - state.shares_down)


def _balance_required_side(state: PositionState, cfg: KngtopConfig) -> str | None:
    if not state.both_sides_traded():
        return None
    max_gap = _configured_max_share_gap(cfg)
    gap = state.shares_up - state.shares_down
    if gap > max_gap + 1e-12:
        return "DOWN"
    if gap < -max_gap - 1e-12:
        return "UP"
    return None


def _confirmed_position_state(runner: WindowRunner) -> PositionState:
    return _sync_effective_positions(runner)


def _required_hedge_side(state: PositionState, cfg: KngtopConfig) -> str | None:
    missing = _missing_position_side(state)
    if missing is not None:
        return missing
    return _balance_required_side(state, cfg)


def _projected_abs_share_gap_after_limit(state: PositionState, side: str, price: float) -> float:
    shares = _limit_order_shares(price)
    up_shares = state.shares_up + (shares if side == "UP" else 0.0)
    down_shares = state.shares_down + (shares if side == "DOWN" else 0.0)
    return abs(up_shares - down_shares)


def _limit_order_respects_share_gap(state: PositionState, side: str, price: float, cfg: KngtopConfig) -> bool:
    return _projected_abs_share_gap_after_limit(state, side, price) <= _configured_max_share_gap(cfg) + 1e-12


def _balance_limit_improves_gap(state: PositionState, side: str, price: float, cfg: KngtopConfig) -> bool:
    current_gap = _abs_share_gap(state)
    projected_gap = _projected_abs_share_gap_after_limit(state, side, price)
    if projected_gap <= _configured_max_share_gap(cfg) + 1e-12:
        return True
    return projected_gap < current_gap - 1e-12


def _hedge_limit_repair_action(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    cfg: KngtopConfig,
) -> BuyAction | None:
    confirmed = _confirmed_position_state(runner)
    state = _effective_state_with_pending(runner)
    side = _required_hedge_side(confirmed, cfg)
    if side is None:
        return None
    quoted_ask = up_ask if side == "UP" else down_ask
    target_price = _passive_limit_buy_price(quoted_ask)
    if target_price <= 0.0:
        return None
    amount_usd = _limit_order_cost(target_price)
    if not _can_buy(state, side, amount_usd, ask_px=target_price, cfg=cfg):
        return None
    if _missing_position_side(confirmed) is None and not _balance_limit_improves_gap(state, side, target_price, cfg):
        return None
    reason = "missing_side_limit" if _missing_position_side(confirmed) is not None else "balance_limit_repair"
    return BuyAction(
        side=side,
        ask_px=target_price,
        amount_usd=amount_usd,
        reason=reason,
        enforce_avg_cap=False,
    )


def _balance_limit_repair_action(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    cfg: KngtopConfig,
) -> BuyAction | None:
    return _hedge_limit_repair_action(runner, up_ask=up_ask, down_ask=down_ask, cfg=cfg)


def _can_buy(state: PositionState, side: str, amount_usd: float, *, ask_px: float, cfg: KngtopConfig) -> bool:
    if amount_usd + 1e-12 < MIN_ORDER_USD:
        return False
    if ask_px <= 0.0:
        return False
    if state.spent_total() + amount_usd > 20.0 + 1e-12:
        return False
    if _shares_for_side(state, side) + (amount_usd / ask_px) > _configured_max_shares_per_side(cfg) + 1e-12:
        return False
    return True


def _avg_sum(state: PositionState) -> float:
    return state.avg_up() + state.avg_down()


def _avg_after_buy(state: PositionState, side: str, ask_px: float, amount_usd: float) -> float:
    shares = amount_usd / max(ask_px, 1e-9)
    if side == "UP":
        new_avg_up = (state.spent_up + amount_usd) / (state.shares_up + shares)
        return new_avg_up + state.avg_down()
    new_avg_down = (state.spent_down + amount_usd) / (state.shares_down + shares)
    return state.avg_up() + new_avg_down


def _apply_position_fill(state: PositionState, side: str, ask_px: float, amount_usd: float, filled_shares: float | None = None) -> None:
    shares = amount_usd / max(ask_px, 1e-9) if filled_shares is None else max(0.0, float(filled_shares))
    spent = min(amount_usd, shares * max(ask_px, 1e-9))
    if side == "UP":
        state.spent_up += spent
        state.shares_up += shares
        state.orders_up += 1
    else:
        state.spent_down += spent
        state.shares_down += shares
        state.orders_down += 1
    state.total_deals += 1


def _record_local_fill(
    runner: WindowRunner,
    side: str,
    ask_px: float,
    amount_usd: float,
    filled_shares: float | None = None,
    *,
    ensure_floor: bool = True,
) -> None:
    if ensure_floor:
        _ensure_local_position_floor(runner)
    before_orders = runner.local_positions.total_deals
    _apply_position_fill(runner.local_positions, side, ask_px, amount_usd, filled_shares)
    if before_orders == runner.local_positions.total_deals:
        runner.local_positions.total_deals = runner.local_positions.orders_up + runner.local_positions.orders_down
    _sync_effective_positions(runner)


def _refresh_positions_from_pm(
    runner: WindowRunner,
    *,
    cfg: KngtopConfig,
) -> PositionState:
    _ensure_local_position_floor(runner)
    prev_effective = _sync_effective_positions(runner)
    prev_shares_up = prev_effective.shares_up
    prev_shares_down = prev_effective.shares_down
    prev_spent_up = prev_effective.spent_up
    prev_spent_down = prev_effective.spent_down
    prev_orders_up = prev_effective.orders_up
    prev_orders_down = prev_effective.orders_down
    rows = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
    token_by_side = {"UP": runner.contract.up.token_id, "DOWN": runner.contract.down.token_id}
    shares_by_side = {"UP": 0.0, "DOWN": 0.0}
    cost_by_side = {"UP": 0.0, "DOWN": 0.0}
    fallback_cost_by_side = {"UP": False, "DOWN": False}
    prev_shares_by_side = {"UP": prev_shares_up, "DOWN": prev_shares_down}
    prev_cost_by_side = {"UP": prev_spent_up, "DOWN": prev_spent_down}
    for row in rows:
        slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        asset_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
        outcome = str(row.get("outcome") or "").strip().upper()
        side: str | None = None
        if slug and slug == runner.contract.slug and outcome in {"UP", "DOWN"}:
            side = outcome
        else:
            for candidate_side, token_id in token_by_side.items():
                if asset_id and asset_id == token_id:
                    side = candidate_side
                    break
        if side is None:
            continue
        size = _extract_numeric(row, "size", "amount", "shares")
        avg_price = _extract_numeric(row, "avgPrice", "averagePrice", "avg_price", "price")
        if size is None or size <= 0:
            continue
        size_f = float(size)
        shares_by_side[side] += size_f
        if avg_price is not None and 0.0 < float(avg_price) < 1.0:
            cost_by_side[side] += size_f * float(avg_price)
            continue
        prev_shares = prev_shares_by_side[side]
        prev_cost = prev_cost_by_side[side]
        prev_avg = prev_cost / prev_shares if prev_shares > 1e-12 and prev_cost > 1e-12 else 0.0
        carried_shares = min(size_f, prev_shares)
        added_shares = max(0.0, size_f - carried_shares)
        if prev_avg > 1e-12:
            cost_by_side[side] += carried_shares * prev_avg
        if runner.pending_side == side and runner.pending_ask_px > 1e-12:
            cost_by_side[side] += added_shares * runner.pending_ask_px
            fallback_cost_by_side[side] = True
        elif prev_avg > 1e-12:
            cost_by_side[side] += added_shares * prev_avg
            fallback_cost_by_side[side] = True

    for side, used_fallback in fallback_cost_by_side.items():
        if used_fallback:
            _log_tag(
                "AVG_PRICE FALLBACK",
                slug=runner.contract.slug,
                side=side,
                shares=f"{shares_by_side[side]:.6f}",
                pending_ask=f"{runner.pending_ask_px:.4f}" if runner.pending_side == side else None,
                cost=f"{cost_by_side[side]:.4f}",
            )

    del prev_shares_up, prev_shares_down, prev_spent_up, prev_spent_down
    orders_up = prev_orders_up
    orders_down = prev_orders_down
    if shares_by_side["UP"] > 1e-6 and orders_up == 0:
        orders_up = 1
    if shares_by_side["DOWN"] > 1e-6 and orders_down == 0:
        orders_down = 1

    refreshed = PositionState(
        spent_up=cost_by_side["UP"],
        spent_down=cost_by_side["DOWN"],
        shares_up=shares_by_side["UP"],
        shares_down=shares_by_side["DOWN"],
        orders_up=orders_up,
        orders_down=orders_down,
        total_deals=orders_up + orders_down,
    )
    runner.api_positions = refreshed
    return _sync_effective_positions(runner)


def _confirm_fill_from_pm(
    runner: WindowRunner,
    *,
    side: str,
    pre_state: PositionState,
    cfg: KngtopConfig,
) -> bool:
    deadline = time.monotonic() + POST_ORDER_RECONCILE_TIMEOUT_SEC
    while time.monotonic() <= deadline:
        refreshed = _refresh_positions_from_pm(runner, cfg=cfg)
        prev_shares = pre_state.shares_up if side == "UP" else pre_state.shares_down
        new_shares = refreshed.shares_up if side == "UP" else refreshed.shares_down
        if new_shares > prev_shares + 1e-6:
            return True
        time.sleep(POST_ORDER_RECONCILE_POLL_SEC)
    return False


def _reconcile_pending_limit_order(
    runner: WindowRunner,
    *,
    cfg: KngtopConfig,
    clob: KngtopClob | None,
) -> bool:
    if not runner.pending_order or runner.pending_side not in {"UP", "DOWN"}:
        return False
    pre_state = _copy_position_state(runner.positions)
    if not cfg.dry_run:
        try:
            refreshed = _refresh_positions_from_pm(runner, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            _log_tag("LIMIT_RECONCILE", slug=runner.contract.slug, status="error", error=str(exc))
            return True
        side = runner.pending_side
        if _shares_for_side(refreshed, side) > _shares_for_side(pre_state, side) + 1e-6:
            filled_delta = _shares_for_side(refreshed, side) - _shares_for_side(pre_state, side)
            fill_price = runner.pending_ask_px if runner.pending_ask_px > 1e-12 else 0.0
            if filled_delta > 1e-12 and fill_price > 1e-12:
                _record_local_fill(
                    runner,
                    side,
                    fill_price,
                    filled_delta * fill_price,
                    filled_delta,
                    ensure_floor=False,
                )
            runner.last_successful_buy_ts = runner.pending_created_ts
            runner.last_position_refresh_ts = runner.pending_created_ts
            filled_order_id = runner.pending_order_id
            _clear_pending_limit_state(runner)
            runner.execution_state = WAIT_NEXT_DECISION
            runner.next_decision_ts = datetime.now(timezone.utc).timestamp()
            _drop_tracked_open_order(clob, filled_order_id)
            _log_tag("LIMIT FILLED", slug=runner.contract.slug, side=side)
            return False
        if clob is not None:
            still_open = _fetch_window_open_buy_orders(runner, clob)
            if still_open:
                _adopt_tracked_limit_order(runner, still_open[0], reason="reconcile_open_order")
                return True
            if runner.pending_order_id:
                try:
                    token = _token_for_side(runner, side)
                    if clob.is_order_open_for_asset(token, runner.pending_order_id):
                        return True
                except Exception as exc:  # noqa: BLE001
                    _log_tag("LIMIT_RECONCILE", slug=runner.contract.slug, status="open_check_error", error=str(exc))
                    return True
            _clear_pending_limit_state(runner)
            runner.execution_state = READY
            _log_tag("LIMIT CLOSED_UNFILLED", slug=runner.contract.slug, side=side)
            return False
    return True


def _next_retry_delay(reason: str) -> float:
    if reason in {"initial_lower_ask", "cheap_weak_repair", "guarded_high_repair"}:
        return 1.0
    return float(ACTIVE_REPAIR_INTERVAL_SEC)


def _schedule_next_decision(runner: WindowRunner, *, now_ts: float, reason: str) -> None:
    runner.execution_state = WAIT_NEXT_DECISION
    runner.next_decision_ts = now_ts + _next_retry_delay(reason)


def _is_initial_reason(reason: str) -> bool:
    return reason in {"initial_lower_ask", "initial_retry"}


def _is_balance_or_allowance_error(error: object) -> bool:
    text = str(error).lower()
    return "not enough balance" in text or "not enough allowance" in text or "allowance" in text


def _record_initial_intent(runner: WindowRunner, *, side: str, ask_px: float, elapsed: float) -> None:
    runner.initial_intent_attempted = True
    runner.last_initial_attempt_side = side
    runner.last_initial_attempt_price = ask_px
    runner.last_initial_attempt_elapsed = elapsed


def _record_initial_failure(runner: WindowRunner, *, side: str) -> None:
    if side == "UP":
        runner.initial_failed_up += 1
    else:
        runner.initial_failed_down += 1


def _initial_retry_allowed(runner: WindowRunner, *, side: str, ask_px: float, elapsed: float) -> bool:
    if runner.last_initial_attempt_side != side:
        return True
    if ask_px <= runner.last_initial_attempt_price - INITIAL_RETRY_PRICE_IMPROVEMENT + 1e-12:
        return True
    return elapsed - runner.last_initial_attempt_elapsed >= INITIAL_RETRY_WAIT_SEC - 1e-12


def _record_order_intent(runner: WindowRunner, *, side: str) -> None:
    if side == "UP":
        runner.intent_count_up += 1
    else:
        runner.intent_count_down += 1


def _record_confirmed_order(runner: WindowRunner, *, side: str) -> None:
    if side == "UP":
        runner.local_positions.orders_up += 1
    else:
        runner.local_positions.orders_down += 1
    runner.local_positions.total_deals = runner.local_positions.orders_up + runner.local_positions.orders_down
    _sync_effective_positions(runner)


def _locked_profit_stop_reached(state: PositionState, cfg: KngtopConfig) -> bool:
    profit_target = _locked_profit_target_usd(cfg)
    return (
        state.both_sides_traded()
        and state.pnl_if_up() >= profit_target - 1e-12
        and state.pnl_if_down() >= profit_target - 1e-12
        and _avg_sum(state) <= LOCKED_PROFIT_STOP_AVG_SUM + 1e-12
        and state.share_imbalance() <= LOCKED_PROFIT_STOP_IMBALANCE + 1e-12
        and abs(state.shares_up - state.shares_down) <= _configured_max_share_gap(cfg) + 1e-12
    )


def _log_current_pnl_state(runner: WindowRunner) -> None:
    state = _sync_effective_positions(runner)
    _log_tag(
        "PNL_STATE",
        slug=runner.contract.slug,
        spent=f"{state.spent_total():.4f}",
        up_shares=f"{state.shares_up:.6f}",
        down_shares=f"{state.shares_down:.6f}",
        up_orders=str(state.orders_up),
        down_orders=str(state.orders_down),
        pnl_if_up=f"{state.pnl_if_up():.4f}",
        pnl_if_down=f"{state.pnl_if_down():.4f}",
        gap=f"{_current_gap(state):.4f}",
        weak_side=_weak_outcome_side(state) or "TIE",
    )


def _log_pnl_projection(runner: WindowRunner, *, side: str, ask_px: float, amount_usd: float) -> None:
    _sync_effective_positions(runner)
    proj_up, proj_down, proj_worst, proj_gap, proj_share_gap, proj_avg_sum = _projected_after_buy(
        runner.positions, side, ask_px, amount_usd
    )
    shares = amount_usd / max(ask_px, 1e-9)
    projected_up_shares = runner.positions.shares_up + (shares if side == "UP" else 0.0)
    projected_down_shares = runner.positions.shares_down + (shares if side == "DOWN" else 0.0)
    _log_tag(
        "PNL_PROJECTION",
        slug=runner.contract.slug,
        side=side,
        ask=f"{ask_px:.4f}",
        amount=f"{amount_usd:.2f}",
        projected_pnl_if_up=f"{proj_up:.4f}",
        projected_pnl_if_down=f"{proj_down:.4f}",
        projected_worst=f"{proj_worst:.4f}",
        projected_gap=f"{proj_gap:.4f}",
        projected_share_gap=f"{proj_share_gap:.4f}",
        projected_abs_share_gap=f"{abs(projected_up_shares - projected_down_shares):.4f}",
        projected_up_shares=f"{projected_up_shares:.6f}",
        projected_down_shares=f"{projected_down_shares:.6f}",
        projected_avg_sum=f"{proj_avg_sum:.4f}",
    )


def _send_limit_buy(
    *,
    runner: WindowRunner,
    side: str,
    price: float,
    reason: str,
    elapsed: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> bool:
    if price <= 0.0 or price > min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price)) + 1e-12:
        return False
    pre_state = _copy_position_state(runner.positions)
    shares = _limit_order_shares(price)
    amount_usd = shares * price
    if not _can_place_limit_buy(runner, side, price, cfg):
        _log_tag(
            "LIMIT BLOCK",
            slug=runner.contract.slug,
            side=side,
            reason=reason,
            price=f"{price:.4f}",
            shares=f"{shares:.6f}",
            amount=f"{amount_usd:.2f}",
        )
        return False
    if not cfg.dry_run and clob is not None and _sync_and_enforce_single_open_limit(runner, clob=clob, cfg=cfg):
        _log_tag("LIMIT BLOCK", slug=runner.contract.slug, side=side, reason="active_open_order_exists")
        return False

    _log_current_pnl_state(runner)
    _log_pnl_projection(runner, side=side, ask_px=price, amount_usd=amount_usd)
    _record_order_intent(runner, side=side)
    if _is_initial_reason(reason) and runner.positions.total_deals == 0:
        _record_initial_intent(runner, side=side, ask_px=price, elapsed=elapsed)
    runner.pending_order = True
    runner.execution_state = ORDER_IN_FLIGHT
    runner.pending_side = side
    runner.pending_reason = reason
    runner.pending_ask_px = price
    runner.pending_amount_usd = amount_usd
    runner.pending_reserved_shares = shares
    runner.pending_created_ts = elapsed
    _log_tag(
        "LIMIT SENT",
        slug=runner.contract.slug,
        side=side,
        reason=reason,
        price=f"{price:.4f}",
        shares=f"{shares:.6f}",
        amount=f"{amount_usd:.2f}",
    )

    if cfg.dry_run or clob is None:
        _record_local_fill(runner, side, price, amount_usd, shares)
        runner.pending_order = False
        runner.pending_order_id = None
        runner.pending_side = None
        runner.pending_reason = None
        runner.pending_ask_px = 0.0
        runner.pending_amount_usd = 0.0
        runner.pending_reserved_shares = 0.0
        runner.pending_created_ts = 0.0
        if _is_initial_reason(reason):
            runner.initial_filled = True
        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
        return True

    try:
        token = _token_for_side(runner, side)
        payload = clob.limit_buy_shares(token, price=price, shares=shares, post_only=True)
        runner.pending_order_id = _extract_order_id(payload)
        if _confirm_fill_from_pm(runner, side=side, pre_state=pre_state, cfg=cfg):
            order_id = runner.pending_order_id
            filled_delta = max(0.0, _shares_for_side(runner.positions, side) - _shares_for_side(pre_state, side))
            _record_local_fill(runner, side, price, filled_delta * price, filled_delta, ensure_floor=False)
            _drop_tracked_open_order(clob, order_id)
            _clear_pending_limit_state(runner)
            if _is_initial_reason(reason):
                runner.initial_filled = True
            _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
            _log_tag("LIMIT FILLED_IMMEDIATE", slug=runner.contract.slug, side=side, order_id=order_id)
            return True
        runner.execution_state = ORDER_IN_FLIGHT
        _log_tag("LIMIT POSTED", slug=runner.contract.slug, side=side, order_id=runner.pending_order_id)
        return True
    except Exception as exc:  # noqa: BLE001
        _clear_pending_limit_state(runner)
        runner.execution_state = READY
        if _is_balance_or_allowance_error(exc):
            runner.stop_reason = "balance_or_allowance"
            _log_tag("STOP", slug=runner.contract.slug, side=side, reason="balance_or_allowance", error=str(exc))
            return False
        if "invalid post-only order" in str(exc).lower() or "crosses book" in str(exc).lower():
            _log_tag("LIMIT CROSSED", slug=runner.contract.slug, side=side, reason=reason, error=str(exc))
            _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
            return False
        if _is_initial_reason(reason) and runner.positions.total_deals == 0:
            _record_initial_failure(runner, side=side)
        _log_tag("LIMIT FAILED", slug=runner.contract.slug, side=side, reason=reason, error=str(exc))
        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
        return False


def _send_fak_buy(
    *,
    runner: WindowRunner,
    side: str,
    ask_px: float,
    amount_usd: float,
    reason: str,
    elapsed: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    enforce_avg_cap: bool = True,
) -> bool:
    _sync_effective_positions(runner)
    pre_state = PositionState(
        spent_up=runner.positions.spent_up,
        spent_down=runner.positions.spent_down,
        shares_up=runner.positions.shares_up,
        shares_down=runner.positions.shares_down,
        orders_up=runner.positions.orders_up,
        orders_down=runner.positions.orders_down,
        total_deals=runner.positions.total_deals,
    )
    if ask_px <= 0.0 or ask_px > min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price)) + 1e-12:
        return False
    risk_state = _effective_state_with_pending(runner)
    if not _can_buy(risk_state, side, amount_usd, ask_px=ask_px, cfg=cfg):
        _log_tag(
            "BUDGET BLOCK",
            slug=runner.contract.slug,
            side=side,
            reason=reason,
            amount=f"{amount_usd:.2f}",
            spent_total=f"{runner.positions.spent_total():.2f}",
            side_shares=f"{_shares_for_side(runner.positions, side):.6f}",
            max_side_shares=f"{_configured_max_shares_per_side(cfg):.6f}",
            deals=str(runner.positions.total_deals),
        )
        return False
    after_avg_sum = _avg_after_buy(runner.positions, side, ask_px, amount_usd)
    if enforce_avg_cap and after_avg_sum > AVG_SUM_CAP + 1e-12:
        _log_tag(
            "AVG_SUM BLOCK",
            slug=runner.contract.slug,
            side=side,
            reason=reason,
            ask=f"{ask_px:.4f}",
            amount=f"{amount_usd:.2f}",
            post_avg_sum=f"{after_avg_sum:.4f}",
        )
        return False

    _log_current_pnl_state(runner)
    _log_pnl_projection(runner, side=side, ask_px=ask_px, amount_usd=amount_usd)
    _log_tag("INTENT CREATED", slug=runner.contract.slug, side=side, reason=reason, ask=f"{ask_px:.4f}", amount=f"{amount_usd:.2f}")
    _record_order_intent(runner, side=side)
    if _is_initial_reason(reason) and runner.positions.total_deals == 0:
        _record_initial_intent(runner, side=side, ask_px=ask_px, elapsed=elapsed)
    runner.pending_order = True
    runner.execution_state = ORDER_IN_FLIGHT
    runner.pending_side = side
    runner.pending_reason = reason
    runner.pending_ask_px = ask_px
    runner.pending_amount_usd = amount_usd
    runner.pending_reserved_shares = amount_usd / max(ask_px, 1e-9)
    runner.pending_created_ts = elapsed
    _log_tag("ORDER SENT", slug=runner.contract.slug, side=side, reason=reason, ask=f"{ask_px:.4f}", amount=f"{amount_usd:.2f}")

    try:
        token = _token_for_side(runner, side)
        confirmed_by_reconcile = False
        response_filled_shares = 0.0
        if not cfg.dry_run and clob is not None:
            attempts = max(1, int(cfg.order_retry_on_error) + 1)
            last_error: Exception | None = None
            for _attempt in range(1, attempts + 1):
                try:
                    payload = clob.market_buy_usdc(token, amount_usd, max_price=min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price), ask_px))
                    filled_shares = _extract_filled_shares(payload, fallback_amount_usd=amount_usd, fallback_price=ask_px)
                    response_filled_shares = max(response_filled_shares, filled_shares)
                    if filled_shares <= 0.000001:
                        if _confirm_fill_from_pm(runner, side=side, pre_state=pre_state, cfg=cfg):
                            confirmed_by_reconcile = True
                            _log_tag(
                                "ORDER CONFIRMED_AFTER_NOFILL",
                                slug=runner.contract.slug,
                                side=side,
                                reason=reason,
                                ask=f"{ask_px:.4f}",
                                amount=f"{amount_usd:.2f}",
                            )
                            break
                        _log_tag(
                            "ORDER NOFILL",
                            slug=runner.contract.slug,
                            side=side,
                            reason=reason,
                            ask=f"{ask_px:.4f}",
                            amount=f"{amount_usd:.2f}",
                        )
                        if _is_initial_reason(reason) and runner.positions.total_deals == 0:
                            _record_initial_failure(runner, side=side)
                        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
                        return False
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if "no orders found to match with FAK order" in str(exc):
                        if _confirm_fill_from_pm(runner, side=side, pre_state=pre_state, cfg=cfg):
                            confirmed_by_reconcile = True
                            _log_tag(
                                "ORDER CONFIRMED_AFTER_ERROR",
                                slug=runner.contract.slug,
                                side=side,
                                reason=reason,
                                ask=f"{ask_px:.4f}",
                                amount=f"{amount_usd:.2f}",
                                error=str(exc),
                            )
                            break
                        _log_tag(
                            "ORDER FAILED",
                            slug=runner.contract.slug,
                            side=side,
                            reason=reason,
                            ask=f"{ask_px:.4f}",
                            amount=f"{amount_usd:.2f}",
                            error=str(exc),
                        )
                        if _is_initial_reason(reason) and runner.positions.total_deals == 0:
                            _record_initial_failure(runner, side=side)
                        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
                        return False
                    if _is_balance_or_allowance_error(exc):
                        runner.stop_reason = "balance_or_allowance"
                        _log_tag(
                            "STOP",
                            slug=runner.contract.slug,
                            side=side,
                            reason="balance_or_allowance",
                            error=str(exc),
                        )
                        return False
                    time.sleep(0.5)
            else:
                if last_error is not None:
                    _log_tag(
                        "ORDER FAILED",
                        slug=runner.contract.slug,
                        side=side,
                        reason=reason,
                        ask=f"{ask_px:.4f}",
                        amount=f"{amount_usd:.2f}",
                        error=str(last_error),
                    )
                    if _is_initial_reason(reason) and runner.positions.total_deals == 0:
                        _record_initial_failure(runner, side=side)
                    _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
                return False

        if cfg.dry_run or clob is None:
            _record_local_fill(runner, side, ask_px, amount_usd)
            fill_confirmed = True
        else:
            fill_confirmed = response_filled_shares > 0.000001 or confirmed_by_reconcile or _confirm_fill_from_pm(runner, side=side, pre_state=pre_state, cfg=cfg)
        if not fill_confirmed:
            _log_tag(
                "ORDER UNCONFIRMED",
                slug=runner.contract.slug,
                side=side,
                reason=reason,
                ask=f"{ask_px:.4f}",
                amount=f"{amount_usd:.2f}",
            )
            if _is_initial_reason(reason) and runner.positions.total_deals == 0:
                _record_initial_failure(runner, side=side)
            _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
            return False
        if not (cfg.dry_run or clob is None):
            refreshed_side_shares = _shares_for_side(runner.positions, side)
            filled_delta = max(0.0, refreshed_side_shares - _shares_for_side(pre_state, side))
            filled_for_local = response_filled_shares if response_filled_shares > 0.000001 else (filled_delta if filled_delta > 0.000001 else None)
            _record_local_fill(runner, side, ask_px, amount_usd, filled_for_local, ensure_floor=False)
        if _is_initial_reason(reason):
            runner.initial_filled = True
        runner.last_successful_buy_ts = elapsed
        runner.last_position_refresh_ts = elapsed
        _log_tag(
            "ORDER FILLED",
            slug=runner.contract.slug,
            side=side,
            reason=reason,
            ask_px=f"{ask_px:.4f}",
            amount_usd=f"{amount_usd:.2f}",
            orders=str(runner.positions.total_deals),
            avg_sum=f"{_avg_sum(runner.positions):.4f}",
            imbalance=f"{runner.positions.share_imbalance():.4f}",
        )
        _log_tag(
            "POSITION UPDATED",
            slug=runner.contract.slug,
            spent_total=f"{runner.positions.spent_total():.2f}",
            up_shares=f"{runner.positions.shares_up:.6f}",
            down_shares=f"{runner.positions.shares_down:.6f}",
            avg_sum=f"{_avg_sum(runner.positions):.4f}",
        )
        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
        return True
    finally:
        runner.pending_order = False
        runner.pending_side = None
        runner.pending_reason = None
        runner.pending_ask_px = 0.0
        runner.pending_amount_usd = 0.0
        runner.pending_reserved_shares = 0.0
        runner.pending_created_ts = 0.0


def _high_repair_allowed(
    state: PositionState,
    *,
    side: str,
    ask_px: float,
    amount_usd: float,
    elapsed: float,
    remaining: float,
) -> bool:
    if ask_px <= HIGH_REPAIR_GUARD + 1e-12:
        return True
    if ask_px > HIGH_REPAIR_PRE240_CAP + 1e-12 and elapsed < 240.0:
        return False
    if ask_px > FINAL_60_DANGER_CAP + 1e-12:
        return False
    _proj_up, _proj_down, projected_worst, _proj_gap, projected_share_gap, _proj_avg_sum = _projected_after_buy(
        state, side, ask_px, amount_usd
    )
    dangerously_weak = state.pnl_if_up() < DANGEROUS_WEAK_PNL if side == "UP" else state.pnl_if_down() < DANGEROUS_WEAK_PNL
    if remaining <= 60.0 and ask_px <= FINAL_60_DANGER_CAP + 1e-12 and dangerously_weak:
        return True
    return projected_worst >= HIGH_REPAIR_WORST_TARGET - 1e-12 or projected_share_gap <= HIGH_REPAIR_SHARE_GAP_TARGET + 1e-12


def _choose_initial_buy(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    cfg: KngtopConfig,
) -> BuyAction | None:
    ordered = (("UP", up_ask), ("DOWN", down_ask))
    ordered = tuple(sorted(ordered, key=lambda item: item[1]))
    available_sides = 0
    for side, ask_px in ordered:
        if ask_px > BOOTSTRAP_CHEAP_CAP + 1e-12:
            continue
        if _initial_failures_for_side(runner, side) >= MAX_INITIAL_RETRIES_PER_SIDE:
            continue
        available_sides += 1
        if not runner.initial_intent_attempted:
            amount = _cap_amount_to_share_room(runner.positions, side, ask_px, _bootstrap_amount_usd(cfg), cfg)
            if amount is None:
                continue
            return BuyAction(
                side=side,
                ask_px=ask_px,
                amount_usd=amount,
                reason="initial_lower_ask",
                enforce_avg_cap=False,
            )
        if not _initial_retry_allowed(runner, side=side, ask_px=ask_px, elapsed=elapsed):
            continue
        amount = _cap_amount_to_share_room(runner.positions, side, ask_px, MIN_ORDER_USD, cfg)
        if amount is None:
            continue
        return BuyAction(
            side=side,
            ask_px=ask_px,
            amount_usd=amount,
            reason="initial_retry",
            enforce_avg_cap=False,
        )
    if (
        runner.initial_intent_attempted
        and runner.positions.total_deals == 0
        and runner.initial_failed_up >= MAX_INITIAL_RETRIES_PER_SIDE
        and runner.initial_failed_down >= MAX_INITIAL_RETRIES_PER_SIDE
    ):
        runner.stop_reason = "bootstrap_exhausted"
        _log_tag(
            "STOP",
            slug=runner.contract.slug,
            reason="bootstrap_exhausted",
            up_failures=str(runner.initial_failed_up),
            down_failures=str(runner.initial_failed_down),
            up_intents=str(runner.intent_count_up),
            down_intents=str(runner.intent_count_down),
        )
    return None


def _choose_guarded_pnl_buy(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    remaining: float,
    cfg: KngtopConfig,
    current_winning_side: str | None = None,
) -> BuyAction | None:
    state = _effective_state_with_pending(runner)
    confirmed = _confirmed_position_state(runner)
    synced = _sync_effective_positions(runner)
    if (
        synced.total_deals == 0
        and not runner.pending_order
        and not has_real_position(confirmed, "UP")
        and not has_real_position(confirmed, "DOWN")
    ):
        return _choose_initial_buy(runner, up_ask=up_ask, down_ask=down_ask, elapsed=elapsed, cfg=cfg)
    over_cap_side = _over_cap_side(state, cfg)
    if over_cap_side is not None:
        runner.stop_reason = "over_cap_position"
        _log_tag(
            "STOP",
            slug=runner.contract.slug,
            reason="over_cap_position",
            side=over_cap_side,
            up_shares=f"{state.shares_up:.6f}",
            down_shares=f"{state.shares_down:.6f}",
            max_side_shares=f"{_configured_max_shares_per_side(cfg):.6f}",
        )
        return None
    if _locked_profit_stop_reached(state, cfg):
        runner.stop_reason = "locked_profit"
        _log_tag(
            "STOP",
            slug=runner.contract.slug,
            reason="locked_profit",
            pnl_if_up=f"{state.pnl_if_up():.4f}",
            pnl_if_down=f"{state.pnl_if_down():.4f}",
            profit_target=f"{_locked_profit_target_usd(cfg):.4f}",
            avg_sum=f"{_avg_sum(state):.4f}",
            imbalance=f"{state.share_imbalance():.4f}",
        )
        return None

    required_side = _required_hedge_side(confirmed, cfg)
    if required_side is not None:
        if (hedge_action := _hedge_limit_repair_action(runner, up_ask=up_ask, down_ask=down_ask, cfg=cfg)) is not None:
            return hedge_action
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=required_side,
            reason="hedge_side_required",
            required_side=required_side,
            shares_up=f"{confirmed.shares_up:.6f}",
            shares_down=f"{confirmed.shares_down:.6f}",
            abs_share_gap=f"{_abs_share_gap(state):.4f}",
            max_share_gap=f"{_configured_max_share_gap(cfg):.4f}",
        )
        return None

    missing_side = _missing_position_side(state)
    if missing_side is not None:
        side = missing_side
    elif state.both_sides_traded() and _weak_outcome_side(state) is not None and (
        (state.pnl_if_up() if _weak_outcome_side(state) == "UP" else state.pnl_if_down()) < -1e-12
    ):
        side = _weak_outcome_side(state)
    elif (
        current_winning_side is not None
        and state.both_sides_traded()
        and _shares_for_side(state, current_winning_side) <= _shares_for_side(state, _other_side(current_winning_side)) + 1e-12
    ):
        side = current_winning_side
    elif state.both_sides_traded() and abs(state.shares_up - state.shares_down) > 1e-12:
        side = "UP" if state.shares_up < state.shares_down else "DOWN"
    else:
        side = _weak_outcome_side(state)
    if side is None:
        side = "UP" if up_ask <= down_ask else "DOWN"
    ask_px = up_ask if side == "UP" else down_ask
    other = _other_side(side)
    weak_side = _weak_outcome_side(state)
    balancing_overloaded_side = (
        state.both_sides_traded()
        and abs(state.shares_up - state.shares_down) > _configured_max_share_gap(cfg) + 1e-12
        and _shares_for_side(state, side) < _shares_for_side(state, other)
    )
    if missing_side is not None and has_real_position(state, other):
        pass
    elif weak_side is not None and side != weak_side and not state.both_sides_traded() and not balancing_overloaded_side:
        _log_tag("SKIP", slug=runner.contract.slug, side=side, reason="would_increase_stronger_outcome", weak_side=weak_side)
        return None
    enforce_repair_guards = missing_side is None and state.both_sides_traded()
    amount_usd = _choose_order_amount_usd(
        side=side,
        ask_px=ask_px,
        state=state,
        cfg=cfg,
        enforce_repair_guards=enforce_repair_guards,
        preferred_larger_side=current_winning_side,
    )
    if amount_usd is None:
        if missing_side is not None and remaining <= float(cfg.order_cutoff_remaining_sec):
            runner.stop_reason = "one_sided_unhedged"
            _log_tag(
                "STOP",
                slug=runner.contract.slug,
                side=side,
                reason="one_sided_unhedged",
                ask=f"{ask_px:.4f}",
                shares_up=f"{state.shares_up:.6f}",
                shares_down=f"{state.shares_down:.6f}",
            )
            return None
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason=(
                _repair_candidate_skip_reason(
                    side=side,
                    ask_px=ask_px,
                    state=state,
                    cfg=cfg,
                    preferred_larger_side=current_winning_side,
                )
                if enforce_repair_guards
                else "no_valid_amount"
            ),
            ask=f"{ask_px:.4f}",
            shares_up=f"{state.shares_up:.6f}",
            shares_down=f"{state.shares_down:.6f}",
            abs_share_gap=f"{abs(state.shares_up - state.shares_down):.4f}",
            max_share_gap=f"{_configured_max_share_gap(cfg):.4f}",
            avg_sum=f"{_avg_sum(state):.4f}",
            avg_sum_cap=f"{_configured_repair_avg_sum_cap(cfg):.4f}",
        )
        return None

    locked_profit = (
        state.pnl_if_up() >= _locked_profit_target_usd(cfg) - 1e-12
        and state.pnl_if_down() >= _locked_profit_target_usd(cfg) - 1e-12
    )
    if locked_profit and not (ask_px <= WEAK_REPAIR_CHEAP_CAP + 1e-12 or state.share_imbalance() > LOCKED_PROFIT_IMBALANCE_TRIGGER + 1e-12):
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason="locked_profit_guard",
            ask=f"{ask_px:.4f}",
            pnl_if_up=f"{state.pnl_if_up():.4f}",
            pnl_if_down=f"{state.pnl_if_down():.4f}",
            imbalance=f"{state.share_imbalance():.4f}",
        )
        return None

    limit_price = _passive_limit_buy_price(ask_px)
    required_side = _required_hedge_side(confirmed, cfg)
    if required_side is not None and side != required_side:
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason="balance_side_required",
            required_side=required_side,
            shares_up=f"{state.shares_up:.6f}",
            shares_down=f"{state.shares_down:.6f}",
            abs_share_gap=f"{_abs_share_gap(state):.4f}",
            max_share_gap=f"{_configured_max_share_gap(cfg):.4f}",
        )
        return None
    if (
        missing_side is None
        and state.both_sides_traded()
        and not _limit_order_respects_share_gap(state, side, limit_price, cfg)
    ):
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason="share_gap_limit_guard",
            ask=f"{ask_px:.4f}",
            limit_price=f"{limit_price:.4f}",
            shares_up=f"{state.shares_up:.6f}",
            shares_down=f"{state.shares_down:.6f}",
            abs_share_gap=f"{_abs_share_gap(state):.4f}",
            projected_abs_share_gap=f"{_projected_abs_share_gap_after_limit(state, side, limit_price):.4f}",
            max_share_gap=f"{_configured_max_share_gap(cfg):.4f}",
        )
        return None

    if missing_side is not None:
        missing_cap = FINAL_60_DANGER_CAP if remaining <= 60.0 else HIGH_REPAIR_PRE240_CAP
        if ask_px <= missing_cap + 1e-12:
            return BuyAction(side=side, ask_px=ask_px, amount_usd=amount_usd, reason="missing_side_open", enforce_avg_cap=False)
        if remaining <= float(cfg.order_cutoff_remaining_sec):
            runner.stop_reason = "one_sided_unhedged"
            _log_tag(
                "STOP",
                slug=runner.contract.slug,
                side=side,
                reason="one_sided_unhedged",
                ask=f"{ask_px:.4f}",
                cap=f"{missing_cap:.4f}",
                shares_up=f"{state.shares_up:.6f}",
                shares_down=f"{state.shares_down:.6f}",
            )
            return None
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason="missing_side_cap",
            ask=f"{ask_px:.4f}",
            cap=f"{missing_cap:.4f}",
            pnl_if_up=f"{state.pnl_if_up():.4f}",
            pnl_if_down=f"{state.pnl_if_down():.4f}",
            imbalance=f"{state.share_imbalance():.4f}",
        )
        return None

    if ask_px <= WEAK_REPAIR_CHEAP_CAP + 1e-12:
        _proj_up, _proj_down, _proj_worst, _proj_gap, _proj_share_gap, projected_avg_sum = _projected_after_buy(
            state, side, ask_px, amount_usd
        )
        if locked_profit and projected_avg_sum > AVG_SUM_CAP + 1e-12:
            _log_tag("SKIP", slug=runner.contract.slug, side=side, reason="locked_profit_avg_sum_guard", ask=f"{ask_px:.4f}", projected_avg_sum=f"{projected_avg_sum:.4f}")
            return None
        return BuyAction(side=side, ask_px=ask_px, amount_usd=amount_usd, reason="cheap_weak_repair", enforce_avg_cap=False)

    if not _high_repair_allowed(state, side=side, ask_px=ask_px, amount_usd=amount_usd, elapsed=elapsed, remaining=remaining):
        _log_tag(
            "SKIP",
            slug=runner.contract.slug,
            side=side,
            reason="high_repair_guard",
            ask=f"{ask_px:.4f}",
            pnl_if_up=f"{state.pnl_if_up():.4f}",
            pnl_if_down=f"{state.pnl_if_down():.4f}",
            imbalance=f"{state.share_imbalance():.4f}",
        )
        return None

    return BuyAction(side=side, ask_px=ask_px, amount_usd=amount_usd, reason="guarded_high_repair", enforce_avg_cap=False)


def _maybe_guarded_pnl_buy(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    remaining: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    current_winning_side: str | None = None,
) -> bool:
    urgent_balance = _needs_urgent_balance_order(runner, cfg)
    slot = int(elapsed) // ACTIVE_REPAIR_INTERVAL_SEC
    if not urgent_balance and slot <= runner.last_repair_slot:
        return False
    if slot > runner.last_repair_slot:
        runner.last_repair_slot = slot

    action = _choose_guarded_pnl_buy(
        runner,
        up_ask=up_ask,
        down_ask=down_ask,
        elapsed=elapsed,
        remaining=remaining,
        cfg=cfg,
        current_winning_side=current_winning_side,
    )
    if action is None:
        return False
    limit_price = action.ask_px if action.reason in {"balance_limit_repair", "missing_side_limit"} else _passive_limit_buy_price(action.ask_px)
    return _send_limit_buy(
        runner=runner,
        side=action.side,
        price=limit_price,
        reason=action.reason,
        elapsed=elapsed,
        clob=clob,
        cfg=cfg,
    )


def _window_order_notional_usd(*, clob: KngtopClob | None, cfg: KngtopConfig) -> float:
    del clob
    return max(MIN_ORDER_USD, float(cfg.notional_usd))


def _discover_target_windows(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    binance_symbol: str,
    clob: KngtopClob | None,
) -> None:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for start_sec in _candidate_window_starts(now_ts):
        if start_sec in runners:
            continue
        contract = discover_updown_window_by_start(
            market_symbol=TRADE_PAIR_KEY.lower(),
            window_minutes=TRADE_WINDOW_MINUTES,
            start_sec=start_sec,
            timeout=cfg.request_timeout_sec,
        )
        if contract is None:
            continue
        window_open_px = fetch_binance_window_open_px(
            symbol=binance_symbol,
            window_start_sec=start_sec,
            window_minutes=TRADE_WINDOW_MINUTES,
            timeout=cfg.request_timeout_sec,
        )
        runners[start_sec] = WindowRunner(
            pair_key=TRADE_PAIR_KEY,
            binance_symbol=binance_symbol,
            contract=contract,
            window_minutes=TRADE_WINDOW_MINUTES,
            window_open_px=window_open_px,
        )
        _log_tag(
            "INIT",
            slug=contract.slug,
            start_sec=str(start_sec),
            default_notional_usd=f"{_window_order_notional_usd(clob=clob, cfg=cfg):.2f}",
        )


def _refresh_subscriptions(*, runners: dict[int, WindowRunner], poly: MarketWsFeed) -> None:
    asset_ids: list[str] = []
    for runner in runners.values():
        asset_ids.append(runner.contract.up.token_id)
        asset_ids.append(runner.contract.down.token_id)
    poly.set_assets(asset_ids)


def _purge_finished_windows(*, runners: dict[int, WindowRunner]) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    for start_sec, runner in list(runners.items()):
        elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
        if elapsed is None or remaining is None:
            continue
        if remaining > 0:
            continue
        runners.pop(start_sec, None)
        if runner.positions.total_deals > 0:
            _log_tag(
                "WINDOW END",
                slug=runner.contract.slug,
                deals=str(runner.positions.total_deals),
                both_sides=str(int(runner.positions.both_sides_traded())),
                pnl_if_up=f"{runner.positions.pnl_if_up():.4f}",
                pnl_if_down=f"{runner.positions.pnl_if_down():.4f}",
            )


def _tick_runner(
    runner: WindowRunner,
    *,
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    if runner.stop_reason is not None:
        return
    if not cfg.dry_run and clob is not None:
        if _sync_and_enforce_single_open_limit(runner, clob=clob, cfg=cfg):
            _reconcile_pending_limit_order(runner, cfg=cfg, clob=clob)
            return
    elif runner.pending_order:
        if _reconcile_pending_limit_order(runner, cfg=cfg, clob=clob):
            return
    if runner.execution_state == ORDER_IN_FLIGHT:
        return
    if runner.next_decision_ts > now_ts + 1e-12:
        runner.execution_state = WAIT_NEXT_DECISION
        return
    runner.execution_state = READY
    elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
    if elapsed is None or remaining is None:
        return
    if elapsed < 0:
        return
    if not cfg.dry_run:
        try:
            _refresh_positions_from_pm(runner, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            _log_tag("RECONCILE", slug=runner.contract.slug, status="error", error=str(exc))
            return
    if runner.window_open_px is None:
        start_sec = runner.start_sec()
        if start_sec is None:
            return
        runner.window_open_px = fetch_binance_window_open_px(
            symbol=runner.binance_symbol,
            window_start_sec=start_sec,
            window_minutes=runner.window_minutes,
            timeout=cfg.request_timeout_sec,
        )
        if runner.window_open_px is None:
            return
    current_px = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
    if current_px is None:
        return
    current_winning_side: str | None = None
    if runner.window_open_px is not None:
        if float(current_px) > float(runner.window_open_px) + 1e-12:
            current_winning_side = "UP"
        elif float(current_px) < float(runner.window_open_px) - 1e-12:
            current_winning_side = "DOWN"
    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    if up_quote is None or down_quote is None:
        return
    _up_bid, up_ask = up_quote
    _down_bid, down_ask = down_quote

    if _maybe_guarded_pnl_buy(
        runner,
        up_ask=float(up_ask),
        down_ask=float(down_ask),
        elapsed=elapsed,
        remaining=remaining,
        clob=clob,
        cfg=cfg,
        current_winning_side=current_winning_side,
    ):
        return
    runner.execution_state = WAIT_NEXT_DECISION
    runner.next_decision_ts = now_ts + float(ACTIVE_REPAIR_INTERVAL_SEC)


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
) -> None:
    binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    _discover_target_windows(cfg, runners=runners, binance_symbol=binance_symbol, clob=clob)
    _refresh_subscriptions(runners=runners, poly=poly)
    for runner in list(runners.values()):
        try:
            _tick_runner(runner, poly=poly, binance=binance, clob=clob, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", slug=runner.contract.slug, stage="tick", error=str(exc))
    _purge_finished_windows(runners=runners)


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    btc_binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    coord = EvalCoordinator(debounce_sec=0.0, heartbeat_sec=cfg.poll_interval_sec)
    runtime_state: dict[str, Any] = {}

    def _on_poly_quote() -> None:
        _log_ws_update(runtime_state, feed="polymarket")
        coord.notify()

    def _on_binance_trade(symbol: str) -> None:
        _log_ws_update(runtime_state, feed="binance", symbol=symbol)
        coord.notify()

    poly = MarketWsFeed(
        on_quote_update=_on_poly_quote,
        on_ws_reconnect=lambda dt: _ws_reconnected_event("polymarket", dt),
    )
    binance = BinanceCombinedTradeFeed(
        [btc_binance_symbol],
        on_trade=_on_binance_trade,
        on_ws_reconnect=lambda dt: _ws_reconnected_event("binance", dt),
    )
    poly.start()
    binance.start()

    rest_poll_stop = threading.Event()
    if cfg.ws_rest_poll_enabled:
        threading.Thread(
            target=run_ws_rest_fallback_loop,
            args=(rest_poll_stop, cfg, binance, poly),
            name="ws-rest-fallback",
            daemon=True,
        ).start()

    clob: KngtopClob | None = None
    if not cfg.dry_run:
        clob = KngtopClob(
            private_key=cfg.private_key,
            funder=cfg.funder,
            signature_type=cfg.signature_type,
            relayer_api_key=cfg.relayer_api_key,
            relayer_secret=cfg.relayer_secret,
            relayer_passphrase=cfg.relayer_passphrase,
            market_buy_max_price=cfg.market_buy_max_price,
        )

    runners: dict[int, WindowRunner] = {}
    _log_tag(
        "INIT",
        pair=TRADE_PAIR_KEY,
        window_minutes=str(TRADE_WINDOW_MINUTES),
        strategy="guarded_pnl_balance_C",
        bootstrap="initial_lower_ask<=0.55_amount2",
        active_repair="5s_weak_outcome_cheap<=0.45_high_guard<=0.60",
        high_repair="pre240<=0.65_final60_danger<=0.80",
        min_order_usd=f"{MIN_ORDER_USD:.2f}",
        first_order_usd=f"{min(float(cfg.notional_usd), LARGE_ORDER_USD):.2f}",
        later_order_usd="1.00-2.00_projected",
        max_shares_per_side=f"{_configured_max_shares_per_side(cfg):.2f}",
        max_share_gap=f"{_configured_max_share_gap(cfg):.2f}",
        locked_profit_target=f"{_locked_profit_target_usd(cfg):.2f}",
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
