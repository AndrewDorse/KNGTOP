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
from kngtop import live_orders as lo
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
READY = "READY"
ORDER_IN_FLIGHT = "ORDER_IN_FLIGHT"
WAIT_NEXT_DECISION = "WAIT_NEXT_DECISION"


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
    last_successful_buy_ts: float = -10_000.0
    last_position_refresh_ts: float = 0.0
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
    orders_sent: int = 0
    cycle: lo.OrderCycle = field(default_factory=lo.OrderCycle)
    orders: dict[str, lo.LiveOrder] = field(default_factory=dict)
    open_orders: dict[str, list[lo.OpenOrderView]] = field(default_factory=lambda: {"UP": [], "DOWN": []})
    last_reconcile_monotonic: float = 0.0
    reconcile_seq: int = 0

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)

    @property
    def pending_order(self) -> bool:
        return lo.has_active_order(self)

    @property
    def pending_side(self) -> str | None:
        order = lo.active_order(self)
        return order.side if order else None

    @property
    def pending_order_id(self) -> str | None:
        order = lo.active_order(self)
        return order.order_id if order else None

    @property
    def pending_reason(self) -> str | None:
        order = lo.active_order(self)
        return order.reason if order else None

    @property
    def pending_ask_px(self) -> float:
        order = lo.active_order(self)
        return order.price if order else 0.0

    @property
    def pending_amount_usd(self) -> float:
        order = lo.active_order(self)
        return order.reserved_shares() * order.price if order else 0.0

    @property
    def pending_reserved_shares(self) -> float:
        order = lo.active_order(self)
        return order.reserved_shares() if order else 0.0

    @property
    def pending_created_ts(self) -> float:
        order = lo.active_order(self)
        return order.sent_ts if order else 0.0

    @property
    def execution_state(self) -> str:
        if lo.has_active_order(self):
            return ORDER_IN_FLIGHT
        if self.next_decision_ts > datetime.now(timezone.utc).timestamp() + 1e-12:
            return WAIT_NEXT_DECISION
        return READY


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


def _needs_urgent_balance_order(runner: WindowRunner, cfg: KngtopConfig) -> bool:
    confirmed = _confirmed_position_state(runner)
    required_side = _required_hedge_side(confirmed, cfg)
    if required_side is None:
        return False
    active = lo.active_order(runner)
    if active is not None and active.side == required_side:
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


def _current_gap(state: PositionState) -> float:
    return abs(state.pnl_if_up() - state.pnl_if_down())


def _bootstrap_amount_usd(cfg: KngtopConfig) -> float:
    return max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))


def _configured_max_shares_per_side(cfg: KngtopConfig) -> float:
    return max(1.0, float(getattr(cfg, "max_shares_per_side", MAX_SHARES_PER_SIDE)))


def _configured_max_share_gap(cfg: KngtopConfig) -> float:
    return max(0.0, float(getattr(cfg, "max_share_gap", MAX_SHARE_GAP)))


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


def _side_over_cap(state: PositionState, side: str, cfg: KngtopConfig) -> bool:
    return _shares_for_side(state, side) > _configured_max_shares_per_side(cfg) + 1e-12


def _share_room(state: PositionState, side: str, cfg: KngtopConfig) -> float:
    return max(0.0, _configured_max_shares_per_side(cfg) - _shares_for_side(state, side))


def _effective_state_with_pending(runner: WindowRunner) -> PositionState:
    return lo.projected_positions(runner, _sync_effective_positions(runner))


def _can_place_limit_buy(runner: WindowRunner, side: str, price: float, cfg: KngtopConfig) -> bool:
    if not lo.cycle_is_idle(runner):
        return False
    shares = _limit_order_shares(price)
    cost = shares * max(price, 0.0)
    if shares <= 0.0 or cost + 1e-12 < LIMIT_MIN_USD:
        return False
    confirmed = _confirmed_position_state(runner)
    if _side_over_cap(confirmed, side, cfg):
        return False
    flat = not has_real_position(confirmed, "UP") and not has_real_position(confirmed, "DOWN")
    required_side = _required_hedge_side(confirmed, cfg)
    if flat:
        state = lo.projected_positions(runner, confirmed)
        return _can_buy(state, side, cost, ask_px=price, cfg=cfg)
    if required_side is None or side != required_side:
        return False
    state = lo.projected_positions(runner, confirmed)
    if not _can_buy(state, side, cost, ask_px=price, cfg=cfg):
        return False
    if _missing_position_side(confirmed) is not None:
        return True
    return _balance_limit_improves_gap(state, side, price, cfg)


def _cap_amount_to_share_room(state: PositionState, side: str, ask_px: float, amount_usd: float, cfg: KngtopConfig) -> float | None:
    if ask_px <= 0.0:
        return None
    capped = min(float(amount_usd), _share_room(state, side, cfg) * float(ask_px))
    if capped + 1e-12 < MIN_ORDER_USD:
        return None
    return max(MIN_ORDER_USD, min(capped, float(amount_usd)))


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
    rows: list[dict[str, object]] | None = None,
) -> PositionState:
    _ensure_local_position_floor(runner)
    prev_effective = _sync_effective_positions(runner)
    prev_shares_up = prev_effective.shares_up
    prev_shares_down = prev_effective.shares_down
    prev_spent_up = prev_effective.spent_up
    prev_spent_down = prev_effective.spent_down
    prev_orders_up = prev_effective.orders_up
    prev_orders_down = prev_effective.orders_down
    if rows is None:
        rows = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
    active = lo.active_order(runner)
    if (
        active is not None
        and not runner.initial_filled
        and prev_shares_up <= 1e-6
        and prev_shares_down <= 1e-6
    ):
        pending_token = runner.contract.up.token_id if active.side == "UP" else runner.contract.down.token_id
        scoped_rows: list[dict[str, object]] = []
        for row in rows:
            slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
            asset_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
            outcome = str(row.get("outcome") or "").strip().upper()
            if slug and slug == runner.contract.slug and outcome == active.side:
                scoped_rows.append(row)
            elif asset_id and asset_id == pending_token:
                scoped_rows.append(row)
        rows = scoped_rows
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
        if active is not None and active.side == side and active.price > 1e-12:
            cost_by_side[side] += added_shares * active.price
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
                pending_ask=f"{active.price:.4f}" if active is not None and active.side == side else None,
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


def _apply_cached_reconcile_snapshot(
    runner: WindowRunner,
    *,
    runtime_state: dict[str, Any],
    cfg: KngtopConfig,
    clob: KngtopClob | None,
) -> None:
    seq = int(runtime_state.get("reconcile_seq", 0))
    if seq <= runner.reconcile_seq:
        if cfg.dry_run or clob is None:
            return
        if time.perf_counter() - runner.last_reconcile_monotonic + 1e-12 < lo.RECONCILE_INTERVAL_SEC:
            return
        _refresh_positions_from_pm(runner, cfg=cfg)
        confirmed = _confirmed_position_state(runner)
        lo.reconcile_runner_orders(
            runner,
            clob=clob,
            open_order_rows=clob.get_open_orders(),
            now_ts=time.time(),
            pre_shares_by_side={"UP": confirmed.shares_up, "DOWN": confirmed.shares_down},
        )
        runner.last_reconcile_monotonic = time.perf_counter()
        return

    position_rows = lo._filtered_positions_for_runner(
        runner,
        list(runtime_state.get("reconcile_positions", [])),
    )
    open_rows = list(runtime_state.get("reconcile_open_orders", []))
    _refresh_positions_from_pm(runner, cfg=cfg, rows=position_rows)
    confirmed = _confirmed_position_state(runner)
    lo.reconcile_runner_orders(
        runner,
        clob=clob,
        open_order_rows=open_rows,
        now_ts=float(runtime_state.get("reconcile_wall_ts", time.time())),
        pre_shares_by_side={"UP": confirmed.shares_up, "DOWN": confirmed.shares_down},
    )
    runner.reconcile_seq = seq
    runner.last_reconcile_monotonic = float(runtime_state.get("reconcile_cache_at", time.perf_counter()))
    active = lo.active_order(runner)
    _log_tag(
        "RECONCILE",
        slug=runner.contract.slug,
        seq=str(runner.reconcile_seq),
        up_open=str(len(runner.open_orders.get("UP", []))),
        down_open=str(len(runner.open_orders.get("DOWN", []))),
        orders_active=str(1 if active is not None else 0),
    )


def _next_retry_delay(reason: str) -> float:
    if reason in {"initial_lower_ask", "cheap_weak_repair", "guarded_high_repair"}:
        return 1.0
    return float(ACTIVE_REPAIR_INTERVAL_SEC)


def _schedule_next_decision(runner: WindowRunner, *, now_ts: float, reason: str) -> None:
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


def _positions_balanced_and_capped(state: PositionState, cfg: KngtopConfig) -> bool:
    if _over_cap_side(state, cfg) is not None:
        return False
    return abs(state.shares_up - state.shares_down) <= _configured_max_share_gap(cfg) + 1e-12


def _cycle_side_progress(c: lo.OrderCycle, state: PositionState, side: str, expected_shares: float) -> bool:
    if expected_shares <= 1e-12:
        return True
    start = c.pm_up_start if side == "UP" else c.pm_down_start
    if start < 0.0:
        return True
    current = _shares_for_side(state, side)
    return current >= start + expected_shares * 0.5 - 1e-3


def _cycle_fills_reflected(runner: WindowRunner) -> bool:
    c = runner.cycle
    pos = _confirmed_position_state(runner)
    primary_ok = _cycle_side_progress(c, pos, c.primary_side, c.primary_shares)
    if not c.hedge_sent:
        return primary_ok
    return primary_ok and _cycle_side_progress(c, pos, c.hedge_side, c.hedge_shares)


def _cycle_ready_to_close(runner: WindowRunner, cfg: KngtopConfig) -> bool:
    pos = _confirmed_position_state(runner)
    if not _cycle_fills_reflected(runner):
        return False
    return _positions_balanced_and_capped(pos, cfg)


def _begin_pm_wait(runner: WindowRunner, *, cfg: KngtopConfig) -> None:
    _refresh_positions_from_pm(runner, cfg=cfg)
    pos = _confirmed_position_state(runner)
    lo.cycle_start_pm_wait(runner, up_shares=pos.shares_up, down_shares=pos.shares_down)


def _post_cycle_limit(
    runner: WindowRunner,
    *,
    side: str,
    price: float,
    shares: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> str | None:
    """Single CLOB send entry — primary and hedge both go through here."""
    if cfg.dry_run or clob is None:
        return f"dry-{side.lower()}-{runner.cycle.cycle_n}"
    token = _token_for_side(runner, side)
    payload = clob.limit_buy_shares(token, price=price, shares=shares, post_only=True)
    return _extract_order_id(payload) if isinstance(payload, dict) else None


def _advance_order_cycle(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    up_ask: float,
    down_ask: float,
) -> bool:
    """Returns True while cycle busy — strategy must STOP."""
    c = runner.cycle
    if lo.cycle_is_idle(runner):
        return False

    lo.sync_clob_open_orders(runner, clob=clob)
    lo.log_cycle(runner)
    now_ts = datetime.now(timezone.utc).timestamp()

    if c.phase == lo.PHASE_WAIT_PRIMARY:
        primary_on_book = lo.order_on_clob(
            runner, clob=clob, order_id=c.primary_order_id, side=c.primary_side
        )
        if not primary_on_book:
            return True
        _log_tag("CYCLE PRIMARY ON BOOK", slug=runner.contract.slug, order_id=c.primary_order_id)
        _refresh_positions_from_pm(runner, cfg=cfg)
        confirmed = _confirmed_position_state(runner)
        hedge_side = _required_hedge_side(confirmed, cfg)
        if hedge_side is not None and hedge_side != c.primary_side and not c.hedge_sent:
            hedge_ask = up_ask if hedge_side == "UP" else down_ask
            hedge_price = _passive_limit_buy_price(hedge_ask)
            hedge_shares = _limit_order_shares(hedge_price)
            if not _can_buy(confirmed, hedge_side, hedge_shares * hedge_price, ask_px=hedge_price, cfg=cfg):
                _log_tag(
                    "CYCLE HEDGE BLOCK",
                    slug=runner.contract.slug,
                    side=hedge_side,
                    up_shares=f"{confirmed.shares_up:.6f}",
                    down_shares=f"{confirmed.shares_down:.6f}",
                )
                return True
            if not lo.cycle_begin_hedge(runner, side=hedge_side, price=hedge_price, shares=hedge_shares):
                return True
            try:
                oid = _post_cycle_limit(
                    runner, side=hedge_side, price=hedge_price, shares=hedge_shares, clob=clob, cfg=cfg
                )
            except Exception as exc:  # noqa: BLE001
                lo.cycle_reset(runner)
                _log_tag("CYCLE HEDGE FAILED", slug=runner.contract.slug, side=hedge_side, error=str(exc))
                return True
            lo.cycle_mark_hedge_id(runner, oid)
            return True
        if hedge_side is not None and hedge_side != c.primary_side and c.hedge_sent:
            return True
        _begin_pm_wait(runner, cfg=cfg)
        return True

    if c.phase == lo.PHASE_WAIT_HEDGE:
        if not lo.order_on_clob(runner, clob=clob, order_id=c.hedge_order_id, side=c.hedge_side):
            return True
        _log_tag("CYCLE HEDGE ON BOOK", slug=runner.contract.slug, order_id=c.hedge_order_id)
        _begin_pm_wait(runner, cfg=cfg)
        return True

    if c.phase == lo.PHASE_WAIT_PM:
        _refresh_positions_from_pm(runner, cfg=cfg)
        pos = _confirmed_position_state(runner)
        if not _cycle_ready_to_close(runner, cfg):
            runner.cycle.pm_stable_streak = 0
            _log_tag(
                "CYCLE PM WAIT",
                slug=runner.contract.slug,
                cycle_n=str(c.cycle_n),
                reason="not_ready",
                up_shares=f"{pos.shares_up:.6f}",
                down_shares=f"{pos.shares_down:.6f}",
                gap=f"{abs(pos.shares_up - pos.shares_down):.4f}",
            )
            return True
        if lo.tick_pm_stable(runner, up_shares=pos.shares_up, down_shares=pos.shares_down, now_ts=now_ts):
            _log_tag("CYCLE DONE", slug=runner.contract.slug, cycle_n=str(c.cycle_n))
            if _is_initial_reason(c.primary_reason):
                runner.initial_filled = True
            runner.local_positions = _copy_position_state(pos)
            runner.last_successful_buy_ts = now_ts
            lo.cycle_reset(runner)
            runner.next_decision_ts = now_ts + float(ACTIVE_REPAIR_INTERVAL_SEC)
            return False
        return True

    return True


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
    pre_state = _copy_position_state(_confirmed_position_state(runner))
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
            up_shares=f"{pre_state.shares_up:.6f}",
            down_shares=f"{pre_state.shares_down:.6f}",
        )
        return False

    confirmed = _confirmed_position_state(runner)
    if not cfg.dry_run and clob is not None:
        required_side = _required_hedge_side(confirmed, cfg)
        lo.enforce_single_order(runner, clob=clob, required_side=required_side)
        if not lo.cycle_is_idle(runner):
            _log_tag("LIMIT BLOCK", slug=runner.contract.slug, side=side, reason="cycle_busy")
            return False

    _log_current_pnl_state(runner)
    _log_pnl_projection(runner, side=side, ask_px=price, amount_usd=amount_usd)
    _record_order_intent(runner, side=side)
    if _is_initial_reason(reason) and confirmed.total_deals == 0:
        _record_initial_intent(runner, side=side, ask_px=price, elapsed=elapsed)

    if not lo.cycle_begin_primary(
        runner,
        side=side,
        price=price,
        shares=shares,
        reason=reason,
        pm_up=confirmed.shares_up,
        pm_down=confirmed.shares_down,
    ):
        return False
    sent_ts = datetime.now(timezone.utc).timestamp()

    if cfg.dry_run or clob is None:
        oid = _post_cycle_limit(runner, side=side, price=price, shares=shares, clob=clob, cfg=cfg)
        lo.cycle_mark_primary_id(runner, oid)
        _record_local_fill(runner, side, price, amount_usd, shares)
        if _is_initial_reason(reason):
            runner.initial_filled = True
        runner.last_successful_buy_ts = sent_ts
        pos = _confirmed_position_state(runner)
        lo.cycle_start_pm_wait(runner, up_shares=pos.shares_up, down_shares=pos.shares_down)
        return True

    try:
        order_id = _post_cycle_limit(
            runner, side=side, price=price, shares=shares, clob=clob, cfg=cfg
        )
        if not order_id:
            lo.cycle_reset(runner)
            runner.orders_sent = max(0, runner.orders_sent - 1)
            if _is_initial_reason(reason) and confirmed.total_deals == 0:
                _record_initial_failure(runner, side=side)
            _schedule_next_decision(runner, now_ts=sent_ts, reason=reason)
            return False
        lo.cycle_mark_primary_id(runner, order_id)
        _log_tag(
            "CYCLE WAIT PRIMARY",
            slug=runner.contract.slug,
            side=side,
            order_id=order_id,
            send_n=str(runner.orders_sent),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        lo.cycle_reset(runner)
        runner.orders_sent = max(0, runner.orders_sent - 1)
        if _is_balance_or_allowance_error(exc):
            runner.stop_reason = "balance_or_allowance"
            _log_tag("STOP", slug=runner.contract.slug, side=side, reason="balance_or_allowance", error=str(exc))
            return False
        if "invalid post-only order" in str(exc).lower() or "crosses book" in str(exc).lower():
            _log_tag("LIMIT CROSSED", slug=runner.contract.slug, side=side, reason=reason, error=str(exc))
            _schedule_next_decision(runner, now_ts=sent_ts, reason=reason)
            return False
        if _is_initial_reason(reason) and confirmed.total_deals == 0:
            _record_initial_failure(runner, side=side)
        _log_tag("LIMIT FAILED", slug=runner.contract.slug, side=side, reason=reason, error=str(exc))
        _schedule_next_decision(runner, now_ts=sent_ts, reason=reason)
        return False


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
    over_cap_side = _over_cap_side(confirmed, cfg)
    required_side = _required_hedge_side(confirmed, cfg)
    if over_cap_side is not None:
        if required_side is None or required_side == over_cap_side:
            runner.stop_reason = "over_cap_position"
            _log_tag(
                "STOP",
                slug=runner.contract.slug,
                reason="over_cap_position",
                side=over_cap_side,
                up_shares=f"{confirmed.shares_up:.6f}",
                down_shares=f"{confirmed.shares_down:.6f}",
                max_side_shares=f"{_configured_max_shares_per_side(cfg):.6f}",
            )
            return None
        _log_tag(
            "OVER_CAP_HEDGE",
            slug=runner.contract.slug,
            over_side=over_cap_side,
            hedge_side=required_side,
            up_shares=f"{confirmed.shares_up:.6f}",
            down_shares=f"{confirmed.shares_down:.6f}",
        )
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

    _log_tag(
        "SKIP",
        slug=runner.contract.slug,
        reason="balanced_no_trade",
        shares_up=f"{confirmed.shares_up:.6f}",
        shares_down=f"{confirmed.shares_down:.6f}",
        abs_share_gap=f"{_abs_share_gap(state):.4f}",
    )
    return None


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
        lo.clear_window_orders(runner)
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
    runtime_state: dict[str, Any] | None = None,
) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    if runner.stop_reason is not None:
        return
    state = runtime_state if runtime_state is not None else {}
    if not cfg.dry_run:
        try:
            _apply_cached_reconcile_snapshot(runner, runtime_state=state, cfg=cfg, clob=clob)
            confirmed = _confirmed_position_state(runner)
            required_side = _required_hedge_side(confirmed, cfg)
            if clob is not None:
                lo.sync_clob_open_orders(runner, clob=clob)
                if lo.cycle_is_idle(runner):
                    lo.enforce_single_order(runner, clob=clob, required_side=required_side)
        except Exception as exc:  # noqa: BLE001
            _log_tag("RECONCILE", slug=runner.contract.slug, status="error", error=str(exc))

    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    up_ask = float(up_quote[1]) if up_quote is not None else 0.0
    down_ask = float(down_quote[1]) if down_quote is not None else 0.0

    if _advance_order_cycle(runner, clob=clob, cfg=cfg, up_ask=up_ask, down_ask=down_ask):
        return

    if runner.next_decision_ts > now_ts + 1e-12:
        return
    elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
    if elapsed is None or remaining is None:
        return
    if elapsed < 0:
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
    if up_quote is None or down_quote is None:
        return

    if _maybe_guarded_pnl_buy(
        runner,
        up_ask=up_ask,
        down_ask=down_ask,
        elapsed=elapsed,
        remaining=remaining,
        clob=clob,
        cfg=cfg,
        current_winning_side=current_winning_side,
    ):
        _advance_order_cycle(runner, clob=clob, cfg=cfg, up_ask=up_ask, down_ask=down_ask)
        return
    runner.next_decision_ts = now_ts + float(ACTIVE_REPAIR_INTERVAL_SEC)


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    state = runtime_state if runtime_state is not None else {}
    state["runners"] = runners
    binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    _discover_target_windows(cfg, runners=runners, binance_symbol=binance_symbol, clob=clob)
    _refresh_subscriptions(runners=runners, poly=poly)
    for runner in list(runners.values()):
        try:
            _tick_runner(runner, poly=poly, binance=binance, clob=clob, cfg=cfg, runtime_state=state)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", slug=runner.contract.slug, stage="tick", error=str(exc))
    _purge_finished_windows(runners=runners)


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    btc_binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    coord = EvalCoordinator(debounce_sec=0.0, heartbeat_sec=cfg.poll_interval_sec)
    runtime_state: dict[str, Any] = {"runners": {}}
    reconcile_stop = threading.Event()

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
    runtime_state["runners"] = runners
    if not cfg.dry_run and clob is not None:
        threading.Thread(
            target=lo.run_live_reconcile_loop,
            args=(reconcile_stop,),
            kwargs={
                "clob": clob,
                "cfg": cfg,
                "runtime_state": runtime_state,
                "on_update": coord.notify,
            },
            name="live-reconcile",
            daemon=True,
        ).start()
    _log_tag(
        "INIT",
        pair=TRADE_PAIR_KEY,
        window_minutes=str(TRADE_WINDOW_MINUTES),
        strategy="guarded_pnl_balance_C",
        reconcile_interval_sec=f"{lo.RECONCILE_INTERVAL_SEC:.1f}",
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
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob, runtime_state=runtime_state)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
