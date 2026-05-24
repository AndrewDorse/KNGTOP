"""BTC 5m balance-first fixed-limit-order engine.

Rule shape:
- Trade fixed 5-share limit buys.
- Use the C Strategy / Balanced Fixed 47c Strategy:
  - start with one UP and one DOWN limit buy at 0.47 during the prestart/opening gate;
  - recover imbalance only on the smaller-share side;
  - keep local pending orders and pending cancels as hard duplicate locks;
  - cap hedge price at 0.70 and avg_sum_after_buy at 0.95.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_window_open_px
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.eval_coordinator import EvalCoordinator
from kngtop.gamma import ActiveContract, TokenMarket, discover_updown_window_by_start, window_start_ts_from_slug
from kngtop.pm_data import fetch_user_positions
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")

TRADE_PAIR_KEY = "BTC"
TRADE_WINDOW_MINUTES = 5
WINDOW_SECONDS = TRADE_WINDOW_MINUTES * 60
PRESTART_SEC = 20
OPENING_GRACE_SEC = 2
INITIAL_PAIR_PRICE = 0.47
ORDER_SHARES = 5.0
ORDER_SIZE_SHARES = ORDER_SHARES
MAX_SHARES_PER_SIDE = 15.0
INITIAL_PRICE = INITIAL_PAIR_PRICE
MAX_HEDGE_PRICE = 0.70
MAX_AVG_SUM_AFTER_BUY = 0.95
ACTION_COOLDOWN_SECONDS = 1.0
UNKNOWN_ORDER_TIMEOUT_SECONDS = 7.0
RECENT_SENT_CACHE_SECONDS = 10.0
MAX_SPENT_PER_WINDOW = 20.0
REPRICE_TOLERANCE = 0.005
MIN_ORDER_USD = 1.05
AVG_IMPROVE_BUFFER = 0.02
AVG_SUM_CAP = 0.95
C_MISSING_HEDGE_CAP = 0.70
C_WEAK_CAP = 0.45
C_HIGH_GUARD = 0.60
C_BALANCE_EPSILON = 1e-9
TRADE_HISTORY_LOOKBACK_SEC = 600


@dataclass(slots=True)
class PositionState:
    spent_up: float = 0.0
    spent_down: float = 0.0
    shares_up: float = 0.0
    shares_down: float = 0.0

    def spent_total(self) -> float:
        return self.spent_up + self.spent_down

    def avg(self, side: str) -> float:
        shares = self.shares_up if side == "UP" else self.shares_down
        spent = self.spent_up if side == "UP" else self.spent_down
        return spent / shares if shares > 1e-12 else 0.0

    def shares(self, side: str) -> float:
        return self.shares_up if side == "UP" else self.shares_down

    def avg_sum(self) -> float:
        return self.avg("UP") + self.avg("DOWN")

    def pnl_if(self, side: str) -> float:
        return self.shares(side) - self.spent_total()

    def worst_case_pnl(self) -> float:
        return min(self.pnl_if("UP"), self.pnl_if("DOWN"))

    def has_side(self, side: str) -> bool:
        return self.shares(side) > C_BALANCE_EPSILON

    def both_sides(self) -> bool:
        return self.has_side("UP") and self.has_side("DOWN")


@dataclass(slots=True)
class OpenOrder:
    order_id: str
    side: str
    price: float
    remaining_shares: float
    client_order_id: str | None = None
    bot_owned: bool = False


@dataclass(slots=True)
class LocalPendingOrder:
    client_order_id: str
    side: str
    token_id: str
    price: float
    shares: float
    sent_at: float
    position_shares_at_send: float = 0.0
    status: str = "SENT_LOCAL"
    exchange_order_id: str | None = None
    resolved: bool = False
    observed_refreshes: int = 0


@dataclass(slots=True)
class LocalPendingCancel:
    order_id: str
    side: str
    price: float
    shares: float
    sent_at: float
    status: str = "SENT_LOCAL"
    resolved: bool = False


@dataclass(slots=True)
class StrategyDecision:
    action: str
    reason: str
    orders: list[tuple[str, float, float]] = field(default_factory=list)
    cancel_order: OpenOrder | None = None


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    positions: PositionState = field(default_factory=PositionState)
    open_orders: dict[str, list[OpenOrder]] = field(default_factory=lambda: {"UP": [], "DOWN": []})
    trade_history_positions: PositionState = field(default_factory=PositionState)
    last_trade_history_fetch_ts: float = 0.0
    local_pending_orders_by_side: dict[str, list[LocalPendingOrder]] = field(default_factory=lambda: {"UP": [], "DOWN": []})
    local_pending_cancels_by_order_id: dict[str, LocalPendingCancel] = field(default_factory=dict)
    fill_history_seen_ids: set[str] = field(default_factory=set)
    initial_batch_sent: bool = False
    initial_batch_resolved: bool = False
    last_action_time: float = 0.0
    action_lock_until: float = 0.0
    recent_sent_cache: dict[str, float] = field(default_factory=dict)
    last_reconcile_summary: dict[str, Any] = field(default_factory=dict)
    stop_reason: str | None = None

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)

    def market_id(self) -> str:
        return self.contract.slug

    def condition_id(self) -> str:
        return self.contract.slug


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


def _current_window_start_sec(now_ts: int, window_minutes: int) -> int:
    window_sec = max(60, int(window_minutes) * 60)
    return (int(now_ts) // window_sec) * window_sec


def _candidate_window_starts(now_ts: int) -> tuple[int, ...]:
    current_start = _current_window_start_sec(now_ts, TRADE_WINDOW_MINUTES)
    next_start = current_start + WINDOW_SECONDS
    if next_start - int(now_ts) <= PRESTART_SEC:
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
    return runner.contract.up if side == "UP" else runner.contract.down


def _opposite_side(side: str) -> str:
    return "DOWN" if side == "UP" else "UP"


def _bot_client_order_id(runner: WindowRunner, side: str) -> str:
    return f"kngtop-c47-{runner.contract.slug}-{side.lower()}-{uuid4().hex[:12]}"


def _pending_orders(runner: WindowRunner, side: str | None = None, *, unresolved_only: bool = True) -> list[LocalPendingOrder]:
    sides = ("UP", "DOWN") if side is None else (side,)
    out: list[LocalPendingOrder] = []
    for item_side in sides:
        for order in runner.local_pending_orders_by_side.get(item_side, []):
            if unresolved_only and order.resolved:
                continue
            out.append(order)
    return out


def _pending_order_shares(runner: WindowRunner, side: str) -> float:
    return sum(max(0.0, order.shares) for order in _pending_orders(runner, side))


def _pending_cancel_count(runner: WindowRunner) -> int:
    return sum(1 for cancel in runner.local_pending_cancels_by_order_id.values() if not cancel.resolved)


def _has_unresolved_local_action(runner: WindowRunner) -> bool:
    return bool(_pending_orders(runner)) or _pending_cancel_count(runner) > 0


def _open_order_count(runner: WindowRunner, side: str | None = None) -> int:
    if side is not None:
        return len(runner.open_orders.get(side, []))
    return sum(len(orders) for orders in runner.open_orders.values())


def _live_order_count(runner: WindowRunner, side: str | None = None) -> int:
    return _open_order_count(runner, side) + len(_pending_orders(runner, side))


def _effective_side_shares(runner: WindowRunner, side: str) -> float:
    return runner.positions.shares(side) + _open_order_shares(runner, side) + _pending_order_shares(runner, side)


def _state_type(pos: PositionState) -> str:
    if pos.shares_up <= C_BALANCE_EPSILON and pos.shares_down <= C_BALANCE_EPSILON:
        return "EMPTY"
    if abs(pos.shares_up - pos.shares_down) <= C_BALANCE_EPSILON:
        return "BALANCED"
    return "IMBALANCED"


def _smaller_side(pos: PositionState) -> str | None:
    if pos.shares_up < pos.shares_down - C_BALANCE_EPSILON:
        return "UP"
    if pos.shares_down < pos.shares_up - C_BALANCE_EPSILON:
        return "DOWN"
    return None


def _sent_cache_key(runner: WindowRunner, side: str, price: float, shares: float) -> str:
    return f"{runner.contract.slug}:{side}:{price:.2f}:{shares:.2f}"


def _prune_recent_sent_cache(runner: WindowRunner, now_ts: float) -> None:
    runner.recent_sent_cache = {
        key: sent_at for key, sent_at in runner.recent_sent_cache.items() if now_ts - float(sent_at) < RECENT_SENT_CACHE_SECONDS
    }


def _extract_order_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("orderID", "orderId", "id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _extract_client_order_id(payload: dict[str, object]) -> str | None:
    for key in ("client_order_id", "clientOrderId", "clientOrderID"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _extract_numeric(row: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        try:
            value = row.get(key)
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _extract_text(row: dict[str, object], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return ""


def _parse_side_from_position(row: dict[str, object], runner: WindowRunner) -> str | None:
    outcome = str(row.get("outcome") or row.get("title") or "").strip().upper()
    if outcome in {"UP", "DOWN"}:
        return outcome
    asset = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
    if asset == runner.contract.up.token_id:
        return "UP"
    if asset == runner.contract.down.token_id:
        return "DOWN"
    return None


def _choose_richer_side(base: PositionState, history: PositionState, side: str) -> tuple[float, float]:
    base_shares = base.shares(side)
    hist_shares = history.shares(side)
    if hist_shares > base_shares + 1e-9:
        return hist_shares, hist_shares * history.avg(side)
    return base_shares, base_shares * base.avg(side)


def _merge_position_sources(base: PositionState, history: PositionState) -> PositionState:
    up_shares, up_spent = _choose_richer_side(base, history, "UP")
    down_shares, down_spent = _choose_richer_side(base, history, "DOWN")
    return PositionState(spent_up=up_spent, spent_down=down_spent, shares_up=up_shares, shares_down=down_shares)


def _refresh_positions(runner: WindowRunner, *, cfg: KngtopConfig, rows: list[dict[str, object]] | None = None) -> PositionState:
    if rows is None:
        rows = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
    previous = runner.positions
    pos = PositionState()
    token_ids = {runner.contract.up.token_id, runner.contract.down.token_id}
    for row in rows:
        slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        asset = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
        if slug and slug != runner.contract.slug:
            continue
        if asset and asset not in token_ids:
            continue
        side = _parse_side_from_position(row, runner)
        if side is None:
            continue
        size = _extract_numeric(row, "size", "amount", "shares") or 0.0
        avg_price = _extract_numeric(row, "avgPrice", "averagePrice", "avg_price", "price") or 0.0
        if size <= 1e-12:
            continue
        cost = size * avg_price if avg_price > 1e-12 else 0.0
        if side == "UP":
            pos.shares_up += size
            pos.spent_up += cost
        else:
            pos.shares_down += size
            pos.spent_down += cost
    merged = _merge_position_sources(pos, runner.trade_history_positions)
    del previous
    runner.positions = merged
    return merged


def _parse_trade_history(runner: WindowRunner, rows_by_side: dict[str, list[dict[str, Any]]], *, cfg: KngtopConfig) -> PositionState:
    del cfg
    pos = PositionState()
    seen: set[str] = set()
    for side, rows in rows_by_side.items():
        for row in rows:
            oid = _extract_text(row, "orderId", "orderID", "order_id", "id", "transactionHash", "transaction_hash")
            dedupe_key = f"{side}:{oid}" if oid else f"{side}:{row}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            price = _extract_numeric(row, "price", "matchedPrice", "matchPrice", "avgPrice")
            size = _extract_numeric(row, "size", "amount", "shares", "matchedAmount")
            if price is None or size is None or price <= 0.0 or size <= 0.0:
                continue
            if side == "UP":
                pos.shares_up += size
                pos.spent_up += size * price
            else:
                pos.shares_down += size
                pos.spent_down += size * price
    return pos


def _refresh_trade_history(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    rows_by_side: dict[str, list[dict[str, Any]]] | None = None,
    now_ts: float | None = None,
) -> PositionState:
    now = datetime.now(timezone.utc).timestamp() if now_ts is None else float(now_ts)
    if rows_by_side is None:
        if clob is None:
            return runner.trade_history_positions
        if now - runner.last_trade_history_fetch_ts < 2.0:
            return runner.trade_history_positions
        start_sec = runner.start_sec()
        after_ts = int(max(0.0, (start_sec or int(now)) - TRADE_HISTORY_LOOKBACK_SEC))
        rows_by_side = {}
        for side in ("UP", "DOWN"):
            try:
                rows_by_side[side] = clob.get_recent_trades(_token_for_side(runner, side), after_ts=after_ts)
            except Exception as exc:  # noqa: BLE001
                _log_tag("TRADE HISTORY", slug=runner.contract.slug, side=side, status="error", error=str(exc))
                rows_by_side[side] = []
        runner.last_trade_history_fetch_ts = now
    runner.fill_history_seen_ids.update(_trade_history_fill_ids(rows_by_side))
    runner.trade_history_positions = _parse_trade_history(runner, rows_by_side, cfg=cfg)
    return runner.trade_history_positions


def _is_bot_owned_open_order(runner: WindowRunner, order_id: str, client_order_id: str | None) -> bool:
    if client_order_id and client_order_id.startswith("kngtop-c47-"):
        return True
    for pending in _pending_orders(runner, unresolved_only=False):
        if pending.exchange_order_id and pending.exchange_order_id == order_id:
            return True
        if client_order_id and pending.client_order_id == client_order_id:
            return True
    return False


def _parse_open_orders(runner: WindowRunner, rows: list[dict[str, Any]]) -> dict[str, list[OpenOrder]]:
    out: dict[str, list[OpenOrder]] = {"UP": [], "DOWN": []}
    for row in rows:
        asset = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
        side = "UP" if asset == runner.contract.up.token_id else "DOWN" if asset == runner.contract.down.token_id else None
        if side is None:
            continue
        raw_side = str(row.get("side") or "").strip().upper()
        if raw_side and raw_side != "BUY":
            continue
        oid = _extract_order_id(row)
        price = _extract_numeric(row, "price") or 0.0
        remaining = _extract_numeric(row, "size_left", "remaining")
        if remaining is None:
            original = _extract_numeric(row, "original_size", "size") or 0.0
            matched = _extract_numeric(row, "size_matched", "matched_size", "filled_size") or 0.0
            remaining = max(0.0, original - matched) if matched > 0.0 else original
        client_order_id = _extract_client_order_id(row)
        if oid and price > 0.0 and remaining > 1e-12:
            out[side].append(
                OpenOrder(
                    order_id=oid,
                    side=side,
                    price=price,
                    remaining_shares=remaining,
                    client_order_id=client_order_id,
                    bot_owned=_is_bot_owned_open_order(runner, oid, client_order_id),
                )
            )
    return out


def _sync_open_orders(runner: WindowRunner, *, clob: KngtopClob | None, rows: list[dict[str, Any]] | None = None) -> dict[str, list[OpenOrder]]:
    if clob is None and rows is None:
        return runner.open_orders
    open_rows = rows if rows is not None else (clob.get_open_orders() if clob is not None else [])
    runner.open_orders = _parse_open_orders(runner, list(open_rows))
    return runner.open_orders


def _trade_history_fill_ids(rows_by_side: dict[str, list[dict[str, Any]]]) -> set[str]:
    out: set[str] = set()
    for side, rows in rows_by_side.items():
        for row in rows:
            oid = _extract_text(row, "orderId", "orderID", "order_id", "id", "transactionHash", "transaction_hash")
            if oid:
                out.add(f"{side}:{oid}")
    return out


def _order_matches_pending(open_order: OpenOrder, pending: LocalPendingOrder) -> bool:
    if pending.exchange_order_id and open_order.order_id == pending.exchange_order_id:
        return True
    if open_order.client_order_id and open_order.client_order_id == pending.client_order_id:
        return True
    return (
        open_order.side == pending.side
        and abs(float(open_order.price) - float(pending.price)) <= REPRICE_TOLERANCE
        and open_order.remaining_shares <= pending.shares + 1e-9
    )


def _trade_history_confirms_pending(runner: WindowRunner, pending: LocalPendingOrder) -> bool:
    keys = {f"{pending.side}:{pending.client_order_id}"}
    if pending.exchange_order_id:
        keys.add(f"{pending.side}:{pending.exchange_order_id}")
    return bool(keys & runner.fill_history_seen_ids)


def _position_confirms_pending_fill(runner: WindowRunner, pending: LocalPendingOrder) -> bool:
    return runner.positions.shares(pending.side) >= pending.position_shares_at_send + pending.shares - 1e-9


def _reconcile_local_state(runner: WindowRunner, *, now_ts: float) -> None:
    detected_open_orders = 0
    resolved_pending = 0
    unknown_pending = 0
    detected_fills = 0
    open_by_id = {order.order_id: order for orders in runner.open_orders.values() for order in orders}

    for side in ("UP", "DOWN"):
        for pending in runner.local_pending_orders_by_side.get(side, []):
            if pending.resolved:
                continue
            pending.observed_refreshes += 1
            matching_open = next((order for order in runner.open_orders.get(side, []) if _order_matches_pending(order, pending)), None)
            fill_confirmed = _trade_history_confirms_pending(runner, pending) or _position_confirms_pending_fill(runner, pending)
            if matching_open is not None:
                pending.exchange_order_id = matching_open.order_id
                pending.status = "OPEN_CONFIRMED"
                matching_open.bot_owned = True
                detected_open_orders += 1
                continue
            if fill_confirmed:
                pending.status = "FILL_CONFIRMED"
                pending.resolved = True
                resolved_pending += 1
                detected_fills += 1
                continue
            pending.status = "UNKNOWN" if pending.status in {"SENT_LOCAL", "UNKNOWN"} else pending.status
            unknown_pending += 1
            if now_ts - pending.sent_at >= UNKNOWN_ORDER_TIMEOUT_SECONDS and pending.observed_refreshes >= 2:
                pending.status = "FAILED"
                pending.resolved = True
                resolved_pending += 1
                unknown_pending -= 1

    for order_id, cancel in list(runner.local_pending_cancels_by_order_id.items()):
        if cancel.resolved:
            continue
        if order_id not in open_by_id:
            cancel.status = "CANCEL_CONFIRMED"
            cancel.resolved = True
            resolved_pending += 1
        else:
            cancel.status = "UNKNOWN" if now_ts - cancel.sent_at >= ACTION_COOLDOWN_SECONDS else "SENT_LOCAL"
            unknown_pending += 1

    if runner.initial_batch_sent:
        runner.initial_batch_resolved = not _pending_orders(runner)

    runner.last_reconcile_summary = {
        "detected_fills": detected_fills,
        "detected_open_orders": detected_open_orders,
        "resolved_pending": resolved_pending,
        "unknown_pending": max(0, unknown_pending),
    }
    _log_tag(
        "RECONCILE",
        market_id=runner.market_id(),
        detected_fills=str(detected_fills),
        detected_open_orders=str(detected_open_orders),
        resolved_pending=str(resolved_pending),
        unknown_pending=str(max(0, unknown_pending)),
    )


def _projected_avg_sum(pos: PositionState, side: str, price: float) -> float:
    up_spent, up_shares = pos.spent_up, pos.shares_up
    down_spent, down_shares = pos.spent_down, pos.shares_down
    if side == "UP":
        up_spent += ORDER_SHARES * price
        up_shares += ORDER_SHARES
    else:
        down_spent += ORDER_SHARES * price
        down_shares += ORDER_SHARES
    up_avg = up_spent / up_shares if up_shares > 1e-12 else 0.0
    down_avg = down_spent / down_shares if down_shares > 1e-12 else 0.0
    return up_avg + down_avg


def _projected_worst_case_pnl(pos: PositionState, side: str, price: float) -> float:
    spent_total = pos.spent_total() + ORDER_SHARES * price
    up_shares = pos.shares_up + (ORDER_SHARES if side == "UP" else 0.0)
    down_shares = pos.shares_down + (ORDER_SHARES if side == "DOWN" else 0.0)
    return min(up_shares - spent_total, down_shares - spent_total)


def _open_order_shares(runner: WindowRunner, side: str) -> float:
    return sum(max(0.0, order.remaining_shares) for order in runner.open_orders.get(side, []))


def _effective_side_exposure(runner: WindowRunner, side: str) -> float:
    return runner.positions.shares(side) + _open_order_shares(runner, side) + _pending_order_shares(runner, side)


def _avg_sum_max_price(pos: PositionState, side: str) -> float:
    other = "DOWN" if side == "UP" else "UP"
    allowed_side_avg = AVG_SUM_CAP - pos.avg(other)
    if allowed_side_avg <= 0.0:
        return 0.0
    spent = pos.spent_up if side == "UP" else pos.spent_down
    shares = pos.shares_up if side == "UP" else pos.shares_down
    return (allowed_side_avg * (shares + ORDER_SHARES) - spent) / ORDER_SHARES


def _can_place_side_order(runner: WindowRunner, side: str, price: float, cfg: KngtopConfig, *, enforce_avg_cap: bool = True) -> bool:
    pos = runner.positions
    if _effective_side_exposure(runner, side) + ORDER_SHARES > float(cfg.max_shares_per_side) + 1e-12:
        return False
    if pos.spent_total() + price * ORDER_SHARES > MAX_SPENT_PER_WINDOW + 1e-12:
        return False
    if price * ORDER_SHARES + 1e-12 < MIN_ORDER_USD:
        return False
    if enforce_avg_cap and _projected_avg_sum(pos, side, price) > AVG_SUM_CAP + 1e-12:
        return False
    return True


def _c_target_prices(runner: WindowRunner, *, up_ask: float | None, down_ask: float | None, cfg: KngtopConfig) -> dict[str, float | None]:
    pos = runner.positions
    asks = {"UP": up_ask, "DOWN": down_ask}
    targets: dict[str, float | None] = {"UP": None, "DOWN": None}

    def clean_price(side: str) -> float | None:
        raw = asks.get(side)
        if raw is None or raw <= 0.0:
            return None
        return round(max(0.01, min(0.99, float(raw))), 2)

    if not pos.has_side("UP") and not pos.has_side("DOWN"):
        for side in ("UP", "DOWN"):
            price = INITIAL_PAIR_PRICE
            if _can_place_side_order(runner, side, price, cfg):
                targets[side] = price
        return targets

    if pos.has_side("UP") != pos.has_side("DOWN"):
        side = "DOWN" if pos.has_side("UP") else "UP"
        price = min(clean_price(side) or C_MISSING_HEDGE_CAP, C_MISSING_HEDGE_CAP)
        if _can_place_side_order(runner, side, price, cfg, enforce_avg_cap=False):
            targets[side] = price
        return targets

    smaller_side: str | None
    if pos.shares_up < pos.shares_down - C_BALANCE_EPSILON:
        smaller_side = "UP"
    elif pos.shares_down < pos.shares_up - C_BALANCE_EPSILON:
        smaller_side = "DOWN"
    else:
        smaller_side = None

    def repair_price(side: str) -> float | None:
        side_avg = pos.avg(side)
        if side_avg <= AVG_IMPROVE_BUFFER + 1e-12:
            return None
        cap = side_avg - AVG_IMPROVE_BUFFER
        if side == smaller_side:
            cap = max(cap, C_WEAK_CAP)
            if _projected_worst_case_pnl(pos, side, min(C_HIGH_GUARD, _avg_sum_max_price(pos, side))) > pos.worst_case_pnl() + 1e-12:
                cap = max(cap, C_HIGH_GUARD)
        cap = min(cap, _avg_sum_max_price(pos, side), 0.99)
        price = round(max(0.01, cap), 2)
        if not _can_place_side_order(runner, side, price, cfg):
            return None
        return price

    if smaller_side is not None:
        price = repair_price(smaller_side)
        if price is not None:
            targets[smaller_side] = price
        return targets

    for side in ("UP", "DOWN"):
        price = repair_price(side)
        if price is None:
            continue
        side_avg = pos.avg(side)
        if side_avg > 1e-12 and price <= side_avg - AVG_IMPROVE_BUFFER + 1e-12:
            targets[side] = price
    return targets


def _inside_opening_gate(elapsed: float) -> bool:
    return -PRESTART_SEC - 1e-12 <= elapsed <= OPENING_GRACE_SEC + 1e-12


def _projected_pair_avg_sum(pos: PositionState, up_price: float, down_price: float) -> float:
    up_shares = pos.shares_up + ORDER_SIZE_SHARES
    down_shares = pos.shares_down + ORDER_SIZE_SHARES
    up_avg = (pos.spent_up + ORDER_SIZE_SHARES * up_price) / up_shares if up_shares > 1e-12 else 0.0
    down_avg = (pos.spent_down + ORDER_SIZE_SHARES * down_price) / down_shares if down_shares > 1e-12 else 0.0
    return up_avg + down_avg


def _can_send_order_intent(runner: WindowRunner, side: str, price: float, shares: float, now_ts: float) -> tuple[bool, str | None]:
    if _pending_cancel_count(runner) > 0:
        return False, "pending_cancel_lock"
    if _pending_orders(runner, side):
        return False, "side_local_pending_lock"
    cache_key = _sent_cache_key(runner, side, price, shares)
    if cache_key in runner.recent_sent_cache:
        return False, "recent_sent_cache"
    state_type = _state_type(runner.positions)
    smaller = _smaller_side(runner.positions)
    if state_type == "IMBALANCED":
        if side != smaller:
            return False, "larger_side_while_imbalanced"
        if _live_order_count(runner) > 0:
            return False, "imbalanced_live_order_exists"
    if runner.positions.shares(side) + _open_order_shares(runner, side) + _pending_order_shares(runner, side) + shares > MAX_SHARES_PER_SIDE + 1e-12:
        return False, "max_shares_per_side"
    del now_ts
    return True, None


def _build_next_decision(
    runner: WindowRunner,
    *,
    elapsed: float,
    up_ask: float | None,
    down_ask: float | None,
    now_ts: float,
    cfg: KngtopConfig,
) -> StrategyDecision:
    state_type = _state_type(runner.positions)
    _log_tag(
        "STATE",
        market_id=runner.market_id(),
        up_shares=f"{runner.positions.shares_up:.2f}",
        up_avg=f"{runner.positions.avg('UP'):.4f}",
        down_shares=f"{runner.positions.shares_down:.2f}",
        down_avg=f"{runner.positions.avg('DOWN'):.4f}",
        open_up_orders=str(_open_order_count(runner, "UP")),
        open_down_orders=str(_open_order_count(runner, "DOWN")),
        pending_up_orders=str(len(_pending_orders(runner, "UP"))),
        pending_down_orders=str(len(_pending_orders(runner, "DOWN"))),
        pending_cancels=str(_pending_cancel_count(runner)),
    )

    if not runner.initial_batch_sent and _inside_opening_gate(elapsed):
        for side in ("UP", "DOWN"):
            ok, reason = _can_send_order_intent(runner, side, INITIAL_PRICE, ORDER_SIZE_SHARES, now_ts)
            if not ok:
                _log_tag("DANGER_BLOCK", market_id=runner.market_id(), side=side, reason=reason)
                return StrategyDecision("NONE", f"initial_blocked:{reason}")
        return StrategyDecision(
            "BATCH",
            "opening_gate_initial_batch",
            [("UP", INITIAL_PRICE, ORDER_SIZE_SHARES), ("DOWN", INITIAL_PRICE, ORDER_SIZE_SHARES)],
        )

    if state_type == "EMPTY":
        return StrategyDecision("NONE", "empty_outside_opening_gate")

    if state_type == "IMBALANCED":
        smaller = _smaller_side(runner.positions)
        larger = _opposite_side(smaller) if smaller else None
        if larger is not None:
            larger_bot_orders = [order for order in runner.open_orders.get(larger, []) if order.bot_owned]
            if larger_bot_orders:
                order = larger_bot_orders[0]
                return StrategyDecision("CANCEL", "larger_side_bot_order_while_imbalanced", cancel_order=order)
        if smaller is None:
            return StrategyDecision("NONE", "imbalanced_no_smaller_side")
        if _live_order_count(runner, smaller) > 0:
            return StrategyDecision("NONE", f"smaller_side_{smaller}_already_covered")
        if _live_order_count(runner) > 0:
            return StrategyDecision("NONE", "imbalanced_live_order_exists")
        raw_price = up_ask if smaller == "UP" else down_ask
        avg_sum_cap_price = _avg_sum_max_price(runner.positions, smaller)
        price = round(min(float(raw_price) if raw_price and raw_price > 0.0 else MAX_HEDGE_PRICE, MAX_HEDGE_PRICE, avg_sum_cap_price), 2)
        if price <= 0.0:
            return StrategyDecision("NONE", "hedge_avg_sum_cap")
        if _projected_avg_sum(runner.positions, smaller, price) > MAX_AVG_SUM_AFTER_BUY + 1e-12:
            return StrategyDecision("NONE", "hedge_avg_sum_cap")
        ok, reason = _can_send_order_intent(runner, smaller, price, ORDER_SIZE_SHARES, now_ts)
        if not ok:
            _log_tag("DANGER_BLOCK", market_id=runner.market_id(), side=smaller, reason=reason)
            return StrategyDecision("NONE", str(reason))
        return StrategyDecision("PLACE", f"imbalance_recovery_{smaller}", [(smaller, price, ORDER_SIZE_SHARES)])

    if runner.positions.shares_up >= MAX_SHARES_PER_SIDE - 1e-12 and runner.positions.shares_down >= MAX_SHARES_PER_SIDE - 1e-12:
        return StrategyDecision("NONE", "max_shares_reached")
    if _live_order_count(runner) > 0:
        return StrategyDecision("NONE", "balanced_live_orders_exist")
    if runner.positions.shares_up + ORDER_SIZE_SHARES > MAX_SHARES_PER_SIDE + 1e-12:
        return StrategyDecision("NONE", "up_max_shares_after_pair")
    if runner.positions.shares_down + ORDER_SIZE_SHARES > MAX_SHARES_PER_SIDE + 1e-12:
        return StrategyDecision("NONE", "down_max_shares_after_pair")

    desired = _c_target_prices(runner, up_ask=up_ask, down_ask=down_ask, cfg=cfg)
    up_price = desired["UP"]
    down_price = desired["DOWN"]
    if up_price is None or down_price is None:
        return StrategyDecision("NONE", "balanced_pair_no_valid_c_price")
    if _projected_pair_avg_sum(runner.positions, up_price, down_price) > MAX_AVG_SUM_AFTER_BUY + 1e-12:
        return StrategyDecision("NONE", "pair_avg_sum_cap")
    for side, price in (("UP", up_price), ("DOWN", down_price)):
        ok, reason = _can_send_order_intent(runner, side, price, ORDER_SIZE_SHARES, now_ts)
        if not ok:
            _log_tag("DANGER_BLOCK", market_id=runner.market_id(), side=side, reason=reason)
            return StrategyDecision("NONE", str(reason))
    return StrategyDecision("BATCH", "balanced_next_pair", [("UP", up_price, ORDER_SIZE_SHARES), ("DOWN", down_price, ORDER_SIZE_SHARES)])


def _record_local_pending_order(
    runner: WindowRunner,
    *,
    side: str,
    price: float,
    shares: float,
    now_ts: float,
) -> LocalPendingOrder:
    pending = LocalPendingOrder(
        client_order_id=_bot_client_order_id(runner, side),
        side=side,
        token_id=_token_for_side(runner, side).token_id,
        price=float(price),
        shares=float(shares),
        sent_at=float(now_ts),
        position_shares_at_send=runner.positions.shares(side),
    )
    runner.local_pending_orders_by_side[side].append(pending)
    runner.recent_sent_cache[_sent_cache_key(runner, side, price, shares)] = now_ts
    return pending


def _hard_pre_send_guard(runner: WindowRunner, *, side: str, price: float, shares: float, now_ts: float) -> tuple[bool, str | None]:
    del price, now_ts
    other = _opposite_side(side)
    side_effective = _effective_side_shares(runner, side)
    other_effective = _effective_side_shares(runner, other)
    if side_effective > other_effective + C_BALANCE_EPSILON:
        return False, "side_already_larger_effective"
    if side_effective + shares > MAX_SHARES_PER_SIDE + 1e-12:
        return False, "max_shares_per_side"
    if _pending_orders(runner, side):
        return False, "side_local_pending_lock"
    return True, None


def _place_one_order(runner: WindowRunner, *, clob: KngtopClob | None, side: str, price: float, shares: float, now_ts: float, reason: str) -> bool:
    ok, blocked_reason = _hard_pre_send_guard(runner, side=side, price=price, shares=shares, now_ts=now_ts)
    if not ok:
        _log_tag(
            "DANGER_BLOCK",
            market_id=runner.market_id(),
            side=side,
            price=f"{price:.2f}",
            shares=f"{shares:.2f}",
            reason=blocked_reason,
            decision_reason=reason,
        )
        return False
    pending = _record_local_pending_order(runner, side=side, price=price, shares=shares, now_ts=now_ts)
    _log_tag(
        "PLACE",
        market_id=runner.market_id(),
        side=side,
        price=f"{price:.2f}",
        shares=f"{shares:.2f}",
        reason=reason,
        client_order_id=pending.client_order_id,
    )
    if clob is None:
        pending.status = "OPEN_CONFIRMED"
        return True
    try:
        payload = clob.limit_buy_shares(_token_for_side(runner, side), price=float(price), shares=float(shares), post_only=True)
    except Exception as exc:  # noqa: BLE001
        pending.status = "UNKNOWN"
        _log_tag("PLACE", market_id=runner.market_id(), side=side, status="UNKNOWN", error=str(exc), client_order_id=pending.client_order_id)
        return True
    pending.exchange_order_id = _extract_order_id(payload)
    pending.status = "CONFIRMING"
    return True


def _cancel_one_order(runner: WindowRunner, *, clob: KngtopClob | None, order: OpenOrder, now_ts: float, reason: str) -> None:
    runner.local_pending_cancels_by_order_id[order.order_id] = LocalPendingCancel(
        order_id=order.order_id,
        side=order.side,
        price=order.price,
        shares=order.remaining_shares,
        sent_at=now_ts,
    )
    _log_tag(
        "CANCEL",
        market_id=runner.market_id(),
        order_id=order.order_id,
        side=order.side,
        price=f"{order.price:.2f}",
        shares=f"{order.remaining_shares:.2f}",
        reason=reason,
    )
    if clob is None:
        return
    try:
        clob.cancel_order_by_id(order.order_id)
    except Exception as exc:  # noqa: BLE001
        pending = runner.local_pending_cancels_by_order_id[order.order_id]
        pending.status = "UNKNOWN"
        _log_tag("CANCEL", market_id=runner.market_id(), order_id=order.order_id, status="UNKNOWN", error=str(exc))


def _execute_decision(runner: WindowRunner, *, clob: KngtopClob | None, decision: StrategyDecision, now_ts: float) -> bool:
    if decision.action == "NONE":
        _log_tag("SKIP", market_id=runner.market_id(), reason=decision.reason)
        return False
    if decision.action == "CANCEL" and decision.cancel_order is not None:
        _cancel_one_order(runner, clob=clob, order=decision.cancel_order, now_ts=now_ts, reason=decision.reason)
        return True
    if decision.action == "PLACE":
        side, price, shares = decision.orders[0]
        return _place_one_order(runner, clob=clob, side=side, price=price, shares=shares, now_ts=now_ts, reason=decision.reason)
    if decision.action == "BATCH":
        if decision.reason == "opening_gate_initial_batch":
            runner.initial_batch_sent = True
        sent_any = False
        for side, price, shares in decision.orders:
            sent_any = _place_one_order(runner, clob=clob, side=side, price=price, shares=shares, now_ts=now_ts, reason=decision.reason) or sent_any
        return sent_any
    return False


def _tick_runner(
    runner: WindowRunner,
    *,
    poly: MarketWsFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    if runner.stop_reason is not None:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    _prune_recent_sent_cache(runner, now_ts)
    elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
    if elapsed is None or remaining is None:
        return
    if remaining <= 0:
        return
    _log_tag(
        "WINDOW",
        market_id=runner.market_id(),
        start_time=str(runner.start_sec()),
        now=f"{now_ts:.3f}",
        elapsed=f"{elapsed:.1f}",
        remaining=f"{remaining:.1f}",
    )
    state = runtime_state if runtime_state is not None else {}
    api_errors: list[str] = []
    try:
        if state.get("trade_history_error"):
            raise RuntimeError(str(state.get("trade_history_error")))
        history_rows = state.get("reconcile_trade_history")
        if isinstance(history_rows, dict):
            _refresh_trade_history(runner, clob=clob, cfg=cfg, rows_by_side={str(k): list(v) for k, v in history_rows.items() if isinstance(v, list)}, now_ts=now_ts)
        elif clob is not None:
            _refresh_trade_history(runner, clob=clob, cfg=cfg, now_ts=now_ts)
    except Exception as exc:  # noqa: BLE001
        api_errors.append("trade_history")
        _log_tag("RECONCILE", market_id=runner.market_id(), source="trade_history", status="error", error=str(exc))
    try:
        if state.get("positions_error"):
            raise RuntimeError(str(state.get("positions_error")))
        rows = state.get("reconcile_positions")
        _refresh_positions(runner, cfg=cfg, rows=list(rows) if isinstance(rows, list) else None)
    except Exception as exc:  # noqa: BLE001
        api_errors.append("positions")
        _log_tag("RECONCILE", market_id=runner.market_id(), source="positions", status="error", error=str(exc))
    try:
        if state.get("open_orders_error"):
            raise RuntimeError(str(state.get("open_orders_error")))
        if clob is not None:
            _sync_open_orders(runner, clob=clob)
        else:
            open_rows = state.get("reconcile_open_orders")
            _sync_open_orders(runner, clob=clob, rows=list(open_rows) if isinstance(open_rows, list) else None)
    except Exception as exc:  # noqa: BLE001
        api_errors.append("open_orders")
        _log_tag("RECONCILE", market_id=runner.market_id(), source="open_orders", status="error", error=str(exc))
    _reconcile_local_state(runner, now_ts=now_ts)
    if elapsed < -PRESTART_SEC - 1e-12:
        _log_tag("SKIP", market_id=runner.market_id(), reason="before_opening_gate")
        return
    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    state_type = _state_type(runner.positions)
    if api_errors:
        _log_tag("DANGER_BLOCK", market_id=runner.market_id(), reason="api_uncertainty", sources=",".join(api_errors))
        _log_tag("DECISION", market_id=runner.market_id(), state_type=state_type, allowed_action="NONE", blocked_reason="api_uncertainty")
        return
    if now_ts < runner.action_lock_until:
        _log_tag("DECISION", market_id=runner.market_id(), state_type=state_type, allowed_action="NONE", blocked_reason="action_lock")
        return
    if _has_unresolved_local_action(runner):
        _log_tag("DECISION", market_id=runner.market_id(), state_type=state_type, allowed_action="NONE", blocked_reason="local_pending_lock")
        return
    if now_ts < runner.last_action_time + ACTION_COOLDOWN_SECONDS:
        _log_tag("DECISION", market_id=runner.market_id(), state_type=state_type, allowed_action="NONE", blocked_reason="action_cooldown")
        return
    decision = _build_next_decision(
        runner,
        elapsed=elapsed,
        up_ask=up_quote[1] if up_quote else None,
        down_ask=down_quote[1] if down_quote else None,
        now_ts=now_ts,
        cfg=cfg,
    )
    _log_tag(
        "DECISION",
        market_id=runner.market_id(),
        state_type=state_type,
        reason=decision.reason,
        allowed_action=decision.action,
        blocked_reason=decision.reason if decision.action == "NONE" else None,
    )
    if _execute_decision(runner, clob=clob, decision=decision, now_ts=now_ts):
        runner.last_action_time = now_ts
        runner.action_lock_until = now_ts + ACTION_COOLDOWN_SECONDS


def _discover_target_windows(cfg: KngtopConfig, *, runners: dict[int, WindowRunner], binance_symbol: str) -> None:
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
        runners[start_sec] = WindowRunner(
            pair_key=TRADE_PAIR_KEY,
            binance_symbol=binance_symbol,
            contract=contract,
            window_minutes=TRADE_WINDOW_MINUTES,
            window_open_px=fetch_binance_window_open_px(
                symbol=binance_symbol,
                window_start_sec=start_sec,
                window_minutes=TRADE_WINDOW_MINUTES,
                timeout=cfg.request_timeout_sec,
            ),
        )
        _log_tag("INIT", slug=contract.slug, start_sec=str(start_sec), strategy="balanced_fixed_47c")


def _refresh_subscriptions(*, runners: dict[int, WindowRunner], poly: MarketWsFeed) -> None:
    asset_ids: list[str] = []
    for runner in runners.values():
        asset_ids.extend([runner.contract.up.token_id, runner.contract.down.token_id])
    poly.set_assets(asset_ids)


def _purge_finished_windows(*, runners: dict[int, WindowRunner]) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    for start_sec, runner in list(runners.items()):
        _elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
        if remaining is not None and remaining <= 0:
            runners.pop(start_sec, None)


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    poly: MarketWsFeed,
    clob: KngtopClob | None,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    state = runtime_state if runtime_state is not None else {}
    state["runners"] = runners
    binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    _discover_target_windows(cfg, runners=runners, binance_symbol=binance_symbol)
    _refresh_subscriptions(runners=runners, poly=poly)
    for runner in list(runners.values()):
        try:
            _tick_runner(runner, poly=poly, clob=clob, cfg=cfg, runtime_state=state)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", slug=runner.contract.slug, stage="tick", error=str(exc))
    _purge_finished_windows(runners=runners)


def _reconcile_loop(
    stop: threading.Event,
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any],
    on_update: Any,
) -> None:
    while not stop.wait(1.0):
        if cfg.dry_run or clob is None:
            continue
        try:
            runtime_state["reconcile_positions"] = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
            runtime_state["reconcile_open_orders"] = clob.get_open_orders()
            runners = runtime_state.get("runners")
            if isinstance(runners, dict):
                now_ts = datetime.now(timezone.utc).timestamp()
                for runner in list(runners.values()):
                    if isinstance(runner, WindowRunner):
                        _refresh_trade_history(runner, clob=clob, cfg=cfg, now_ts=now_ts)
            on_update()
        except Exception as exc:  # noqa: BLE001
            _log_tag("RECONCILE", status="error", error=str(exc))


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    coord = EvalCoordinator(debounce_sec=0.0, heartbeat_sec=cfg.poll_interval_sec)
    runtime_state: dict[str, Any] = {"runners": {}}
    poly = MarketWsFeed(on_quote_update=coord.notify)
    poly.start()
    rest_poll_stop = threading.Event()
    binance = BinanceCombinedTradeFeed([binance_symbol], on_trade=lambda _symbol: None)
    if cfg.ws_rest_poll_enabled:
        threading.Thread(target=run_ws_rest_fallback_loop, args=(rest_poll_stop, cfg, binance, poly), name="ws-rest-fallback", daemon=True).start()
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
        threading.Thread(target=_reconcile_loop, args=(threading.Event(),), kwargs={"clob": clob, "cfg": cfg, "runtime_state": runtime_state, "on_update": coord.notify}, name="limit-reconcile", daemon=True).start()
    runners: dict[int, WindowRunner] = {}
    runtime_state["runners"] = runners
    _log_tag(
        "INIT",
        pair=TRADE_PAIR_KEY,
        window_minutes=str(TRADE_WINDOW_MINUTES),
        strategy="C_Strategy_Balanced_Fixed_47c",
        order_shares=f"{ORDER_SHARES:.2f}",
        max_shares_per_side=f"{cfg.max_shares_per_side:.2f}",
    )
    while True:
        coord.wait_for_turn()
        _run_iteration(cfg, runners=runners, poly=poly, clob=clob, runtime_state=runtime_state)


if __name__ == "__main__":
    main()
