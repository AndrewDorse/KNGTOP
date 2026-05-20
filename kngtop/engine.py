"""BTC 5m balanced two-sided maker driven by websocket updates plus 0.2s heartbeat."""

from __future__ import annotations

import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
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
MIN_REMAINING_SEC = 30
STOP_NEW_BUYS_ELAPSED_SEC = 270
BASE_ORDER_SHARES = 5.0
PRICE_STEP = 0.01
MIN_BUY_PRICE = 0.35
MAX_BUY_PRICE = 0.65
MAX_TOTAL_EXPOSURE_PER_WINDOW = 30.0
MAX_ONE_SIDE_EXPOSURE = 20.0
MAX_OPEN_BUY_ORDERS = 2
MAX_OPEN_BUY_ORDERS_PER_SIDE = 1
MAX_IMBALANCE_SHARES = 5.0
DISCOVERY_RETRY_SEC_WHEN_MISSING = 2.0
RECONCILE_COOLDOWN_SEC = 2.0
FAST_RECONCILE_AFTER_ACTION_SEC = 0.75
SENT_ORDER_CACHE_SEC = 10.0
CANCEL_REPLACE_COOLDOWN_SEC = 5.0
SAME_SIDE_ORDER_COOLDOWN_SEC = 5.0
WS_UPDATE_LOG_COOLDOWN_SEC = 1.0


def _side_float_map() -> dict[str, float]:
    return {"UP": 0.0, "DOWN": 0.0}


def _side_orders_map() -> dict[str, list["ManagedOrder"]]:
    return {"UP": [], "DOWN": []}


def _side_time_map() -> dict[str, float]:
    return {"UP": 0.0, "DOWN": 0.0}


@dataclass(slots=True)
class ManagedOrder:
    order_id: str
    side: str
    token_id: str
    price: float
    original_size: float
    remaining_size: float
    matched_size: float
    first_seen_monotonic: float

    def age_sec(self, now_monotonic: float) -> float:
        return max(0.0, float(now_monotonic) - float(self.first_seen_monotonic))


@dataclass
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    stopped: bool = False
    stop_reason: str | None = None
    filled_shares: dict[str, float] = field(default_factory=_side_float_map)
    filled_cost: dict[str, float] = field(default_factory=_side_float_map)
    open_orders: dict[str, list[ManagedOrder]] = field(default_factory=_side_orders_map)
    order_first_seen: dict[str, float] = field(default_factory=dict)
    sent_order_cache: dict[str, float] = field(default_factory=dict)
    last_place_monotonic: dict[str, float] = field(default_factory=_side_time_map)
    last_cancel_monotonic: dict[str, float] = field(default_factory=_side_time_map)
    last_reconcile_monotonic: float = 0.0
    reconcile_after_action_monotonic: float = 0.0
    force_reconcile: bool = True
    _exec_lock: threading.Lock = field(default_factory=threading.Lock)

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)

    def avg_price(self, side: str) -> float | None:
        shares = float(self.filled_shares.get(side, 0.0))
        if shares <= 0:
            return None
        return float(self.filled_cost.get(side, 0.0)) / shares

    def side_exposure(self, side: str) -> float:
        filled = float(self.filled_cost.get(side, 0.0))
        open_exposure = sum(max(0.0, o.remaining_size) * max(0.0, o.price) for o in self.open_orders.get(side, []))
        return filled + open_exposure

    def total_exposure(self) -> float:
        return self.side_exposure("UP") + self.side_exposure("DOWN")

    def imbalance_shares(self) -> float:
        return float(self.filled_shares.get("UP", 0.0)) - float(self.filled_shares.get("DOWN", 0.0))

    def open_order_count(self, side: str | None = None) -> int:
        if side is None:
            return sum(len(rows) for rows in self.open_orders.values())
        return len(self.open_orders.get(side, []))


@dataclass
class DiscoveryState:
    last_checked_monotonic: float = 0.0


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lv,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    for noisy_name in (
        "httpx",
        "httpcore",
        "websocket",
        "websocket-client",
        "py_clob_client_v2",
        "py_clob_client_v2.http_helpers.helpers",
    ):
        noisy = logging.getLogger(noisy_name)
        noisy.setLevel(logging.CRITICAL)
        noisy.propagate = False


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


def _event(kind: str, **fields: object) -> None:
    _log_tag(kind, **fields)


def _ws_reconnected_event(feed: str, downtime_sec: float) -> None:
    _log_tag("WS UPDATE", feed=feed, event="reconnected", downtime_sec=f"{downtime_sec:.3f}")


def _log_ws_update(runtime_state: dict[str, Any], *, feed: str, symbol: str | None = None) -> None:
    now_monotonic = time.perf_counter()
    gate_key = f"ws_log_not_before:{feed}:{symbol or '-'}"
    if now_monotonic < float(runtime_state.get(gate_key, 0.0)):
        return
    runtime_state[gate_key] = now_monotonic + WS_UPDATE_LOG_COOLDOWN_SEC
    _log_tag("WS UPDATE", feed=feed, symbol=symbol or "-")


def _extract_order_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("orderID", "orderId", "id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _extract_numeric(payload: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        raw = payload.get(key)
        try:
            if raw is not None and raw != "":
                return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _normalize_share_qty(raw: float | None) -> float | None:
    if raw is None:
        return None
    qty = float(raw)
    if float(qty).is_integer() and abs(qty) >= 100_000:
        return qty / 1_000_000.0
    return qty


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


def _runner_needs_reconcile(runner: WindowRunner, now_monotonic: float) -> bool:
    if runner.force_reconcile:
        return True
    if runner.reconcile_after_action_monotonic > 0 and now_monotonic >= runner.reconcile_after_action_monotonic:
        return True
    return (now_monotonic - runner.last_reconcile_monotonic) >= RECONCILE_COOLDOWN_SEC


def _compute_limit_buy_price(*, best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    target = float(best_bid) + PRICE_STEP
    target = min(max(target, MIN_BUY_PRICE), MAX_BUY_PRICE)
    if target >= float(best_ask):
        target = float(best_ask) - PRICE_STEP
    target = round(target, 2)
    if target <= 0 or target >= 1:
        return None
    if target < MIN_BUY_PRICE or target > MAX_BUY_PRICE:
        return None
    if target >= float(best_ask):
        return None
    return target


def _desired_sides(imbalance_shares: float) -> tuple[str, ...]:
    if imbalance_shares > 0:
        return ("DOWN",)
    if imbalance_shares < 0:
        return ("UP",)
    return ("UP", "DOWN")


def _prune_sent_order_cache(runner: WindowRunner, now_monotonic: float) -> None:
    runner.sent_order_cache = {
        key: ts for key, ts in runner.sent_order_cache.items() if (now_monotonic - float(ts)) < SENT_ORDER_CACHE_SEC
    }


def _parse_order_rows(
    *,
    rows: list[dict[str, Any]],
    token: TokenMarket,
    side: str,
    order_first_seen: dict[str, float],
    now_monotonic: float,
) -> list[ManagedOrder]:
    parsed: list[ManagedOrder] = []
    token_id = str(token.token_id)
    for row in rows:
        asset_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or "")
        if asset_id and asset_id != token_id:
            continue
        raw_side = str(row.get("side") or row.get("order_side") or "").strip().upper()
        if raw_side and raw_side != "BUY":
            continue
        order_id = _extract_order_id(row)
        price = _extract_numeric(row, "price")
        if not order_id or price is None:
            continue
        original_size = _normalize_share_qty(
            _extract_numeric(row, "original_size", "size", "makerAmount", "amount")
        )
        matched_size = _normalize_share_qty(
            _extract_numeric(row, "size_matched", "matched_amount", "filled_amount", "filled", "makerAmountFilled")
        )
        remaining_size = _normalize_share_qty(
            _extract_numeric(row, "size_left", "remaining", "remaining_amount", "size_remaining", "makerAmountRemaining")
        )
        matched_size = max(0.0, float(matched_size or 0.0))
        if remaining_size is None and original_size is not None:
            remaining_size = max(0.0, float(original_size) - matched_size)
        if original_size is None and remaining_size is not None:
            original_size = max(0.0, float(remaining_size) + matched_size)
        if original_size is None:
            continue
        remaining = max(0.0, float(remaining_size or 0.0))
        if remaining <= 0:
            continue
        first_seen = float(order_first_seen.get(order_id, now_monotonic))
        order_first_seen[order_id] = first_seen
        parsed.append(
            ManagedOrder(
                order_id=order_id,
                side=side,
                token_id=token_id,
                price=float(price),
                original_size=float(original_size),
                remaining_size=remaining,
                matched_size=matched_size,
                first_seen_monotonic=first_seen,
            )
        )
    parsed.sort(key=lambda order: (order.first_seen_monotonic, order.order_id))
    return parsed


def _apply_reconcile_snapshot(
    runner: WindowRunner,
    *,
    open_order_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
    now_monotonic: float,
) -> None:
    runner.open_orders["UP"] = _parse_order_rows(
        rows=open_order_rows,
        token=runner.contract.up,
        side="UP",
        order_first_seen=runner.order_first_seen,
        now_monotonic=now_monotonic,
    )
    runner.open_orders["DOWN"] = _parse_order_rows(
        rows=open_order_rows,
        token=runner.contract.down,
        side="DOWN",
        order_first_seen=runner.order_first_seen,
        now_monotonic=now_monotonic,
    )
    live_order_ids = {order.order_id for orders in runner.open_orders.values() for order in orders}
    stale_ids = [oid for oid in runner.order_first_seen if oid not in live_order_ids]
    for oid in stale_ids:
        runner.order_first_seen.pop(oid, None)
    for side, token in (("UP", runner.contract.up), ("DOWN", runner.contract.down)):
        shares = 0.0
        cost = 0.0
        for row in position_rows:
            slug = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
            asset_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "")
            outcome = str(row.get("outcome") or "").strip().upper()
            if slug and slug != runner.contract.slug:
                continue
            if asset_id and asset_id != token.token_id and outcome != side:
                continue
            if outcome and outcome != side and asset_id != token.token_id:
                continue
            size = _normalize_share_qty(_extract_numeric(row, "size", "amount", "shares"))
            avg_price = _extract_numeric(row, "avgPrice", "averagePrice", "avg_price", "price")
            if size is None or size <= 0:
                continue
            shares += float(size)
            if avg_price is not None and 0 < float(avg_price) < 1:
                cost += float(size) * float(avg_price)
        runner.filled_shares[side] = shares
        runner.filled_cost[side] = cost
    runner.last_reconcile_monotonic = now_monotonic
    runner.reconcile_after_action_monotonic = 0.0
    runner.force_reconcile = False
    runner.sent_order_cache.clear()
    _log_tag(
        "POSITION",
        slug=runner.contract.slug,
        up_shares=f"{runner.filled_shares['UP']:.2f}",
        down_shares=f"{runner.filled_shares['DOWN']:.2f}",
        avg_up="-" if runner.avg_price("UP") is None else f"{runner.avg_price('UP'):.4f}",
        avg_down="-" if runner.avg_price("DOWN") is None else f"{runner.avg_price('DOWN'):.4f}",
    )
    _log_tag(
        "OPEN ORDERS",
        slug=runner.contract.slug,
        up_count=str(len(runner.open_orders["UP"])),
        down_count=str(len(runner.open_orders["DOWN"])),
        total_count=str(runner.open_order_count()),
    )


def _refresh_global_reconcile_cache(
    *,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any],
) -> None:
    if clob is None:
        return
    open_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    try:
        open_rows = clob.get_open_orders()
        position_rows = fetch_user_positions(user=cfg.funder, timeout=cfg.request_timeout_sec)
    except Exception as exc:  # noqa: BLE001
        _log_tag("RECONCILE", scope="global", status="error", error=str(exc))
        return
    runtime_state["reconcile_cache_at"] = time.perf_counter()
    runtime_state["reconcile_open_orders"] = open_rows
    runtime_state["reconcile_positions"] = position_rows
    _log_tag(
        "RECONCILE",
        scope="global",
        status="ok",
        open_orders=str(len(open_rows)),
        positions=str(len(position_rows)),
    )


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


def _cancel_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    order: ManagedOrder,
    reason: str,
) -> bool:
    if clob is None:
        _log_tag("CANCEL", slug=runner.contract.slug, side=order.side, order_id=order.order_id, reason=f"{reason}:dry_run")
        return False
    try:
        clob.cancel_order_by_id(order.order_id)
    except Exception as exc:  # noqa: BLE001
        _log_tag("CANCEL", slug=runner.contract.slug, side=order.side, order_id=order.order_id, reason=reason, error=str(exc))
        return False
    now_monotonic = time.perf_counter()
    runner.last_cancel_monotonic[order.side] = now_monotonic
    runner.force_reconcile = True
    runner.reconcile_after_action_monotonic = now_monotonic + FAST_RECONCILE_AFTER_ACTION_SEC
    _log_tag("CANCEL", slug=runner.contract.slug, side=order.side, order_id=order.order_id, reason=reason)
    return True


def _execute_buy(
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    shares: float,
    budget_usd: float,
    token: TokenMarket,
    label: str,
    *,
    start_px: float,
    spot_px: float,
    pm_trigger_px: float,
    limit_price: float,
    retry_on_error_override: int | None = None,
) -> tuple[bool, str | None, str | None]:
    del budget_usd, retry_on_error_override
    _event(
        "START_DEAL",
        label=label,
        shares=f"{float(shares):.2f}",
        start_px=f"{float(start_px):.10f}",
        spot_px=f"{float(spot_px):.10f}",
        pm_trigger_px=f"{float(pm_trigger_px):.10f}",
        limit_px=f"{float(limit_price):.2f}",
    )
    if cfg.dry_run or clob is None:
        return True, None, None
    try:
        payload = clob.limit_buy_shares(token, price=float(limit_price), shares=float(shares))
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        reason = "insufficient_balance" if "not enough balance" in msg.lower() else "error"
        _event("RETRY_BUY", label=label, reason=reason)
        return False, reason, None
    return True, None, _extract_order_id(payload)


def _place_limit_buy(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    token: TokenMarket,
    side: str,
    price: float,
) -> bool:
    if clob is None:
        _log_tag("LIMIT BUY", slug=runner.contract.slug, side=side, price=f"{price:.2f}", shares=f"{BASE_ORDER_SHARES:.2f}", mode="dry_run")
        return False
    try:
        payload = clob.limit_buy_shares(token, price=float(price), shares=float(BASE_ORDER_SHARES))
    except Exception as exc:  # noqa: BLE001
        _log_tag("LIMIT BUY", slug=runner.contract.slug, side=side, price=f"{price:.2f}", shares=f"{BASE_ORDER_SHARES:.2f}", status="error", error=str(exc))
        return False
    order_id = _extract_order_id(payload) or "unknown"
    now_monotonic = time.perf_counter()
    runner.last_place_monotonic[side] = now_monotonic
    runner.sent_order_cache[f"{side}:{price:.2f}"] = now_monotonic
    runner.force_reconcile = True
    runner.reconcile_after_action_monotonic = now_monotonic + FAST_RECONCILE_AFTER_ACTION_SEC
    _log_tag("LIMIT BUY", slug=runner.contract.slug, side=side, price=f"{price:.2f}", shares=f"{BASE_ORDER_SHARES:.2f}", order_id=order_id)
    return True


def _maybe_cancel_orders(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    side: str,
    keep: int,
    reason: str,
    now_monotonic: float,
) -> None:
    rows = list(runner.open_orders.get(side, []))
    if len(rows) <= keep:
        return
    for order in rows[keep:]:
        if (now_monotonic - runner.last_cancel_monotonic[side]) < CANCEL_REPLACE_COOLDOWN_SEC:
            _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason=f"{reason}:cancel_cooldown")
            return
        if order.age_sec(now_monotonic) < CANCEL_REPLACE_COOLDOWN_SEC:
            _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason=f"{reason}:order_young")
            continue
        if _cancel_order(runner, clob=clob, order=order, reason=reason):
            break


def _window_stop_check(runner: WindowRunner) -> tuple[bool, float | None]:
    avg_up = runner.avg_price("UP")
    avg_down = runner.avg_price("DOWN")
    if avg_up is None or avg_down is None:
        return False, None
    total = float(avg_up) + float(avg_down)
    return total <= 0.96, total


def _book_for_side(runner: WindowRunner, side: str, poly: MarketWsFeed, cfg: KngtopConfig) -> tuple[float | None, float | None]:
    token_id = runner.contract.up.token_id if side == "UP" else runner.contract.down.token_id
    quote = poly.best_bid_ask_for(token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    if quote is None:
        return None, None
    return float(quote[0]), float(quote[1])


def _maybe_place_side(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    poly: MarketWsFeed,
    cfg: KngtopConfig,
    side: str,
    now_monotonic: float,
) -> None:
    if runner.open_order_count() >= MAX_OPEN_BUY_ORDERS:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="max_open_orders")
        return
    if runner.open_order_count(side) >= MAX_OPEN_BUY_ORDERS_PER_SIDE:
        return
    if (now_monotonic - runner.last_place_monotonic[side]) < SAME_SIDE_ORDER_COOLDOWN_SEC:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="same_side_cooldown")
        return
    bid, ask = _book_for_side(runner, side, poly, cfg)
    _log_tag(
        "BOOK",
        slug=runner.contract.slug,
        side=side,
        bid="-" if bid is None else f"{bid:.2f}",
        ask="-" if ask is None else f"{ask:.2f}",
    )
    price = _compute_limit_buy_price(best_bid=bid, best_ask=ask)
    if price is None:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="invalid_book_price")
        return
    cache_key = f"{side}:{price:.2f}"
    if cache_key in runner.sent_order_cache:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="duplicate_cooldown", price=f"{price:.2f}")
        return
    side_exposure_if_placed = runner.side_exposure(side) + (BASE_ORDER_SHARES * price)
    total_exposure_if_placed = runner.total_exposure() + (BASE_ORDER_SHARES * price)
    if side_exposure_if_placed > MAX_ONE_SIDE_EXPOSURE:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="max_one_side_exposure")
        return
    if total_exposure_if_placed > MAX_TOTAL_EXPOSURE_PER_WINDOW:
        _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="max_total_exposure")
        return
    token = runner.contract.up if side == "UP" else runner.contract.down
    _place_limit_buy(runner, clob=clob, token=token, side=side, price=price)


def _maybe_replace_stale_order(
    runner: WindowRunner,
    *,
    clob: KngtopClob | None,
    poly: MarketWsFeed,
    cfg: KngtopConfig,
    side: str,
    now_monotonic: float,
) -> None:
    rows = runner.open_orders.get(side, [])
    if len(rows) != 1:
        return
    order = rows[0]
    bid, ask = _book_for_side(runner, side, poly, cfg)
    _log_tag(
        "BOOK",
        slug=runner.contract.slug,
        side=side,
        bid="-" if bid is None else f"{bid:.2f}",
        ask="-" if ask is None else f"{ask:.2f}",
    )
    target_price = _compute_limit_buy_price(best_bid=bid, best_ask=ask)
    if target_price is None:
        return
    if abs(float(order.price) - float(target_price)) < PRICE_STEP:
        return
    if order.age_sec(now_monotonic) < CANCEL_REPLACE_COOLDOWN_SEC:
        return
    if (now_monotonic - runner.last_cancel_monotonic[side]) < CANCEL_REPLACE_COOLDOWN_SEC:
        return
    _cancel_order(runner, clob=clob, order=order, reason=f"stale_price->{target_price:.2f}")


def _finalize_runner_window(
    runner: WindowRunner | None,
    *,
    binance: BinanceCombinedTradeFeed,
    cfg: KngtopConfig,
) -> None:
    del runner, binance, cfg
    return


def _tick_runner(
    runner: WindowRunner | None,
    *,
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, Any],
) -> None:
    if runner is None:
        return
    with runner._exec_lock:
        now = datetime.now(timezone.utc)
        now_ts = now.timestamp()
        now_monotonic = time.perf_counter()
        elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
        if elapsed is None or remaining is None:
            return
        _prune_sent_order_cache(runner, now_monotonic)
        spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
        if spot is None:
            _log_tag("SKIP BUY", slug=runner.contract.slug, reason="binance_stale")
            return
        _log_tag(
            "WINDOW",
            slug=runner.contract.slug,
            elapsed_sec=f"{elapsed:.1f}",
            remaining_sec=f"{remaining:.1f}",
            stopped=str(runner.stopped).lower(),
            btc_spot=f"{spot:.2f}",
        )
        if elapsed < 0:
            _log_tag("WINDOW", slug=runner.contract.slug, state="prestart_watch")
            return
        if _runner_needs_reconcile(runner, now_monotonic):
            cache_at = float(runtime_state.get("reconcile_cache_at", 0.0))
            if cache_at <= runner.last_reconcile_monotonic:
                _log_tag("SKIP BUY", slug=runner.contract.slug, reason="reconcile_pending")
                return
            open_rows = list(runtime_state.get("reconcile_open_orders", []))
            position_rows = _filtered_positions_for_runner(
                runner,
                list(runtime_state.get("reconcile_positions", [])),
            )
            _apply_reconcile_snapshot(
                runner,
                open_order_rows=open_rows,
                position_rows=position_rows,
                now_monotonic=cache_at,
            )
        imbalance = runner.imbalance_shares()
        _log_tag(
            "IMBALANCE",
            slug=runner.contract.slug,
            up_shares=f"{runner.filled_shares['UP']:.2f}",
            down_shares=f"{runner.filled_shares['DOWN']:.2f}",
            imbalance=f"{imbalance:.2f}",
        )
        stop_hit, avg_sum = _window_stop_check(runner)
        if stop_hit and not runner.stopped:
            runner.stopped = True
            runner.stop_reason = "avg_sum_le_0.96"
            _log_tag("WINDOW STOP", slug=runner.contract.slug, avg_sum=f"{avg_sum:.4f}")
        if runner.filled_shares["UP"] > 0 and runner.filled_shares["DOWN"] > 0 and abs(imbalance) < 1e-9:
            _log_tag(
                "BALANCED",
                slug=runner.contract.slug,
                avg_sum="-" if avg_sum is None else f"{avg_sum:.4f}",
                up_shares=f"{runner.filled_shares['UP']:.2f}",
                down_shares=f"{runner.filled_shares['DOWN']:.2f}",
            )
        if remaining <= MIN_REMAINING_SEC:
            _log_tag("LATE WINDOW", slug=runner.contract.slug, remaining_sec=f"{remaining:.1f}")
            for side in ("UP", "DOWN"):
                _maybe_cancel_orders(
                    runner,
                    clob=clob,
                    side=side,
                    keep=0,
                    reason="late_window",
                    now_monotonic=now_monotonic,
                )
            return
        if runner.stopped:
            for side in ("UP", "DOWN"):
                _maybe_cancel_orders(
                    runner,
                    clob=clob,
                    side=side,
                    keep=0,
                    reason="window_stop",
                    now_monotonic=now_monotonic,
                )
            return
        if elapsed >= STOP_NEW_BUYS_ELAPSED_SEC:
            _log_tag("LATE WINDOW", slug=runner.contract.slug, elapsed_sec=f"{elapsed:.1f}", reason="no_new_buys")
            return
        allowed_sides = set(_desired_sides(imbalance))
        if abs(imbalance) > MAX_IMBALANCE_SHARES:
            _log_tag("IMBALANCE", slug=runner.contract.slug, status="over_limit", max_allowed=f"{MAX_IMBALANCE_SHARES:.2f}")
        for side in ("UP", "DOWN"):
            keep = 1 if side in allowed_sides else 0
            reason = "forbidden_side" if keep == 0 else "extra_orders"
            _maybe_cancel_orders(
                runner,
                clob=clob,
                side=side,
                keep=keep,
                reason=reason,
                now_monotonic=now_monotonic,
            )
        if runner.open_order_count() > MAX_OPEN_BUY_ORDERS:
            for side in ("UP", "DOWN"):
                _maybe_cancel_orders(
                    runner,
                    clob=clob,
                    side=side,
                    keep=0 if side not in allowed_sides else 1,
                    reason="max_open_orders",
                    now_monotonic=now_monotonic,
                )
            return
        for side in tuple(allowed_sides):
            _maybe_replace_stale_order(
                runner,
                clob=clob,
                poly=poly,
                cfg=cfg,
                side=side,
                now_monotonic=now_monotonic,
            )
        for side in ("UP", "DOWN"):
            if side not in allowed_sides:
                continue
            if runner.open_order_count(side) > 0:
                continue
            if abs(imbalance) > MAX_IMBALANCE_SHARES and side not in _desired_sides(imbalance):
                _log_tag("SKIP BUY", slug=runner.contract.slug, side=side, reason="imbalance_guard")
                continue
            _maybe_place_side(
                runner,
                clob=clob,
                poly=poly,
                cfg=cfg,
                side=side,
                now_monotonic=now_monotonic,
            )


def _btc_binance_symbol(cfg: KngtopConfig) -> str:
    pairs = dict(cfg.trading_pairs)
    symbol = (pairs.get(TRADE_PAIR_KEY) or "").strip().upper()
    if not symbol:
        raise RuntimeError("KNGTOP_PAIRS must include BTC:BTCUSDT for the BTC 5m scalper")
    return symbol


def _discover_target_windows(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    discovery: dict[int, DiscoveryState],
    binance_symbol: str,
) -> None:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    now_monotonic = time.perf_counter()
    for start_sec in _candidate_window_starts(now_ts):
        if start_sec in runners:
            continue
        state = discovery.setdefault(start_sec, DiscoveryState())
        if (now_monotonic - state.last_checked_monotonic) < DISCOVERY_RETRY_SEC_WHEN_MISSING:
            continue
        state.last_checked_monotonic = now_monotonic
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
        )
        _log_tag("WINDOW", slug=contract.slug, state="discovered", start_sec=str(start_sec))


def _refresh_subscriptions(
    *,
    runners: dict[int, WindowRunner],
    poly: MarketWsFeed,
    clob: KngtopClob | None,
) -> None:
    asset_ids: list[str] = []
    for runner in runners.values():
        asset_ids.append(runner.contract.up.token_id)
        asset_ids.append(runner.contract.down.token_id)
        if clob is not None:
            clob.prewarm_market_metadata(runner.contract.up)
            clob.prewarm_market_metadata(runner.contract.down)
    poly.set_assets(asset_ids)


def _purge_finished_windows(
    *,
    runners: dict[int, WindowRunner],
    discovery: dict[int, DiscoveryState],
) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    for start_sec, runner in list(runners.items()):
        elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
        if elapsed is None or remaining is None:
            continue
        if remaining > 0:
            continue
        if runner.open_order_count() > 0:
            continue
        runners.pop(start_sec, None)
        discovery.pop(start_sec, None)
        _log_tag("WINDOW", slug=runner.contract.slug, state="dropped", reason="expired")


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    discovery: dict[int, DiscoveryState],
    subscribed_asset_ids: set[str],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    runtime_state: dict[str, Any],
) -> None:
    del subscribed_asset_ids
    binance_symbol = str(runtime_state["btc_binance_symbol"])
    _discover_target_windows(cfg, runners=runners, discovery=discovery, binance_symbol=binance_symbol)
    _refresh_subscriptions(runners=runners, poly=poly, clob=clob)
    now_monotonic = time.perf_counter()
    if clob is not None and any(_runner_needs_reconcile(runner, now_monotonic) for runner in runners.values()):
        _refresh_global_reconcile_cache(clob=clob, cfg=cfg, runtime_state=runtime_state)
    for runner in list(runners.values()):
        try:
            _tick_runner(
                runner,
                poly=poly,
                binance=binance,
                clob=clob,
                cfg=cfg,
                runtime_state=runtime_state,
            )
        except Exception as exc:  # noqa: BLE001
            _log_tag("WINDOW", slug=runner.contract.slug, state="tick_error", error=str(exc))
    _purge_finished_windows(runners=runners, discovery=discovery)


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    btc_binance_symbol = _btc_binance_symbol(cfg)
    coord = EvalCoordinator(debounce_sec=0.0, heartbeat_sec=cfg.poll_interval_sec)
    runtime_state: dict[str, Any] = {"btc_binance_symbol": btc_binance_symbol}

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
    discovery: dict[int, DiscoveryState] = {}
    subscribed_asset_ids: set[str] = set()

    _log_tag(
        "WINDOW",
        state="boot",
        pair=TRADE_PAIR_KEY,
        window_minutes=str(TRADE_WINDOW_MINUTES),
        heartbeat_sec=str(cfg.poll_interval_sec),
        ws_rest_poll=str(cfg.ws_rest_poll_enabled).lower(),
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(
                cfg,
                runners=runners,
                discovery=discovery,
                subscribed_asset_ids=subscribed_asset_ids,
                poly=poly,
                binance=binance,
                clob=clob,
                runtime_state=runtime_state,
            )
        except Exception as exc:  # noqa: BLE001
            _log_tag("WINDOW", state="main_loop_error", error=str(exc))


if __name__ == "__main__":
    main()
