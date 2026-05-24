"""Clean BTC 5m two-sided limit-order engine.

Rule shape:
- Trade fixed 5-share limit buys.
- Use the C_weak_cap045_high_guard060 rules:
  - start with the cheaper side if ask <= 0.55;
  - buy the missing hedge side up to 0.70;
  - after both sides exist, repair only cheap/weak or guarded-improving buys;
  - avg_sum_after_buy must stay <= 0.95.
- Keep at most one buy order per side while that side has room.
- Never keep two buy orders on the same side.
- For buys, a lower resting limit is better; replace only orders that are too expensive.
"""

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
from kngtop.gamma import ActiveContract, TokenMarket, discover_updown_window_by_start, window_start_ts_from_slug
from kngtop.pm_data import fetch_user_positions
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")

TRADE_PAIR_KEY = "BTC"
TRADE_WINDOW_MINUTES = 5
WINDOW_SECONDS = TRADE_WINDOW_MINUTES * 60
PRESTART_SEC = 20
INITIAL_PAIR_PRICE = 0.47
ORDER_SHARES = 5.0
MAX_SPENT_PER_WINDOW = 20.0
REPRICE_TOLERANCE = 0.005
MIN_ORDER_USD = 1.05
AVG_IMPROVE_BUFFER = 0.02
AVG_SUM_CAP = 0.95
C_MISSING_HEDGE_CAP = 0.70
C_WEAK_CAP = 0.45
C_HIGH_GUARD = 0.60
C_BALANCE_EPSILON = 1e-9
REPLACE_COOLDOWN_SEC = 2.0
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


@dataclass(slots=True)
class SentOrder:
    order_id: str | None
    side: str
    price: float
    shares: float
    created_ts: float


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    positions: PositionState = field(default_factory=PositionState)
    open_orders: dict[str, list[OpenOrder]] = field(default_factory=lambda: {"UP": [], "DOWN": []})
    last_replace_ts: dict[str, float] = field(default_factory=lambda: {"UP": 0.0, "DOWN": 0.0})
    sent_shares: dict[str, float] = field(default_factory=lambda: {"UP": 0.0, "DOWN": 0.0})
    sent_cost: dict[str, float] = field(default_factory=lambda: {"UP": 0.0, "DOWN": 0.0})
    sent_orders: list[SentOrder] = field(default_factory=list)
    trade_history_positions: PositionState = field(default_factory=PositionState)
    last_trade_history_fetch_ts: float = 0.0
    stop_reason: str | None = None

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)


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


def _extract_order_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("orderID", "orderId", "id"):
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
    for side in ("UP", "DOWN"):
        reflected_shares = max(0.0, merged.shares(side) - previous.shares(side))
        if reflected_shares > 1e-12:
            _remove_local_sent_exposure(runner, side, reflected_shares, merged.avg(side))
    runner.positions = merged
    return merged


def _parse_trade_history(runner: WindowRunner, rows_by_side: dict[str, list[dict[str, Any]]], *, cfg: KngtopConfig) -> PositionState:
    del cfg
    pos = PositionState()
    seen: set[str] = set()
    for side, rows in rows_by_side.items():
        for row in rows:
            raw_side = _extract_text(row, "side", "takerSide", "taker_side", "makerSide", "maker_side").upper()
            if raw_side and raw_side not in {"BUY", "BID"}:
                continue
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
    runner.trade_history_positions = _parse_trade_history(runner, rows_by_side, cfg=cfg)
    return runner.trade_history_positions


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
        remaining = _extract_numeric(row, "size_left", "remaining", "original_size", "size") or 0.0
        if oid and price > 0.0 and remaining > 1e-12:
            out[side].append(OpenOrder(order_id=oid, side=side, price=price, remaining_shares=remaining))
    return out


def _sync_open_orders(runner: WindowRunner, *, clob: KngtopClob | None, rows: list[dict[str, Any]] | None = None) -> dict[str, list[OpenOrder]]:
    if clob is None and rows is None:
        return runner.open_orders
    open_rows = rows if rows is not None else (clob.get_open_orders() if clob is not None else [])
    runner.open_orders = _parse_open_orders(runner, list(open_rows))
    return runner.open_orders


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


def _local_sent_shares(runner: WindowRunner, side: str) -> float:
    return max(0.0, float(runner.sent_shares.get(side, 0.0)))


def _effective_side_exposure(runner: WindowRunner, side: str) -> float:
    return runner.positions.shares(side) + max(_open_order_shares(runner, side), _local_sent_shares(runner, side))


def _local_sent_total_cost(runner: WindowRunner) -> float:
    return max(0.0, float(runner.sent_cost.get("UP", 0.0))) + max(0.0, float(runner.sent_cost.get("DOWN", 0.0)))


def _remove_local_sent_exposure(runner: WindowRunner, side: str, shares: float, price: float) -> None:
    qty = max(0.0, float(shares))
    px = max(0.0, float(price))
    if qty <= 1e-12:
        return
    runner.sent_shares[side] = max(0.0, _local_sent_shares(runner, side) - qty)
    runner.sent_cost[side] = max(0.0, float(runner.sent_cost.get(side, 0.0)) - qty * px)
    remaining = qty
    kept: list[SentOrder] = []
    for sent in runner.sent_orders:
        if sent.side != side or remaining <= 1e-12:
            kept.append(sent)
            continue
        consume = min(remaining, sent.shares)
        remaining -= consume
        left = sent.shares - consume
        if left > 1e-12:
            kept.append(SentOrder(order_id=sent.order_id, side=sent.side, price=sent.price, shares=left, created_ts=sent.created_ts))
    runner.sent_orders = kept


def _has_unresolved_sent_order(runner: WindowRunner, side: str) -> bool:
    return any(sent.side == side and sent.shares > 1e-12 for sent in runner.sent_orders)


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
    if pos.spent_total() + _local_sent_total_cost(runner) + price * ORDER_SHARES > MAX_SPENT_PER_WINDOW + 1e-12:
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


def _cancel_order(runner: WindowRunner, *, clob: KngtopClob | None, order: OpenOrder, reason: str) -> bool:
    if clob is None:
        _remove_local_sent_exposure(runner, order.side, order.remaining_shares, order.price)
        return True
    try:
        clob.cancel_order_by_id(order.order_id)
    except Exception as exc:  # noqa: BLE001
        _log_tag("CANCEL FAILED", slug=runner.contract.slug, side=order.side, order_id=order.order_id, reason=reason, error=str(exc))
        return False
    _remove_local_sent_exposure(runner, order.side, order.remaining_shares, order.price)
    _log_tag("CANCEL", slug=runner.contract.slug, side=order.side, order_id=order.order_id, price=f"{order.price:.2f}", reason=reason)
    return True


def _post_order(runner: WindowRunner, *, clob: KngtopClob | None, side: str, price: float, cfg: KngtopConfig) -> bool:
    del cfg
    if clob is None:
        _log_tag("DRY ORDER", slug=runner.contract.slug, side=side, price=f"{price:.2f}", shares=f"{ORDER_SHARES:.2f}")
        runner.sent_shares[side] = _local_sent_shares(runner, side) + ORDER_SHARES
        runner.sent_cost[side] = max(0.0, float(runner.sent_cost.get(side, 0.0))) + ORDER_SHARES * price
        runner.sent_orders.append(SentOrder(order_id=None, side=side, price=price, shares=ORDER_SHARES, created_ts=time.time()))
        return True
    try:
        payload = clob.limit_buy_shares(_token_for_side(runner, side), price=price, shares=ORDER_SHARES, post_only=False)
    except Exception as exc:  # noqa: BLE001
        if "not enough balance" in str(exc).lower() or "allowance" in str(exc).lower():
            runner.stop_reason = "balance_or_allowance"
        _log_tag("ORDER FAILED", slug=runner.contract.slug, side=side, price=f"{price:.2f}", error=str(exc))
        return False
    order_id = _extract_order_id(payload)
    runner.sent_shares[side] = _local_sent_shares(runner, side) + ORDER_SHARES
    runner.sent_cost[side] = max(0.0, float(runner.sent_cost.get(side, 0.0))) + ORDER_SHARES * price
    runner.sent_orders.append(SentOrder(order_id=order_id, side=side, price=price, shares=ORDER_SHARES, created_ts=time.time()))
    _log_tag("ORDER SENT", slug=runner.contract.slug, side=side, price=f"{price:.2f}", shares=f"{ORDER_SHARES:.2f}", order_id=order_id)
    return True


def _maintain_side_order(
    runner: WindowRunner,
    *,
    side: str,
    desired_price: float | None,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    now_monotonic: float,
) -> bool:
    rows = list(runner.open_orders.get(side, []))
    for extra in rows[1:]:
        _cancel_order(runner, clob=clob, order=extra, reason="duplicate_side_order")
        return True
    rows = rows[:1]
    if desired_price is None:
        for order in rows:
            _cancel_order(runner, clob=clob, order=order, reason="side_complete_or_blocked")
            return True
        return False
    if not rows and _has_unresolved_sent_order(runner, side):
        _log_tag("WAIT SENT", slug=runner.contract.slug, side=side, reason="unresolved_order")
        return False
    if rows:
        order = rows[0]
        aggressive_reprice = _side_needs_aggressive_reprice(runner, side)
        if not aggressive_reprice and order.price <= desired_price + REPRICE_TOLERANCE:
            return False
        if aggressive_reprice and abs(order.price - desired_price) <= REPRICE_TOLERANCE:
            return False
        if now_monotonic - runner.last_replace_ts[side] < REPLACE_COOLDOWN_SEC:
            return False
        if not _cancel_order(runner, clob=clob, order=order, reason=f"reprice->{desired_price:.2f}"):
            return False
        runner.last_replace_ts[side] = now_monotonic
        return True
    _post_order(runner, clob=clob, side=side, price=desired_price, cfg=cfg)
    return False


def _side_needs_aggressive_reprice(runner: WindowRunner, side: str) -> bool:
    pos = runner.positions
    side_shares = pos.shares(side)
    other_shares = pos.shares("DOWN" if side == "UP" else "UP")
    return side_shares + C_BALANCE_EPSILON < other_shares


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
    elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
    if elapsed is None or remaining is None:
        return
    if remaining <= 0:
        return
    state = runtime_state if runtime_state is not None else {}
    history_rows = state.get("reconcile_trade_history")
    if isinstance(history_rows, dict):
        _refresh_trade_history(runner, clob=clob, cfg=cfg, rows_by_side={str(k): list(v) for k, v in history_rows.items() if isinstance(v, list)}, now_ts=now_ts)
    elif clob is not None:
        _refresh_trade_history(runner, clob=clob, cfg=cfg, now_ts=now_ts)
    rows = state.get("reconcile_positions")
    _refresh_positions(runner, cfg=cfg, rows=list(rows) if isinstance(rows, list) else None)
    if clob is not None:
        _sync_open_orders(runner, clob=clob)
    else:
        open_rows = state.get("reconcile_open_orders")
        _sync_open_orders(runner, clob=clob, rows=list(open_rows) if isinstance(open_rows, list) else None)
    if elapsed < -PRESTART_SEC - 1e-12:
        return
    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    desired = _c_target_prices(
        runner,
        up_ask=up_quote[1] if up_quote else None,
        down_ask=down_quote[1] if down_quote else None,
        cfg=cfg,
    )
    now_monotonic = time.monotonic()
    if _maintain_side_order(runner, side="UP", desired_price=desired["UP"], clob=clob, cfg=cfg, now_monotonic=now_monotonic):
        return
    _sync_open_orders(runner, clob=clob)
    _maintain_side_order(runner, side="DOWN", desired_price=desired["DOWN"], clob=clob, cfg=cfg, now_monotonic=now_monotonic)


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
        _log_tag("INIT", slug=contract.slug, start_sec=str(start_sec), strategy="two_sided_limit_engine")


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
        strategy="C_weak_cap045_high_guard060_limit5",
        order_shares=f"{ORDER_SHARES:.2f}",
        max_shares_per_side=f"{cfg.max_shares_per_side:.2f}",
    )
    while True:
        coord.wait_for_turn()
        _run_iteration(cfg, runners=runners, poly=poly, clob=clob, runtime_state=runtime_state)


if __name__ == "__main__":
    main()
