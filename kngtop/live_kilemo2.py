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
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")

TRADE_PAIR_KEY = "BTC"
TRADE_WINDOW_MINUTES = 5
WINDOW_SECONDS = TRADE_WINDOW_MINUTES * 60
NEXT_WINDOW_LOOKAHEAD_SEC = 20

MIN_ORDER_USD = 1.0
LARGE_ORDER_USD = 2.0
MAX_TOTAL_DEALS = 15
MAX_ORDERS_PER_SIDE = 8

BOOTSTRAP_CHEAP_CAP = 0.55
ACTIVE_REPAIR_INTERVAL_SEC = 5
IMBALANCE_TRIGGER = 0.20
AVG_SUM_CAP = 0.95
WEAK_REPAIR_CHEAP_CAP = 0.45
HIGH_REPAIR_GUARD = 0.60
HIGH_REPAIR_PRE240_CAP = 0.65
FINAL_60_DANGER_CAP = 0.80
LOCKED_PROFIT_PNL = 0.50
LOCKED_PROFIT_IMBALANCE_TRIGGER = 0.25
HIGH_REPAIR_WORST_TARGET = -0.25
HIGH_REPAIR_SHARE_GAP_TARGET = 0.10
DANGEROUS_WEAK_PNL = -2.0
LARGE_ORDER_PRICE_THRESHOLD = 0.30
LARGE_ORDER_IMBALANCE_THRESHOLD = 0.40
MAX_ORDER_PRICE = 0.99
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
    last_repair_slot: int = -1
    last_missing_wait_log_slot: int = -1
    pending_order: bool = False
    pending_side: str | None = None
    pending_reason: str | None = None
    pending_created_ts: float = 0.0
    last_successful_buy_ts: float = -10_000.0
    last_position_refresh_ts: float = 0.0
    execution_state: str = READY
    next_decision_ts: float = 0.0
    stop_reason: str | None = None

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


def _extract_filled_shares(payload: object) -> float:
    if not isinstance(payload, dict):
        return 0.0
    value = _extract_numeric(payload, "size_matched", "matched_amount", "filled_amount", "filled", "makerAmountFilled")
    if value is not None:
        return max(0.0, float(value))
    nested = payload.get("order")
    if isinstance(nested, dict):
        nested_value = _extract_numeric(nested, "size_matched", "matched_amount", "filled_amount", "filled", "makerAmountFilled")
        if nested_value is not None:
            return max(0.0, float(nested_value))
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


def _order_amount_usd(*, ask_px: float, state: PositionState, cfg: KngtopConfig) -> float:
    large_order = max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))
    if ask_px <= LARGE_ORDER_PRICE_THRESHOLD + 1e-12 or state.share_imbalance() > LARGE_ORDER_IMBALANCE_THRESHOLD + 1e-12:
        return large_order
    return MIN_ORDER_USD


def _bootstrap_amount_usd(cfg: KngtopConfig) -> float:
    return max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))


def _can_buy(state: PositionState, side: str, amount_usd: float) -> bool:
    if amount_usd + 1e-12 < MIN_ORDER_USD:
        return False
    if state.spent_total() + amount_usd > 20.0 + 1e-12:
        return False
    if state.total_deals >= MAX_TOTAL_DEALS:
        return False
    if side == "UP":
        return state.orders_up < MAX_ORDERS_PER_SIDE
    return state.orders_down < MAX_ORDERS_PER_SIDE


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


def _next_retry_delay(reason: str) -> float:
    if reason in {"initial_lower_ask", "cheap_weak_repair", "guarded_high_repair"}:
        return 1.0
    return float(ACTIVE_REPAIR_INTERVAL_SEC)


def _schedule_next_decision(runner: WindowRunner, *, now_ts: float, reason: str) -> None:
    runner.execution_state = WAIT_NEXT_DECISION
    runner.next_decision_ts = now_ts + _next_retry_delay(reason)


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
    if ask_px <= 0.0 or ask_px > min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price)) + 1e-12:
        return False
    if not _can_buy(runner.positions, side, amount_usd):
        _log_tag(
            "BUDGET BLOCK",
            slug=runner.contract.slug,
            side=side,
            reason=reason,
            amount=f"{amount_usd:.2f}",
            spent_total=f"{runner.positions.spent_total():.2f}",
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

    proj_up, proj_down, proj_worst, proj_gap, proj_share_gap, proj_avg_sum = _projected_after_buy(runner.positions, side, ask_px, amount_usd)
    _log_tag(
        "PNL_STATE",
        slug=runner.contract.slug,
        pnl_if_up=f"{runner.positions.pnl_if_up():.4f}",
        pnl_if_down=f"{runner.positions.pnl_if_down():.4f}",
        weak_side=_weak_outcome_side(runner.positions) or "TIE",
        projected_side=side,
        projected_worst=f"{proj_worst:.4f}",
        projected_gap=f"{proj_gap:.4f}",
        projected_share_gap=f"{proj_share_gap:.4f}",
        projected_avg_sum=f"{proj_avg_sum:.4f}",
    )
    _log_tag("INTENT CREATED", slug=runner.contract.slug, side=side, reason=reason, ask=f"{ask_px:.4f}", amount=f"{amount_usd:.2f}")
    runner.pending_order = True
    runner.execution_state = ORDER_IN_FLIGHT
    runner.pending_side = side
    runner.pending_reason = reason
    runner.pending_created_ts = elapsed
    _log_tag("ORDER SENT", slug=runner.contract.slug, side=side, reason=reason, ask=f"{ask_px:.4f}", amount=f"{amount_usd:.2f}")

    try:
        token = _token_for_side(runner, side)
        if not cfg.dry_run and clob is not None:
            attempts = max(1, int(cfg.order_retry_on_error) + 1)
            last_error: Exception | None = None
            for _attempt in range(1, attempts + 1):
                try:
                    payload = clob.market_buy_usdc(token, amount_usd, max_price=min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price), ask_px))
                    filled_shares = _extract_filled_shares(payload)
                    if filled_shares <= 0.000001:
                        _log_tag(
                            "ORDER NOFILL",
                            slug=runner.contract.slug,
                            side=side,
                            reason=reason,
                            ask=f"{ask_px:.4f}",
                            amount=f"{amount_usd:.2f}",
                        )
                        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
                        return False
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if "no orders found to match with FAK order" in str(exc):
                        _log_tag(
                            "ORDER FAILED",
                            slug=runner.contract.slug,
                            side=side,
                            reason=reason,
                            ask=f"{ask_px:.4f}",
                            amount=f"{amount_usd:.2f}",
                            error=str(exc),
                        )
                        _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
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
                    _schedule_next_decision(runner, now_ts=datetime.now(timezone.utc).timestamp(), reason=reason)
                return False

        filled_shares_for_state = None if cfg.dry_run or clob is None else filled_shares
        _apply_position_fill(runner.positions, side, ask_px, amount_usd, filled_shares_for_state)
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


def _choose_guarded_pnl_buy(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    remaining: float,
    cfg: KngtopConfig,
) -> BuyAction | None:
    state = runner.positions
    if state.total_deals == 0:
        side = "UP" if up_ask <= down_ask else "DOWN"
        ask_px = up_ask if side == "UP" else down_ask
        if ask_px > BOOTSTRAP_CHEAP_CAP + 1e-12:
            return None
        return BuyAction(side=side, ask_px=ask_px, amount_usd=_bootstrap_amount_usd(cfg), reason="initial_lower_ask", enforce_avg_cap=False)

    side = _weak_outcome_side(state)
    if side is None:
        side = "UP" if up_ask <= down_ask else "DOWN"
    ask_px = up_ask if side == "UP" else down_ask
    amount_usd = _order_amount_usd(ask_px=ask_px, state=state, cfg=cfg)

    locked_profit = state.pnl_if_up() >= LOCKED_PROFIT_PNL - 1e-12 and state.pnl_if_down() >= LOCKED_PROFIT_PNL - 1e-12
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
) -> bool:
    slot = int(elapsed) // ACTIVE_REPAIR_INTERVAL_SEC
    if slot <= runner.last_repair_slot:
        return False
    runner.last_repair_slot = slot

    action = _choose_guarded_pnl_buy(
        runner,
        up_ask=up_ask,
        down_ask=down_ask,
        elapsed=elapsed,
        remaining=remaining,
        cfg=cfg,
    )
    if action is None:
        return False
    return _send_fak_buy(
        runner=runner,
        side=action.side,
        ask_px=action.ask_px,
        amount_usd=action.amount_usd,
        reason=action.reason,
        elapsed=elapsed,
        clob=clob,
        cfg=cfg,
        enforce_avg_cap=action.enforce_avg_cap,
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
    if runner.execution_state == ORDER_IN_FLIGHT or runner.pending_order:
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
    if binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec) is None:
        return
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
        large_order_usd=f"{min(float(cfg.notional_usd), LARGE_ORDER_USD):.2f}",
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
