"""BTC 5m live bot for the KILEMO_2 winner-seed hedge strategy."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
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
WS_UPDATE_LOG_COOLDOWN_SEC = 1.0

SEED_PRICE_MIN = 0.35
SEED_PRICE_MAX = 0.50
SEED_MOVE_LOOKBACK_SEC = 10
SEED_MOVE_MIN_USD = 2.0
HEDGE_PRICE_CAP = 0.35
TARGET_ROI = 0.0
REBALANCE_MULT = 1.0
IMBALANCE_SLACK_USD = 0.5
MAX_BUDGET_USD = 30.0
EXCHANGE_MIN_ORDER_USD = 1.0


@dataclass(slots=True)
class PositionState:
    spent_up: float = 0.0
    spent_down: float = 0.0
    shares_up: float = 0.0
    shares_down: float = 0.0
    orders_up: int = 0
    orders_down: int = 0

    @property
    def spent_total(self) -> float:
        return float(self.spent_up) + float(self.spent_down)

    def pnl_if_up(self) -> float:
        return float(self.shares_up) - self.spent_total

    def pnl_if_down(self) -> float:
        return float(self.shares_down) - self.spent_total


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    positions: PositionState | None = None
    stop_reason: str | None = None
    closed_for_orders: bool = False
    last_action_side: str | None = None
    last_action_elapsed_sec: float | None = None
    last_action_price: float | None = None

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)


@dataclass(frozen=True, slots=True)
class SeedDecision:
    side: str
    spot_px: float
    window_open_px: float
    ask_px: float
    bid_px: float
    move_10s: float


@dataclass(frozen=True, slots=True)
class PnlSnapshot:
    pnl_if_up: float
    pnl_if_down: float
    spent_total: float


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lv,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy_name in ("websocket", "urllib3"):
        noisy = logging.getLogger(noisy_name)
        noisy.setLevel(logging.CRITICAL)
        noisy.propagate = False


def _log_tag(tag: str, **fields: object) -> None:
    parts = [f"{key}={value}" for key, value in fields.items() if value is not None]
    LOGGER.info("[%s] %s", tag, " ".join(parts))


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


def _side_sign(side: str) -> int:
    return 1 if str(side).upper() == "UP" else -1


def _winner_side(*, spot_px: float, window_open_px: float) -> str | None:
    delta = float(spot_px) - float(window_open_px)
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def _opposite_side(side: str) -> str:
    return "DOWN" if str(side).upper() == "UP" else "UP"


def _side_move(*, side: str, price_then_now: tuple[float, float] | None) -> float:
    if price_then_now is None:
        return 0.0
    now_px, past_px = price_then_now
    return float(_side_sign(side)) * (float(now_px) - float(past_px))


def _pnl_snapshot(state: PositionState) -> PnlSnapshot:
    return PnlSnapshot(
        pnl_if_up=state.pnl_if_up(),
        pnl_if_down=state.pnl_if_down(),
        spent_total=state.spent_total,
    )


def evaluate_seed_signal(
    *,
    window_open_px: float,
    spot_px: float,
    up_bid: float,
    up_ask: float,
    down_bid: float,
    down_ask: float,
    price_then_now_10s: tuple[float, float] | None,
) -> SeedDecision | None:
    side = _winner_side(spot_px=float(spot_px), window_open_px=float(window_open_px))
    if side is None:
        return None
    ask_px = float(up_ask) if side == "UP" else float(down_ask)
    bid_px = float(up_bid) if side == "UP" else float(down_bid)
    if ask_px < SEED_PRICE_MIN - 1e-12 or ask_px > SEED_PRICE_MAX + 1e-12:
        return None
    move_10s = _side_move(side=side, price_then_now=price_then_now_10s)
    if move_10s + 1e-12 < SEED_MOVE_MIN_USD:
        return None
    return SeedDecision(
        side=side,
        spot_px=float(spot_px),
        window_open_px=float(window_open_px),
        ask_px=ask_px,
        bid_px=bid_px,
        move_10s=move_10s,
    )


def target_amount_for_side(
    *,
    state: PositionState,
    side: str,
    price: float,
    target_roi: float,
    rebalance_mult: float,
    max_order_usd: float,
    imbalance_slack_usd: float,
) -> float:
    spent = state.spent_total
    pnl_side = state.pnl_if_up() if side == "UP" else state.pnl_if_down()
    pnl_other = state.pnl_if_down() if side == "UP" else state.pnl_if_up()
    denom = (1.0 / float(price)) - 1.0 - float(target_roi)
    if denom <= 1e-12:
        return 0.0
    need_to_target = max(0.0, (float(target_roi) * spent - pnl_side) / denom)
    equalize_raw = max(0.0, float(price) * (pnl_other - pnl_side))
    desired = max(need_to_target, equalize_raw * float(rebalance_mult))
    cap_by_other = max(0.0, (pnl_other - float(target_roi) * spent + float(imbalance_slack_usd)) / (1.0 + float(target_roi)))
    desired = min(desired, float(max_order_usd), cap_by_other, MAX_BUDGET_USD - spent)
    if desired <= 1e-12:
        return 0.0
    return float(desired)


def _token_for_side(runner: WindowRunner, side: str) -> TokenMarket:
    return runner.contract.up if str(side).upper() == "UP" else runner.contract.down


def _send_fak_buy(
    *,
    runner: WindowRunner,
    side: str,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    notional_usd: float,
    ask_px: float,
    bid_px: float,
    reason: str,
) -> bool:
    token = _token_for_side(runner, side)
    max_price = min(float(cfg.market_buy_max_price), float(ask_px))
    if cfg.dry_run or clob is None:
        _log_tag(
            "FAK BUY",
            slug=runner.contract.slug,
            side=side,
            mode="dry_run",
            notional_usd=f"{float(notional_usd):.2f}",
            max_price=f"{max_price:.4f}",
            bid_px=f"{float(bid_px):.4f}",
            ask_px=f"{float(ask_px):.4f}",
            reason=reason,
        )
        return True

    attempts = max(1, int(cfg.order_retry_on_error) + 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = clob.market_buy_usdc(token, float(notional_usd), max_price=max_price)
            order_id = _extract_order_id(payload)
            _log_tag(
                "FAK BUY",
                slug=runner.contract.slug,
                side=side,
                attempt=str(attempt),
                notional_usd=f"{float(notional_usd):.2f}",
                max_price=f"{max_price:.4f}",
                bid_px=f"{float(bid_px):.4f}",
                ask_px=f"{float(ask_px):.4f}",
                reason=reason,
                order_id=order_id or "-",
            )
            return True
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            _log_tag(
                "FAK BUY",
                slug=runner.contract.slug,
                side=side,
                status="error",
                attempt=str(attempt),
                reason=reason,
                error=str(exc),
            )
            time.sleep(0.1)
    if last_error is not None:
        _log_tag("WINDOW", slug=runner.contract.slug, state="buy_failed", side=side, error=str(last_error))
    return False


def _record_fill(*, state: PositionState, side: str, price: float, notional_usd: float) -> None:
    shares = float(notional_usd) / float(price)
    if side == "UP":
        state.spent_up += float(notional_usd)
        state.shares_up += shares
        state.orders_up += 1
    else:
        state.spent_down += float(notional_usd)
        state.shares_down += shares
        state.orders_down += 1


def _can_buy_more(*, state: PositionState, side: str, cfg: KngtopConfig, notional_usd: float, min_order_usd: float) -> bool:
    if float(notional_usd) + 1e-12 < float(min_order_usd):
        return False
    if state.spent_total + float(notional_usd) > MAX_BUDGET_USD + 1e-12:
        return False
    if side == "UP":
        return state.orders_up < int(cfg.hedge_max_orders_per_side)
    return state.orders_down < int(cfg.hedge_max_orders_per_side)


def _floor_pnl_after_buy(*, state: PositionState, side: str, price: float, notional_usd: float) -> float:
    test = PositionState(
        spent_up=state.spent_up,
        spent_down=state.spent_down,
        shares_up=state.shares_up,
        shares_down=state.shares_down,
        orders_up=state.orders_up,
        orders_down=state.orders_down,
    )
    _record_fill(state=test, side=side, price=price, notional_usd=notional_usd)
    return min(test.pnl_if_up(), test.pnl_if_down())


def _maybe_seed_trade(
    *,
    runner: WindowRunner,
    decision: SeedDecision,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    elapsed: float,
) -> bool:
    state = runner.positions
    if state is None:
        state = PositionState()
        runner.positions = state
    if state.spent_total > 1e-12:
        return False
    base_order_usd = max(float(cfg.notional_usd), EXCHANGE_MIN_ORDER_USD)
    order_usd = min(base_order_usd, MAX_BUDGET_USD - state.spent_total)
    if not _can_buy_more(
        state=state,
        side=decision.side,
        cfg=cfg,
        notional_usd=order_usd,
        min_order_usd=EXCHANGE_MIN_ORDER_USD,
    ):
        return False
    if not _send_fak_buy(
        runner=runner,
        side=decision.side,
        clob=clob,
        cfg=cfg,
        notional_usd=order_usd,
        ask_px=decision.ask_px,
        bid_px=decision.bid_px,
        reason="seed_winner",
    ):
        return False
    _record_fill(state=state, side=decision.side, price=decision.ask_px, notional_usd=order_usd)
    runner.last_action_side = decision.side
    runner.last_action_elapsed_sec = elapsed
    runner.last_action_price = decision.ask_px
    _log_tag(
        "SEED",
        slug=runner.contract.slug,
        side=decision.side,
        ask_px=f"{decision.ask_px:.4f}",
        btc_spot=f"{decision.spot_px:.2f}",
        window_open_px=f"{decision.window_open_px:.2f}",
        move_10s=f"{decision.move_10s:.2f}",
        spent_total=f"{state.spent_total:.2f}",
        pnl_if_up=f"{state.pnl_if_up():.4f}",
        pnl_if_down=f"{state.pnl_if_down():.4f}",
    )
    return True


def _maybe_hedge_trade(
    *,
    runner: WindowRunner,
    up_bid: float,
    up_ask: float,
    down_bid: float,
    down_ask: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    elapsed: float,
) -> bool:
    state = runner.positions
    if state is None or state.spent_total + 1e-12 < float(cfg.notional_usd):
        return False
    snapshot = _pnl_snapshot(state)
    side = "UP" if snapshot.pnl_if_up < snapshot.pnl_if_down else "DOWN"
    ask_px = float(up_ask) if side == "UP" else float(down_ask)
    bid_px = float(up_bid) if side == "UP" else float(down_bid)
    if ask_px > HEDGE_PRICE_CAP + 1e-12:
        return False
    base_order_usd = max(float(cfg.notional_usd), EXCHANGE_MIN_ORDER_USD)
    raw_order_usd = target_amount_for_side(
        state=state,
        side=side,
        price=ask_px,
        target_roi=TARGET_ROI,
        rebalance_mult=REBALANCE_MULT,
        max_order_usd=max(base_order_usd, 2.0 * base_order_usd),
        imbalance_slack_usd=IMBALANCE_SLACK_USD,
    )
    if raw_order_usd <= 1e-12:
        return False
    min_order_usd = EXCHANGE_MIN_ORDER_USD
    order_usd = raw_order_usd
    if 0.0 < raw_order_usd < min_order_usd - 1e-12:
        floor_before = min(snapshot.pnl_if_up, snapshot.pnl_if_down)
        floor_after_min = _floor_pnl_after_buy(
            state=state,
            side=side,
            price=ask_px,
            notional_usd=min_order_usd,
        )
        if floor_after_min + 1e-12 < floor_before:
            _log_tag(
                "HEDGE SKIP",
                slug=runner.contract.slug,
                side=side,
                ask_px=f"{ask_px:.4f}",
                raw_buy_usd=f"{raw_order_usd:.4f}",
                rounded_buy_usd=f"{min_order_usd:.2f}",
                reason="rounded_min_worsens_floor",
                pnl_if_up=f"{snapshot.pnl_if_up:.4f}",
                pnl_if_down=f"{snapshot.pnl_if_down:.4f}",
            )
            return False
        order_usd = min_order_usd
    if not _can_buy_more(
        state=state,
        side=side,
        cfg=cfg,
        notional_usd=order_usd,
        min_order_usd=min_order_usd,
    ):
        return False
    if not _send_fak_buy(
        runner=runner,
        side=side,
        clob=clob,
        cfg=cfg,
        notional_usd=order_usd,
        ask_px=ask_px,
        bid_px=bid_px,
        reason="rebalance_deficit",
    ):
        return False
    before_up = snapshot.pnl_if_up
    before_down = snapshot.pnl_if_down
    _record_fill(state=state, side=side, price=ask_px, notional_usd=order_usd)
    runner.last_action_side = side
    runner.last_action_elapsed_sec = elapsed
    runner.last_action_price = ask_px
    _log_tag(
        "HEDGE",
        slug=runner.contract.slug,
        side=side,
        ask_px=f"{ask_px:.4f}",
        buy_usd=f"{order_usd:.2f}",
        pnl_if_up_before=f"{before_up:.4f}",
        pnl_if_down_before=f"{before_down:.4f}",
        pnl_if_up_after=f"{state.pnl_if_up():.4f}",
        pnl_if_down_after=f"{state.pnl_if_down():.4f}",
        spent_total=f"{state.spent_total:.2f}",
        orders_up=str(state.orders_up),
        orders_down=str(state.orders_down),
    )
    return True


def _discover_target_windows(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    binance_symbol: str,
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
            positions=PositionState(),
        )
        _log_tag(
            "WINDOW",
            slug=contract.slug,
            state="discovered",
            start_sec=str(start_sec),
            window_open_px="-" if window_open_px is None else f"{window_open_px:.2f}",
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
        state = runner.positions or PositionState()
        _log_tag(
            "WINDOW",
            slug=runner.contract.slug,
            state="expired",
            reason=runner.stop_reason or "-",
            spent_total=f"{state.spent_total:.2f}",
            pnl_if_up=f"{state.pnl_if_up():.4f}",
            pnl_if_down=f"{state.pnl_if_down():.4f}",
            orders_up=str(state.orders_up),
            orders_down=str(state.orders_down),
        )
        runners.pop(start_sec, None)


def _tick_runner(
    runner: WindowRunner,
    *,
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    elapsed, remaining = _window_elapsed_remaining(runner, now_ts)
    if elapsed is None or remaining is None:
        return
    if elapsed < 0:
        _log_tag("WINDOW", slug=runner.contract.slug, state="prestart_watch")
        return
    if remaining <= float(cfg.order_cutoff_remaining_sec):
        if not runner.closed_for_orders:
            runner.closed_for_orders = True
            runner.stop_reason = "late_window"
            state = runner.positions or PositionState()
            _log_tag(
                "WINDOW",
                slug=runner.contract.slug,
                state="order_cutoff",
                remaining_sec=f"{remaining:.1f}",
                spent_total=f"{state.spent_total:.2f}",
                pnl_if_up=f"{state.pnl_if_up():.4f}",
                pnl_if_down=f"{state.pnl_if_down():.4f}",
            )
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
            _log_tag("SKIP BUY", slug=runner.contract.slug, reason="window_open_missing")
            return
    spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
    if spot is None:
        _log_tag("SKIP BUY", slug=runner.contract.slug, reason="binance_stale")
        return
    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    if up_quote is None or down_quote is None:
        _log_tag("SKIP BUY", slug=runner.contract.slug, reason="poly_stale")
        return
    up_bid, up_ask = up_quote
    down_bid, down_ask = down_quote
    price_then_now_10s = binance.price_then_now(
        runner.binance_symbol,
        lookback_sec=SEED_MOVE_LOOKBACK_SEC,
        max_age_sec=cfg.binance_max_age_sec,
    )
    decision = evaluate_seed_signal(
        window_open_px=float(runner.window_open_px),
        spot_px=float(spot),
        up_bid=float(up_bid),
        up_ask=float(up_ask),
        down_bid=float(down_bid),
        down_ask=float(down_ask),
        price_then_now_10s=price_then_now_10s,
    )
    if decision is not None and _maybe_seed_trade(runner=runner, decision=decision, clob=clob, cfg=cfg, elapsed=elapsed):
        return
    _maybe_hedge_trade(
        runner=runner,
        up_bid=float(up_bid),
        up_ask=float(up_ask),
        down_bid=float(down_bid),
        down_ask=float(down_ask),
        clob=clob,
        cfg=cfg,
        elapsed=elapsed,
    )


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[int, WindowRunner],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
) -> None:
    binance_symbol = dict(cfg.trading_pairs).get(TRADE_PAIR_KEY, "BTCUSDT")
    _discover_target_windows(cfg, runners=runners, binance_symbol=binance_symbol)
    _refresh_subscriptions(runners=runners, poly=poly)
    for runner in list(runners.values()):
        try:
            _tick_runner(runner, poly=poly, binance=binance, clob=clob, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            _log_tag("WINDOW", slug=runner.contract.slug, state="tick_error", error=str(exc))
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
        "WINDOW",
        state="boot",
        pair=TRADE_PAIR_KEY,
        window_minutes=str(TRADE_WINDOW_MINUTES),
        heartbeat_sec=str(cfg.poll_interval_sec),
        strategy="kilemo2_h2725_no_delay",
        order_type="fak",
        order_notional_usd=f"{float(cfg.notional_usd):.2f}",
        hedge_max_orders_per_side=str(cfg.hedge_max_orders_per_side),
        seed_band=f"{SEED_PRICE_MIN:.2f}-{SEED_PRICE_MAX:.2f}",
        seed_move10_usd=f"{SEED_MOVE_MIN_USD:.2f}",
        hedge_price_cap=f"{HEDGE_PRICE_CAP:.2f}",
        budget_cap_usd=f"{MAX_BUDGET_USD:.2f}",
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _log_tag("WINDOW", state="main_loop_error", error=str(exc))


if __name__ == "__main__":
    main()
