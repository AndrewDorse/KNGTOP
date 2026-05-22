"""BTC 5m live bot for bootstrap_active_repair_C + rescue_60_cap080."""

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
BOOTSTRAP_OPPOSITE_CAP = 0.70
ACTIVE_REPAIR_INTERVAL_SEC = 15
IMBALANCE_TRIGGER = 0.20
DROP_TRIGGER = 0.02
AVG_SUM_CAP = 0.95
BALANCED_CHEAP_PRICE = 0.35
REPAIR_CHEAP_CAP = 0.45
LAST30_FORCE_CAP = 0.80
RESCUE_60_CAP = 0.80
LARGE_ORDER_PRICE_THRESHOLD = 0.30
LARGE_ORDER_IMBALANCE_THRESHOLD = 0.40
MAX_ORDER_PRICE = 0.99


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
        return self.orders_up > 0 and self.orders_down > 0


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    positions: PositionState = field(default_factory=PositionState)
    bootstrap_second_attempted: bool = False
    rescue_60_attempted: bool = False
    last_repair_slot: int = -1
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


def _order_amount_usd(*, ask_px: float, state: PositionState, cfg: KngtopConfig) -> float:
    large_order = max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD))
    if ask_px <= LARGE_ORDER_PRICE_THRESHOLD + 1e-12 or state.share_imbalance() > LARGE_ORDER_IMBALANCE_THRESHOLD + 1e-12:
        return large_order
    return MIN_ORDER_USD


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


def _smaller_share_side(state: PositionState) -> str:
    return "UP" if state.shares_up < state.shares_down else "DOWN"


def _apply_position_buy(state: PositionState, side: str, ask_px: float, amount_usd: float) -> None:
    shares = amount_usd / max(ask_px, 1e-9)
    if side == "UP":
        state.spent_up += amount_usd
        state.shares_up += shares
        state.orders_up += 1
    else:
        state.spent_down += amount_usd
        state.shares_down += shares
        state.orders_down += 1
    state.total_deals += 1


def _send_fak_buy(
    *,
    runner: WindowRunner,
    side: str,
    ask_px: float,
    amount_usd: float,
    reason: str,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    enforce_avg_cap: bool = True,
) -> bool:
    if ask_px <= 0.0 or ask_px > min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price)) + 1e-12:
        return False
    if not _can_buy(runner.positions, side, amount_usd):
        return False
    after_avg_sum = _avg_after_buy(runner.positions, side, ask_px, amount_usd)
    if enforce_avg_cap and after_avg_sum > AVG_SUM_CAP + 1e-12:
        return False

    token = _token_for_side(runner, side)
    if not cfg.dry_run and clob is not None:
        attempts = max(1, int(cfg.order_retry_on_error) + 1)
        last_error: Exception | None = None
        for _attempt in range(1, attempts + 1):
            try:
                payload = clob.market_buy_usdc(token, amount_usd, max_price=min(MAX_ORDER_PRICE, float(cfg.market_buy_max_price), ask_px))
                _ = _extract_order_id(payload)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(0.5)
        else:
            if last_error is not None:
                _log_tag("ERROR", slug=runner.contract.slug, stage="buy", side=side, reason=reason, error=str(last_error))
            return False

    _apply_position_buy(runner.positions, side, ask_px, amount_usd)
    _log_tag(
        "BUY",
        slug=runner.contract.slug,
        side=side,
        reason=reason,
        ask_px=f"{ask_px:.4f}",
        amount_usd=f"{amount_usd:.2f}",
        orders=str(runner.positions.total_deals),
        avg_sum=f"{_avg_sum(runner.positions):.4f}",
        imbalance=f"{runner.positions.share_imbalance():.4f}",
    )
    return True


def _choose_active_repair_side(runner: WindowRunner, *, up_ask: float, down_ask: float, remaining: float) -> str | None:
    state = runner.positions
    smaller = _smaller_share_side(state)
    bigger = "DOWN" if smaller == "UP" else "UP"
    smaller_ask = up_ask if smaller == "UP" else down_ask
    bigger_ask = down_ask if smaller == "UP" else up_ask

    if remaining <= 60.0:
        return smaller
    if state.share_imbalance() > IMBALANCE_TRIGGER + 1e-12:
        return smaller

    smaller_avg = state.avg_up() if smaller == "UP" else state.avg_down()
    bigger_avg = state.avg_down() if smaller == "UP" else state.avg_up()
    if smaller_avg > 1e-12 and smaller_ask <= smaller_avg - DROP_TRIGGER + 1e-12:
        return smaller
    if bigger_avg > 1e-12 and bigger_ask <= bigger_avg - DROP_TRIGGER + 1e-12:
        return bigger

    if _avg_sum(state) <= AVG_SUM_CAP + 1e-12:
        if smaller_ask <= REPAIR_CHEAP_CAP + 1e-12:
            return smaller
        if bigger_ask <= REPAIR_CHEAP_CAP + 1e-12:
            return bigger

    if state.share_imbalance() <= IMBALANCE_TRIGGER + 1e-12:
        if up_ask <= BALANCED_CHEAP_PRICE + 1e-12 and up_ask <= down_ask + 1e-12:
            return "UP"
        if down_ask <= BALANCED_CHEAP_PRICE + 1e-12:
            return "DOWN"
    return None


def _maybe_bootstrap_first_leg(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if runner.positions.total_deals > 0 or elapsed >= 15.0:
        return
    side = "UP" if up_ask <= down_ask else "DOWN"
    ask_px = up_ask if side == "UP" else down_ask
    if ask_px > BOOTSTRAP_CHEAP_CAP + 1e-12:
        return
    _send_fak_buy(
        runner=runner,
        side=side,
        ask_px=ask_px,
        amount_usd=MIN_ORDER_USD,
        reason="bootstrap_cheaper",
        clob=clob,
        cfg=cfg,
        enforce_avg_cap=False,
    )


def _maybe_bootstrap_second_leg(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if runner.bootstrap_second_attempted or elapsed < 15.0:
        return
    runner.bootstrap_second_attempted = True
    if runner.positions.both_sides_traded() or runner.positions.total_deals == 0:
        return
    missing = "UP" if runner.positions.orders_up == 0 else "DOWN"
    ask_px = up_ask if missing == "UP" else down_ask
    if ask_px > BOOTSTRAP_OPPOSITE_CAP + 1e-12:
        return
    _send_fak_buy(
        runner=runner,
        side=missing,
        ask_px=ask_px,
        amount_usd=MIN_ORDER_USD,
        reason="bootstrap_opposite",
        clob=clob,
        cfg=cfg,
        enforce_avg_cap=False,
    )


def _maybe_active_repair(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    elapsed: float,
    remaining: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if not runner.positions.both_sides_traded():
        return
    slot = int(elapsed) // ACTIVE_REPAIR_INTERVAL_SEC
    if slot <= runner.last_repair_slot:
        return
    runner.last_repair_slot = slot

    side = _choose_active_repair_side(runner, up_ask=up_ask, down_ask=down_ask, remaining=remaining)
    if side is None:
        return
    smaller = _smaller_share_side(runner.positions)
    ask_px = up_ask if side == "UP" else down_ask
    amount_usd = _order_amount_usd(ask_px=ask_px, state=runner.positions, cfg=cfg)
    if remaining <= 60.0 and side != smaller:
        return
    if remaining <= 30.0 and side == smaller and runner.positions.share_imbalance() > 0.15 + 1e-12 and ask_px > LAST30_FORCE_CAP + 1e-12:
        return
    current_avg_sum = _avg_sum(runner.positions)
    after_avg_sum = _avg_after_buy(runner.positions, side, ask_px, amount_usd)
    if after_avg_sum > AVG_SUM_CAP + 1e-12:
        return
    if side != smaller and current_avg_sum > 1e-12 and after_avg_sum > current_avg_sum + 0.02 + 1e-12:
        return
    _send_fak_buy(runner=runner, side=side, ask_px=ask_px, amount_usd=amount_usd, reason="active_repair", clob=clob, cfg=cfg)


def _maybe_rescue_missing_side(
    runner: WindowRunner,
    *,
    up_ask: float,
    down_ask: float,
    remaining: float,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if runner.rescue_60_attempted or remaining > 60.0:
        return
    runner.rescue_60_attempted = True
    if runner.positions.total_deals <= 0 or runner.positions.both_sides_traded():
        return
    missing = "UP" if runner.positions.orders_up == 0 else "DOWN"
    ask_px = up_ask if missing == "UP" else down_ask
    if ask_px > RESCUE_60_CAP + 1e-12:
        return
    amount_usd = max(MIN_ORDER_USD, min(float(cfg.notional_usd), LARGE_ORDER_USD)) if ask_px <= 0.40 + 1e-12 else MIN_ORDER_USD
    _send_fak_buy(runner=runner, side=missing, ask_px=ask_px, amount_usd=amount_usd, reason="rescue_60_missing", clob=clob, cfg=cfg)


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

    _maybe_bootstrap_first_leg(runner, up_ask=float(up_ask), down_ask=float(down_ask), elapsed=elapsed, clob=clob, cfg=cfg)
    _maybe_bootstrap_second_leg(runner, up_ask=float(up_ask), down_ask=float(down_ask), elapsed=elapsed, clob=clob, cfg=cfg)
    _maybe_active_repair(
        runner,
        up_ask=float(up_ask),
        down_ask=float(down_ask),
        elapsed=elapsed,
        remaining=remaining,
        clob=clob,
        cfg=cfg,
    )
    _maybe_rescue_missing_side(
        runner,
        up_ask=float(up_ask),
        down_ask=float(down_ask),
        remaining=remaining,
        clob=clob,
        cfg=cfg,
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
        strategy="bootstrap_active_repair_C_rescue_60_cap080",
        bootstrap="cheap<=0.55_then_opposite15s<=0.70",
        active_repair="15s_imb20_drop02",
        rescue="remaining60_missing<=0.80",
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
