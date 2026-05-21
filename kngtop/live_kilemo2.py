"""BTC 5m live bot for the current S0184 winner-side start-window rule."""

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

ENTRY_MIN_PRICE = 0.45
ENTRY_MAX_PRICE = 0.55
ENTRY_MAX_ELAPSED_SEC = 25.0
MOVE_FROM_OPEN_MIN_USD = 1.0
MIN_ORDER_NOTIONAL_USD = 1.0
ORDER_SIZE_BALANCE_FRACTION = 0.10
BUY_FAK_PRICE = 0.58
MAX_TAKER_PRICE = 0.99
EXIT_SELL_PRICE: float | None = None


@dataclass(slots=True)
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    window_open_px: float | None = None
    attempted: bool = False
    attempt_side: str | None = None
    attempt_elapsed_sec: float | None = None
    trigger_ask_px: float | None = None
    trigger_max_price: float | None = None
    btc_move_from_open: float | None = None
    order_notional_usd: float = MIN_ORDER_NOTIONAL_USD
    estimated_shares: float = 0.0
    exited: bool = False
    exit_order_id: str | None = None
    order_id: str | None = None
    stop_reason: str | None = None

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)


@dataclass(frozen=True, slots=True)
class SignalDecision:
    side: str
    ask_px: float
    btc_spot: float
    window_open_px: float
    btc_move_from_open: float
    max_price: float


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


def _winner_side(*, spot_px: float, window_open_px: float) -> str | None:
    delta = float(spot_px) - float(window_open_px)
    if delta > 1e-12:
        return "UP"
    if delta < -1e-12:
        return "DOWN"
    return None


def evaluate_signal(
    *,
    window_open_px: float,
    spot_px: float,
    up_ask: float,
    down_ask: float,
) -> SignalDecision | None:
    side = _winner_side(spot_px=float(spot_px), window_open_px=float(window_open_px))
    if side is None:
        return None
    ask_px = float(up_ask) if side == "UP" else float(down_ask)
    if ask_px < ENTRY_MIN_PRICE - 1e-12 or ask_px > ENTRY_MAX_PRICE + 1e-12:
        return None
    btc_move_from_open = abs(float(spot_px) - float(window_open_px))
    if btc_move_from_open + 1e-12 < MOVE_FROM_OPEN_MIN_USD:
        return None
    return SignalDecision(
        side=side,
        ask_px=ask_px,
        btc_spot=float(spot_px),
        window_open_px=float(window_open_px),
        btc_move_from_open=btc_move_from_open,
        max_price=min(MAX_TAKER_PRICE, BUY_FAK_PRICE),
    )


def _token_for_side(runner: WindowRunner, side: str) -> TokenMarket:
    return runner.contract.up if str(side).upper() == "UP" else runner.contract.down


def _window_order_notional_usd(*, clob: KngtopClob | None, cfg: KngtopConfig) -> float:
    if cfg.dry_run or clob is None:
        return max(MIN_ORDER_NOTIONAL_USD, float(cfg.notional_usd))
    available = clob.available_balance_usdc()
    if available is None:
        return max(MIN_ORDER_NOTIONAL_USD, float(cfg.notional_usd))
    return max(MIN_ORDER_NOTIONAL_USD, float(available) * ORDER_SIZE_BALANCE_FRACTION)


def _send_fak_buy(
    *,
    runner: WindowRunner,
    decision: SignalDecision,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> bool:
    side = decision.side
    token = _token_for_side(runner, side)
    if cfg.dry_run or clob is None:
        runner.order_id = None
        return True

    attempts = max(1, int(cfg.order_retry_on_error) + 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = clob.market_buy_usdc(token, runner.order_notional_usd, max_price=decision.max_price)
            runner.order_id = _extract_order_id(payload)
            return True
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5)
    if last_error is not None:
        _log_tag("ERROR", slug=runner.contract.slug, stage="buy", side=side, error=str(last_error))
    return False


def _send_fak_sell(
    *,
    runner: WindowRunner,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    bid_px: float,
) -> bool:
    side = runner.attempt_side
    if side is None or runner.estimated_shares <= 0:
        return False
    token = _token_for_side(runner, side)
    if cfg.dry_run or clob is None:
        runner.exit_order_id = None
        return True

    attempts = max(1, int(cfg.order_retry_on_error) + 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = clob.market_sell_shares_fak(token, shares=runner.estimated_shares, min_price=EXIT_SELL_PRICE)
            runner.exit_order_id = _extract_order_id(payload)
            return True
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5)
    if last_error is not None:
        _log_tag("ERROR", slug=runner.contract.slug, stage="sell", side=side, error=str(last_error))
    return False


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
            order_notional_usd=_window_order_notional_usd(clob=clob, cfg=cfg),
        )
        _log_tag("INIT", slug=contract.slug, start_sec=str(start_sec), order_notional_usd=f"{runners[start_sec].order_notional_usd:.2f}")


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
        if runner.attempt_side is not None and not runner.exited:
            _log_tag(
                "DEAL END",
                slug=runner.contract.slug,
                side=runner.attempt_side,
                result="loss",
                reason="expired_without_tp",
                order_notional_usd=f"{runner.order_notional_usd:.2f}",
                pnl_usd_est=f"{-runner.order_notional_usd:.2f}",
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
    spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
    if spot is None:
        return
    up_quote = poly.best_bid_ask_for(runner.contract.up.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    down_quote = poly.best_bid_ask_for(runner.contract.down.token_id, max_age_sec=cfg.poly_mid_max_age_sec)
    if up_quote is None or down_quote is None:
        return
    up_bid, up_ask = up_quote
    down_bid, down_ask = down_quote
    if runner.attempted:
        if runner.exited or runner.attempt_side is None:
            return
        held_bid = float(up_bid) if runner.attempt_side == "UP" else float(down_bid)
        if EXIT_SELL_PRICE is not None and held_bid + 1e-12 >= EXIT_SELL_PRICE:
            if _send_fak_sell(runner=runner, clob=clob, cfg=cfg, bid_px=held_bid):
                runner.exited = True
                runner.stop_reason = "take_profit_exit"
                pnl_usd = runner.estimated_shares * EXIT_SELL_PRICE - runner.order_notional_usd
                _log_tag(
                    "DEAL END",
                    slug=runner.contract.slug,
                    side=runner.attempt_side,
                    result="success",
                    reason="tp85",
                    order_notional_usd=f"{runner.order_notional_usd:.2f}",
                    exit_price=f"{EXIT_SELL_PRICE:.2f}",
                    pnl_usd_est=f"{pnl_usd:.2f}",
                )
        return
    if elapsed > ENTRY_MAX_ELAPSED_SEC + 1e-12:
        runner.attempted = True
        runner.stop_reason = "entry_window_closed"
        return
    if remaining <= float(cfg.order_cutoff_remaining_sec):
        runner.attempted = True
        runner.stop_reason = "late_window"
        return
    decision = evaluate_signal(
        window_open_px=float(runner.window_open_px),
        spot_px=float(spot),
        up_ask=float(up_ask),
        down_ask=float(down_ask),
    )
    if decision is None:
        return
    if _send_fak_buy(runner=runner, decision=decision, clob=clob, cfg=cfg):
        runner.attempted = True
        runner.attempt_side = decision.side
        runner.attempt_elapsed_sec = elapsed
        runner.trigger_ask_px = decision.ask_px
        runner.trigger_max_price = decision.max_price
        runner.btc_move_from_open = decision.btc_move_from_open
        runner.estimated_shares = runner.order_notional_usd / max(decision.max_price, 1e-9)
        runner.stop_reason = "signal_submitted"
        _log_tag(
            "DEAL START",
            slug=runner.contract.slug,
            side=decision.side,
            order_notional_usd=f"{runner.order_notional_usd:.2f}",
            ask_px=f"{decision.ask_px:.4f}",
            max_price=f"{decision.max_price:.4f}",
            btc_move_from_open=f"{decision.btc_move_from_open:.2f}",
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
        strategy="s0184_start_window_winner",
        order_type="fak",
        order_size_rule="10pct_balance_min1",
        ask_band=f"{ENTRY_MIN_PRICE:.2f}-{ENTRY_MAX_PRICE:.2f}",
        btc_move_min_usd=f"{MOVE_FROM_OPEN_MIN_USD:.2f}",
        entry_max_elapsed_sec=f"{ENTRY_MAX_ELAPSED_SEC:.1f}",
        fak_buy_price=f"{BUY_FAK_PRICE:.2f}",
        exit_sell_price="disabled" if EXIT_SELL_PRICE is None else f"{EXIT_SELL_PRICE:.2f}",
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _log_tag("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
