"""BTC 5m live bot for the KILEMO_1 cheap-hit + close-to-open + volume AND move strategy."""

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

CHEAP_TRIGGER_MAX = 0.15
ORDER_LIMIT_PRICE = 0.25
ORDER_NOTIONAL_USD = 1.0
CLOSE_TO_OPEN_MAX_USD = 30.0
VOLUME_LOOKBACK_SEC = 20
VOLUME_RATIO_MIN = 1.4
VOLUME_ALIGN_LOOKBACK_SEC = 5
MOVE_LOOKBACK_SEC = 20
MOVE_MIN_USD = 2.0


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
    trigger_mid: float | None = None
    order_id: str | None = None
    stop_reason: str | None = None

    def start_sec(self) -> int | None:
        return window_start_ts_from_slug(self.contract.slug)


@dataclass(frozen=True, slots=True)
class SignalDecision:
    side: str
    cheap_mid: float
    abs_from_open: float
    side_vs_open: float
    align_5s: float
    align_20s: float
    volume_ratio: float
    move_gate: bool
    volume_gate: bool


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


def _cheap_side(mid_up: float, mid_dn: float) -> tuple[str, float] | None:
    up_hit = float(mid_up) <= CHEAP_TRIGGER_MAX + 1e-12
    dn_hit = float(mid_dn) <= CHEAP_TRIGGER_MAX + 1e-12
    if up_hit and dn_hit:
        return ("UP", float(mid_up)) if float(mid_up) <= float(mid_dn) else ("DOWN", float(mid_dn))
    if up_hit:
        return "UP", float(mid_up)
    if dn_hit:
        return "DOWN", float(mid_dn)
    return None


def _side_sign(side: str) -> int:
    return 1 if str(side).upper() == "UP" else -1


def evaluate_signal(
    *,
    window_open_px: float,
    spot_px: float,
    mid_up: float,
    mid_dn: float,
    price_then_now_5s: tuple[float, float] | None,
    price_then_now_20s: tuple[float, float] | None,
    volume_ratio_20s: float | None,
) -> SignalDecision | None:
    cheap = _cheap_side(mid_up, mid_dn)
    if cheap is None:
        return None
    side, cheap_mid = cheap
    sign = _side_sign(side)
    abs_from_open = abs(float(spot_px) - float(window_open_px))
    if abs_from_open > CLOSE_TO_OPEN_MAX_USD + 1e-12:
        return None
    side_vs_open = float(sign) * (float(spot_px) - float(window_open_px))
    align_5s = 0.0
    if price_then_now_5s is not None:
        now_5, past_5 = price_then_now_5s
        align_5s = float(sign) * (float(now_5) - float(past_5))
    align_20s = 0.0
    if price_then_now_20s is not None:
        now_20, past_20 = price_then_now_20s
        align_20s = float(sign) * (float(now_20) - float(past_20))
    vol_ratio = 0.0 if volume_ratio_20s is None else float(volume_ratio_20s)
    volume_gate = vol_ratio + 1e-12 >= VOLUME_RATIO_MIN and (align_5s >= -1e-12 or side_vs_open >= -1e-12)
    move_gate = align_20s + 1e-12 >= MOVE_MIN_USD or side_vs_open >= -1e-12
    if not (volume_gate and move_gate):
        return None
    return SignalDecision(
        side=side,
        cheap_mid=cheap_mid,
        abs_from_open=abs_from_open,
        side_vs_open=side_vs_open,
        align_5s=align_5s,
        align_20s=align_20s,
        volume_ratio=vol_ratio,
        move_gate=move_gate,
        volume_gate=volume_gate,
    )


def _token_for_side(runner: WindowRunner, side: str) -> TokenMarket:
    return runner.contract.up if str(side).upper() == "UP" else runner.contract.down


def _send_fak_buy(
    *,
    runner: WindowRunner,
    decision: SignalDecision,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    ask_px: float,
) -> bool:
    side = decision.side
    token = _token_for_side(runner, side)
    if cfg.dry_run or clob is None:
        _log_tag(
            "FAK BUY",
            slug=runner.contract.slug,
            side=side,
            mode="dry_run",
            notional_usd=f"{ORDER_NOTIONAL_USD:.2f}",
            limit_px=f"{ORDER_LIMIT_PRICE:.2f}",
            ask_px=f"{ask_px:.2f}",
        )
        runner.order_id = None
        return True

    attempts = max(1, int(cfg.order_retry_on_error) + 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = clob.market_buy_usdc(token, ORDER_NOTIONAL_USD, max_price=ORDER_LIMIT_PRICE)
            runner.order_id = _extract_order_id(payload)
            _log_tag(
                "FAK BUY",
                slug=runner.contract.slug,
                side=side,
                attempt=str(attempt),
                notional_usd=f"{ORDER_NOTIONAL_USD:.2f}",
                limit_px=f"{ORDER_LIMIT_PRICE:.2f}",
                ask_px=f"{ask_px:.2f}",
                order_id=runner.order_id or "-",
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
                error=str(exc),
            )
            time.sleep(0.1)
    if last_error is not None:
        _log_tag("WINDOW", slug=runner.contract.slug, state="buy_failed", error=str(last_error))
    return False


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
        runners.pop(start_sec, None)
        _log_tag("WINDOW", slug=runner.contract.slug, state="dropped", reason="expired")


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
    if runner.attempted:
        return
    if remaining <= float(cfg.order_cutoff_remaining_sec):
        _log_tag("WINDOW", slug=runner.contract.slug, state="late_window", remaining_sec=f"{remaining:.1f}")
        runner.attempted = True
        runner.stop_reason = "late_window"
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
    mid_up = (float(up_bid) + float(up_ask)) / 2.0
    mid_down = (float(down_bid) + float(down_ask)) / 2.0
    price_then_now_5s = binance.price_then_now(
        runner.binance_symbol,
        lookback_sec=VOLUME_ALIGN_LOOKBACK_SEC,
        max_age_sec=cfg.binance_max_age_sec,
    )
    price_then_now_20s = binance.price_then_now(
        runner.binance_symbol,
        lookback_sec=MOVE_LOOKBACK_SEC,
        max_age_sec=cfg.binance_max_age_sec,
    )
    volume_ratio_20s = binance.current_volume_ratio(
        runner.binance_symbol,
        lookback_sec=VOLUME_LOOKBACK_SEC,
        max_age_sec=cfg.binance_max_age_sec,
    )
    decision = evaluate_signal(
        window_open_px=float(runner.window_open_px),
        spot_px=float(spot),
        mid_up=mid_up,
        mid_dn=mid_down,
        price_then_now_5s=price_then_now_5s,
        price_then_now_20s=price_then_now_20s,
        volume_ratio_20s=volume_ratio_20s,
    )
    if decision is None:
        return
    side_ask = up_ask if decision.side == "UP" else down_ask
    _log_tag(
        "SIGNAL",
        slug=runner.contract.slug,
        side=decision.side,
        cheap_mid=f"{decision.cheap_mid:.4f}",
        ask_px=f"{side_ask:.4f}",
        btc_spot=f"{spot:.2f}",
        window_open_px=f"{float(runner.window_open_px):.2f}",
        abs_from_open=f"{decision.abs_from_open:.2f}",
        side_vs_open=f"{decision.side_vs_open:.2f}",
        align_5s=f"{decision.align_5s:.2f}",
        align_20s=f"{decision.align_20s:.2f}",
        volume_ratio=f"{decision.volume_ratio:.4f}",
        volume_gate=str(decision.volume_gate).lower(),
        move_gate=str(decision.move_gate).lower(),
    )
    if _send_fak_buy(runner=runner, decision=decision, clob=clob, cfg=cfg, ask_px=side_ask):
        runner.attempted = True
        runner.attempt_side = decision.side
        runner.attempt_elapsed_sec = elapsed
        runner.trigger_mid = decision.cheap_mid
        runner.stop_reason = "signal_submitted"


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
        strategy="cheap_hit_close_volume_and_move",
        order_type="fak",
        order_notional_usd=f"{ORDER_NOTIONAL_USD:.2f}",
        order_limit_px=f"{ORDER_LIMIT_PRICE:.2f}",
    )

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _log_tag("WINDOW", state="main_loop_error", error=str(exc))


if __name__ == "__main__":
    main()
