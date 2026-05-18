"""Multi-asset BTC/ETH/XRP Up/Down — 5m+15m parallel, WS-triggered eval + heartbeat."""

from __future__ import annotations

import logging
import math
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_spot_price, fetch_binance_window_open_px
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.eval_coordinator import EvalCoordinator
from kngtop.gamma import ActiveContract, discover_active_btc_window, window_start_ts_from_slug
from kngtop.strategy_params import MispriceRule, rules_for_asset
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")
WINDOWS_TO_TRADE: tuple[int, ...] = (5, 15)
BOOT_WARMUP_DELAY_SEC = 0.0
MAX_SPOT_HISTORY_SEC = 180
ENTRY_BALANCE_FRACTION = 0.05
ENTRY_MIN_NOTIONAL_USD = 1.0
ENTRY_MAX_NOTIONAL_USD = 200.0
DISCOVERY_RETRY_SEC_WHEN_MISSING = 15.0
FAILED_RETRY_COOLDOWN_SEC = 2.0
INSUFFICIENT_BALANCE_BACKOFF_SEC = 30.0


@dataclass
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    rules: tuple[MispriceRule, ...]
    start_px: float | None = None
    trade_notional_usd: float | None = None
    rule_notional_usd: dict[str, float] = field(default_factory=dict)
    traded_rule_keys: set[str] = field(default_factory=set)
    executed_rule_sides: dict[str, str] = field(default_factory=dict)
    rule_retry_not_before: dict[str, float] = field(default_factory=dict)
    spot_history: deque[tuple[float, float]] = field(default_factory=deque)
    _exec_lock: threading.Lock = field(default_factory=threading.Lock)

    def refresh_start_px(self, cfg: KngtopConfig) -> None:
        if self.start_px is not None:
            return
        w0 = window_start_ts_from_slug(self.contract.slug)
        if w0 is None:
            return
        self.start_px = fetch_binance_window_open_px(
            symbol=self.binance_symbol,
            window_start_sec=w0,
            window_minutes=self.window_minutes,
            timeout=cfg.request_timeout_sec,
        )


@dataclass
class DiscoveryState:
    last_window_start_sec: int | None = None
    last_checked_monotonic: float = 0.0


def _planned_window_notional_usd(
    cfg: KngtopConfig,
    *,
    pair_key: str,
    window_minutes: int,
    available_balance_usdc: float | None,
) -> float:
    rules = rules_for_asset(pair_key, window_minutes)
    if not rules:
        return 0.0
    min_required_budget = ENTRY_MIN_NOTIONAL_USD
    if available_balance_usdc is None:
        return min_required_budget
    if available_balance_usdc < min_required_budget:
        return 0.0
    budget_usd = available_balance_usdc * ENTRY_BALANCE_FRACTION
    budget_usd = max(min_required_budget, budget_usd)
    budget_usd = min(ENTRY_MAX_NOTIONAL_USD, budget_usd)
    return float(budget_usd)


def _rule_notional_usd(rule: MispriceRule, runner: WindowRunner) -> float:
    if rule.key in runner.rule_notional_usd:
        return float(runner.rule_notional_usd[rule.key])
    return float(runner.trade_notional_usd or 1.0)


def _shares_for_budget(rule: MispriceRule, *, budget_usd: float, max_price_override: float | None = None) -> float:
    max_price = float(max_price_override if max_price_override is not None else (rule.market_buy_max_price or 0.0))
    if max_price <= 0:
        return 0.0
    raw_shares = float(budget_usd) / max_price
    # Keep order size within the budget while matching Polymarket amount precision.
    quantized = math.floor(raw_shares * 100.0) / 100.0
    return quantized if quantized >= 0.01 else 0.0


def _current_window_start_sec(now_ts: int, window_minutes: int) -> int:
    window_sec = max(60, int(window_minutes) * 60)
    return (int(now_ts) // window_sec) * window_sec


def _runner_matches_current_window(runner: WindowRunner | None, *, now_ts: int, window_minutes: int) -> bool:
    if runner is None:
        return False
    start_ts = window_start_ts_from_slug(runner.contract.slug)
    if start_ts is None:
        return False
    return int(start_ts) == _current_window_start_sec(now_ts, window_minutes)


def _should_discover_contract(
    runner: WindowRunner | None,
    state: DiscoveryState,
    *,
    now_ts: int,
    now_monotonic: float,
    window_minutes: int,
) -> bool:
    current_start = _current_window_start_sec(now_ts, window_minutes)
    if _runner_matches_current_window(runner, now_ts=now_ts, window_minutes=window_minutes):
        return False
    if state.last_window_start_sec != current_start:
        return True
    if runner is None and (now_monotonic - state.last_checked_monotonic) >= DISCOVERY_RETRY_SEC_WHEN_MISSING:
        return True
    return False


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.ERROR)
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


def _event(kind: str, **fields: object) -> None:
    if kind not in {"INIT", "BOOT_DELAY", "START_DEAL", "RETRY_BUY", "DEAL_WINDOW_CLOSED", "ERROR"}:
        return
    parts = [f"{k}={v}" for k, v in fields.items()]
    print(f"{kind} " + " ".join(parts), flush=True)


def _timing(stage: str, **fields: object) -> None:
    _event("TIMING", stage=stage, **fields)


def _ws_reconnected_event(feed: str, downtime_sec: float) -> None:
    _event("WS_RECONNECTED", feed=feed, downtime_sec=f"{downtime_sec:.3f}")


def _pick_token(c: ActiveContract, side: str):
    return c.up if side.upper() == "UP" else c.down


def _opposite_side(side: str) -> str:
    return "DOWN" if side.upper() == "UP" else "UP"


def _window_elapsed_ready(runner: WindowRunner, now: datetime) -> bool:
    start_ts = window_start_ts_from_slug(runner.contract.slug)
    if start_ts is None:
        return False
    elapsed = now.timestamp() - float(start_ts)
    min_elapsed = min((rule.min_elapsed_sec for rule in runner.rules), default=0)
    return elapsed >= min_elapsed


def _window_elapsed_sec(runner: WindowRunner, now: datetime) -> float | None:
    start_ts = window_start_ts_from_slug(runner.contract.slug)
    if start_ts is None:
        return None
    return now.timestamp() - float(start_ts)


def _effective_market_buy_cap(rule: MispriceRule, *, cfg: KngtopConfig, window_elapsed: float | None) -> float | None:
    del cfg, window_elapsed
    return rule.market_buy_max_price


def _signal_ready(
    rule: MispriceRule,
    *,
    now_ts: float,
    spot: float,
    start_px: float,
    ask_up: float | None,
    ask_dn: float | None,
    history: deque[tuple[float, float]],
) -> tuple[bool, float | None]:
    if start_px <= 0:
        return False, None
    gap = abs((ask_up or 0.0) - (ask_dn or 0.0)) if ask_up is not None and ask_dn is not None else None
    if rule.kind == "reclaim_up":
        if ask_up is None or spot <= start_px:
            return False, None
        if gap is None or gap < rule.gap_min:
            return False, None
        reclaimed = any(
            ts < now_ts and (now_ts - ts) <= rule.lookback_sec and hist_spot < start_px
            for ts, hist_spot in history
        )
        return reclaimed and rule.price_min <= ask_up <= rule.cheap_max, ask_up
    if rule.kind == "reclaim_dn":
        if ask_dn is None or spot >= start_px:
            return False, None
        if gap is None or gap < rule.gap_min:
            return False, None
        reclaimed = any(
            ts < now_ts and (now_ts - ts) <= rule.lookback_sec and hist_spot > start_px
            for ts, hist_spot in history
        )
        return reclaimed and rule.price_min <= ask_dn <= rule.cheap_max, ask_dn
    return False, None


def _window_result(side: str, *, start_px: float, final_spot_px: float) -> str:
    if final_spot_px > start_px:
        return "RIGHT" if side.upper() == "UP" else "WRONG"
    if final_spot_px < start_px:
        return "RIGHT" if side.upper() == "DOWN" else "WRONG"
    return "TIE"


def _finalize_runner_window(
    runner: WindowRunner | None,
    *,
    binance: BinanceCombinedTradeFeed,
    cfg: KngtopConfig,
) -> None:
    if runner is None or runner.start_px is None or not runner.executed_rule_sides:
        return
    final_spot = binance.last_price(runner.binance_symbol, max_age_sec=max(cfg.binance_max_age_sec, 30.0))
    if final_spot is None:
        final_spot = fetch_binance_spot_price(symbol=runner.binance_symbol, timeout=cfg.request_timeout_sec)
    if final_spot is None:
        return
    start_px = float(runner.start_px)
    for rule in runner.rules:
        side = runner.executed_rule_sides.get(rule.key)
        if side is None:
            continue
        _event(
            "DEAL_WINDOW_CLOSED",
            label=f"{runner.pair_key}/{runner.window_minutes}m/{rule.key}/{side}",
            result=_window_result(side, start_px=start_px, final_spot_px=float(final_spot)),
            start_px=f"{start_px:.10f}",
            final_spot_px=f"{float(final_spot):.10f}",
        )


def _append_spot_history(runner: WindowRunner, *, now_ts: float, spot: float) -> None:
    runner.spot_history.append((now_ts, spot))
    cutoff = now_ts - MAX_SPOT_HISTORY_SEC
    while runner.spot_history and runner.spot_history[0][0] < cutoff:
        runner.spot_history.popleft()


def _execute_buy(
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    shares: float,
    budget_usd: float,
    token,
    label: str,
    *,
    start_px: float,
    spot_px: float,
    pm_trigger_px: float,
    market_buy_max_price: float | None = None,
    retry_on_error_override: int | None = None,
) -> tuple[bool, str | None]:
    shares_f = float(shares)
    _event(
        "START_DEAL",
        label=label,
        shares=str(shares_f),
        budget_usd=f"{float(budget_usd):.10f}",
        start_px=f"{start_px:.10f}",
        spot_px=f"{spot_px:.10f}",
        pm_trigger_px=f"{pm_trigger_px:.10f}",
        market_px_cap=f"{float(market_buy_max_price):.2f}" if market_buy_max_price is not None else "default",
    )
    if cfg.dry_run:
        return True, None
    assert clob is not None
    started = time.perf_counter()
    try:
        _ = clob.market_buy_usdc(token, usdc=float(budget_usd), max_price=market_buy_max_price)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        lower_msg = msg.lower()
        if "no orders found to match" in lower_msg:
            reason = "no_match"
        elif "not enough balance / allowance" in lower_msg:
            reason = "insufficient_balance"
        else:
            reason = "error"
        _event(
            "RETRY_BUY",
            label=label,
            elapsed_ms=f"{(time.perf_counter() - started) * 1000.0:.1f}",
            reason=reason,
        )
        if reason != "no_match":
            _event("ERROR", stage="market_buy", label=label, error=msg)
        return False, reason
    return True, None


def _tick_runner(
    runner: WindowRunner | None,
    *,
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    runtime_state: dict[str, float],
) -> None:
    if runner is None:
        return
    with runner._exec_lock:
        if runner.start_px is None or runner.trade_notional_usd is None:
            return
        if runner.trade_notional_usd <= 0:
            return
        now = datetime.now(timezone.utc)
        spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
        if spot is None:
            return
        _append_spot_history(runner, now_ts=now.timestamp(), spot=spot)
        up_id = runner.contract.up.token_id
        dn_id = runner.contract.down.token_id
        quote_up = poly.best_bid_ask_for(up_id, max_age_sec=cfg.poly_mid_max_age_sec)
        quote_dn = poly.best_bid_ask_for(dn_id, max_age_sec=cfg.poly_mid_max_age_sec)
        ask_up = quote_up[1] if quote_up is not None else None
        ask_dn = quote_dn[1] if quote_dn is not None else None
        if runner.traded_rule_keys:
            return
        start = float(runner.start_px)
        window_elapsed = _window_elapsed_sec(runner, now)
        now_monotonic = time.perf_counter()
        for rule in runner.rules:
            if rule.key in runner.traded_rule_keys:
                continue
            if now_monotonic < runner.rule_retry_not_before.get(rule.key, 0.0):
                continue
            ready, trigger_px = _signal_ready(
                rule,
                now_ts=now.timestamp(),
                spot=spot,
                start_px=start,
                ask_up=ask_up,
                ask_dn=ask_dn,
                history=runner.spot_history,
            )
            if not ready:
                continue
            if window_elapsed is None:
                continue
            if window_elapsed < rule.min_elapsed_sec or window_elapsed > rule.max_elapsed_sec:
                continue
            budget_usd = _rule_notional_usd(rule, runner)
            effective_cap = _effective_market_buy_cap(rule, cfg=cfg, window_elapsed=window_elapsed)
            shares = _shares_for_budget(rule, budget_usd=budget_usd, max_price_override=effective_cap)
            if shares <= 0:
                continue
            tok = _pick_token(runner.contract, rule.side)
            label = f"{runner.pair_key}/{runner.window_minutes}m/{rule.key}/{rule.side}"
            executed, reason = _execute_buy(
                clob,
                cfg,
                shares,
                budget_usd,
                tok,
                label,
                start_px=start,
                spot_px=spot,
                pm_trigger_px=float(trigger_px),
                market_buy_max_price=effective_cap,
                retry_on_error_override=rule.retry_on_error_override,
            )
            if not executed:
                cooldown = FAILED_RETRY_COOLDOWN_SEC
                if reason == "insufficient_balance":
                    cooldown = INSUFFICIENT_BALANCE_BACKOFF_SEC
                    runtime_state["insufficient_balance_not_before"] = time.perf_counter() + cooldown
                runner.rule_retry_not_before[rule.key] = time.perf_counter() + cooldown
                continue
            runner.traded_rule_keys.add(rule.key)
            runner.executed_rule_sides[rule.key] = rule.side
            break


def _pairs_summary(cfg: KngtopConfig) -> str:
    return ",".join(f"{k}:{s}" for k, s in cfg.trading_pairs)


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[tuple[str, int], WindowRunner | None],
    discovery: dict[tuple[str, int], DiscoveryState],
    subscribed_asset_ids: set[str],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    runtime_state: dict[str, float],
) -> None:
    timeout = cfg.request_timeout_sec
    sym_for_pair = dict(cfg.trading_pairs)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    now_monotonic = time.perf_counter()
    refreshed_keys: list[tuple[str, int]] = []

    for pair_key in sym_for_pair:
        gamma_sym = pair_key.lower()
        bs_sym = sym_for_pair[pair_key]
        for wm in WINDOWS_TO_TRADE:
            rk = (pair_key, wm)
            state = discovery.setdefault(rk, DiscoveryState())
            cur = runners.get(rk)
            current_start = _current_window_start_sec(now_ts, wm)
            if not _should_discover_contract(
                cur,
                state,
                now_ts=now_ts,
                now_monotonic=now_monotonic,
                window_minutes=wm,
            ):
                continue
            state.last_window_start_sec = current_start
            state.last_checked_monotonic = now_monotonic
            t0 = time.perf_counter()
            if cur is not None and not _runner_matches_current_window(cur, now_ts=now_ts, window_minutes=wm):
                _finalize_runner_window(cur, binance=binance, cfg=cfg)
            c = discover_active_btc_window(market_symbol=gamma_sym, window_minutes=wm, timeout=timeout)
            _timing(
                "gamma_discovery",
                pair=pair_key,
                window_minutes=str(wm),
                elapsed_ms=f"{(time.perf_counter() - t0) * 1000.0:.1f}",
                found=str(c is not None).lower(),
            )
            if c is None:
                runners[rk] = None
                continue
            if cur is None or cur.contract.slug != c.slug:
                runners[rk] = WindowRunner(
                    pair_key=pair_key,
                    binance_symbol=bs_sym,
                    contract=c,
                    window_minutes=wm,
                    rules=rules_for_asset(pair_key, wm),
                )
                refreshed_keys.append(rk)

    available_balance_usdc: float | None = None
    if refreshed_keys and clob is not None:
        t0 = time.perf_counter()
        available_balance_usdc = clob.available_balance_usdc()
        _timing(
            "balance_fetch",
            elapsed_ms=f"{(time.perf_counter() - t0) * 1000.0:.1f}",
            available_balance="none" if available_balance_usdc is None else f"{available_balance_usdc:.6f}",
        )
    for rk in refreshed_keys:
        rv = runners.get(rk)
        if rv is None:
            continue
        rv.trade_notional_usd = _planned_window_notional_usd(
            cfg,
            pair_key=rv.pair_key,
            window_minutes=rv.window_minutes,
            available_balance_usdc=available_balance_usdc,
        )
        if clob is not None:
            clob.prewarm_market_metadata(rv.contract.up)
            clob.prewarm_market_metadata(rv.contract.down)

    asset_ids: list[str] = []
    for rv in runners.values():
        if rv is not None:
            asset_ids.extend([rv.contract.up.token_id, rv.contract.down.token_id])
    next_asset_ids = set(asset_ids)
    if next_asset_ids != subscribed_asset_ids:
        poly.set_assets(asset_ids)
        subscribed_asset_ids.clear()
        subscribed_asset_ids.update(next_asset_ids)

    for rv in runners.values():
        if rv is not None:
            rv.refresh_start_px(cfg)

    if now_monotonic < runtime_state.get("insufficient_balance_not_before", 0.0):
        return

    for pair_key in sym_for_pair:
        for wm in WINDOWS_TO_TRADE:
            try:
                _tick_runner(
                    runners.get((pair_key, wm)),
                    poly=poly,
                    binance=binance,
                    clob=clob,
                    cfg=cfg,
                    runtime_state=runtime_state,
                )
            except Exception as exc:  # noqa: BLE001
                _event("ERROR", stage="tick", pair=str(pair_key), window_minutes=str(wm), error=str(exc))


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    coord = EvalCoordinator(debounce_sec=cfg.eval_debounce_sec, heartbeat_sec=cfg.poll_interval_sec)

    bin_syms_sorted = sorted({s for _, s in cfg.trading_pairs})

    poly = MarketWsFeed(
        on_quote_update=coord.notify,
        on_ws_reconnect=lambda dt: _ws_reconnected_event("polymarket", dt),
    )
    binance = BinanceCombinedTradeFeed(
        bin_syms_sorted,
        on_trade=lambda _sym: coord.notify(),
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
        t0 = time.perf_counter()
        clob = KngtopClob(
            private_key=cfg.private_key,
            funder=cfg.funder,
            signature_type=cfg.signature_type,
            relayer_api_key=cfg.relayer_api_key,
            relayer_secret=cfg.relayer_secret,
            relayer_passphrase=cfg.relayer_passphrase,
            market_buy_max_price=cfg.market_buy_max_price,
        )
        _timing("clob_init", elapsed_ms=f"{(time.perf_counter() - t0) * 1000.0:.1f}")

    runners: dict[tuple[str, int], WindowRunner | None] = {}
    discovery: dict[tuple[str, int], DiscoveryState] = {}
    subscribed_asset_ids: set[str] = set()
    runtime_state: dict[str, float] = {}

    _event(
        "INIT",
        scope="BOOT",
        pairs=_pairs_summary(cfg),
        dry_run=str(cfg.dry_run).lower(),
        heartbeat_sec=str(cfg.poll_interval_sec),
        debounce_sec=str(cfg.eval_debounce_sec),
        notional_usd=str(cfg.notional_usd),
        retry_on_error=str(cfg.order_retry_on_error),
        ws_rest_poll=str(cfg.ws_rest_poll_enabled).lower(),
        ws_rest_poll_interval_sec=str(cfg.ws_rest_poll_interval_sec),
    )
    _event("BOOT_DELAY", seconds=str(int(BOOT_WARMUP_DELAY_SEC)))
    if BOOT_WARMUP_DELAY_SEC > 0:
        time.sleep(BOOT_WARMUP_DELAY_SEC)

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
            _event("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
