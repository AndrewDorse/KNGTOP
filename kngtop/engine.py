"""Multi-asset BTC/ETH/XRP Up/Down — 5m+15m parallel, WS-triggered eval + heartbeat."""

from __future__ import annotations

import logging
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_window_open_px
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.eval_coordinator import EvalCoordinator
from kngtop.gamma import ActiveContract, discover_active_btc_window, window_start_ts_from_slug
from kngtop.strategy_params import MispriceRule, rules_for_asset
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")
BALANCE_NOTIONAL_FRACTION = 0.07
ALT_BALANCE_NOTIONAL_FRACTION = 0.07
WINDOWS_TO_TRADE: tuple[int, ...] = (5,)
ALT_BALANCE_ASSETS = frozenset({"DOGE", "BNB", "HYPE", "LINK"})
MIN_WINDOW_PROGRESS_FRACTION = 0.20
ENTRY_MARKET_FRACTION = 0.50
ENTRY_LIMIT_FRACTION = 0.50
ENTRY_LIMIT_PRICE = 0.20
BOOT_WARMUP_DELAY_SEC = 0.0
MAX_SPOT_HISTORY_SEC = 180
MIN_MARKET_BUY_USDC = 1.0
MIN_LIMIT_SHARES = 5.0


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


def _planned_window_notional_usd(
    cfg: KngtopConfig,
    *,
    pair_key: str,
    window_minutes: int,
    available_balance_usdc: float | None,
) -> float:
    floor_usd = max(1.0, float(cfg.notional_usd))
    if int(window_minutes) >= 60:
        return floor_usd
    if available_balance_usdc is None:
        return floor_usd
    frac = ALT_BALANCE_NOTIONAL_FRACTION if pair_key.upper() in ALT_BALANCE_ASSETS else BALANCE_NOTIONAL_FRACTION
    return max(floor_usd, available_balance_usdc * frac)


def _rule_notional_usd(rule: MispriceRule, runner: WindowRunner) -> float:
    if rule.key in runner.rule_notional_usd:
        return float(runner.rule_notional_usd[rule.key])
    return float(runner.trade_notional_usd or 1.0)


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.ERROR)
    logging.basicConfig(
        level=lv,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def _event(kind: str, **fields: object) -> None:
    parts = [f"{k}={v}" for k, v in fields.items()]
    print(f"{kind} " + " ".join(parts), flush=True)


def _timing(stage: str, **fields: object) -> None:
    _event("TIMING", stage=stage, **fields)


def _ws_reconnected_event(feed: str, downtime_sec: float) -> None:
    _event("WS_RECONNECTED", feed=feed, downtime_sec=f"{downtime_sec:.3f}")


def _pick_token(c: ActiveContract, side: str):
    return c.up if side.upper() == "UP" else c.down


def _window_elapsed_ready(runner: WindowRunner, now: datetime) -> bool:
    start_ts = window_start_ts_from_slug(runner.contract.slug)
    if start_ts is None:
        return False
    elapsed = now.timestamp() - float(start_ts)
    min_elapsed = float(runner.window_minutes) * 60.0 * MIN_WINDOW_PROGRESS_FRACTION
    return elapsed >= min_elapsed


def _signal_ready(
    rule: MispriceRule,
    *,
    spot: float,
    start_px: float,
    mid_up: float | None,
    mid_dn: float | None,
) -> tuple[bool, float | None]:
    diff_bps = abs((spot - start_px) / start_px * 10_000.0) if start_px > 0 else 0.0
    if diff_bps > rule.close_bps:
        return False, None
    if rule.kind == "close_up":
        if mid_up is None:
            return False, None
        return spot > start_px and mid_up <= rule.cheap_max, mid_up
    if rule.kind == "close_dn":
        if mid_dn is None:
            return False, None
        return spot < start_px and mid_dn <= rule.cheap_max, mid_dn
    return False, None


def _append_spot_history(runner: WindowRunner, *, now_ts: float, spot: float) -> None:
    runner.spot_history.append((now_ts, spot))
    cutoff = now_ts - MAX_SPOT_HISTORY_SEC
    while runner.spot_history and runner.spot_history[0][0] < cutoff:
        runner.spot_history.popleft()


def _revert_signal_ready(
    rule: MispriceRule,
    *,
    now_ts: float,
    spot: float,
    start_px: float,
    trigger_px: float | None,
    history: deque[tuple[float, float]],
) -> bool:
    if trigger_px is None or trigger_px > rule.cheap_max:
        return False
    if start_px <= 0:
        return False
    diff_bps = (spot - start_px) / start_px * 10_000.0
    if rule.kind == "revert_up":
        if diff_bps < 0.0 or diff_bps > rule.close_bps:
            return False
        return any(
            ts < now_ts and now_ts - ts <= rule.lookback_sec and (hist_spot - start_px) / start_px * 10_000.0 >= rule.lead_bps
            for ts, hist_spot in history
        )
    if rule.kind == "revert_dn":
        if diff_bps > 0.0 or -diff_bps > rule.close_bps:
            return False
        return any(
            ts < now_ts and now_ts - ts <= rule.lookback_sec and (hist_spot - start_px) / start_px * 10_000.0 <= -rule.lead_bps
            for ts, hist_spot in history
        )
    return False


def _flip_signal_ready(
    rule: MispriceRule,
    *,
    now_ts: float,
    spot: float,
    start_px: float,
    trigger_px: float | None,
    history: deque[tuple[float, float]],
) -> bool:
    if trigger_px is None or trigger_px > rule.cheap_max or start_px <= 0:
        return False
    if rule.kind == "flip_up":
        if spot <= start_px:
            return False
        return any(ts < now_ts and now_ts - ts <= rule.lookback_sec and hist_spot < start_px for ts, hist_spot in history)
    if rule.kind == "flip_dn":
        if spot >= start_px:
            return False
        return any(ts < now_ts and now_ts - ts <= rule.lookback_sec and hist_spot > start_px for ts, hist_spot in history)
    return False


def _execute_buy(
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    usdc: float,
    token,
    label: str,
    *,
    start_px: float,
    spot_px: float,
    pm_trigger_px: float,
    market_buy_max_price: float | None = None,
    retry_on_error_override: int | None = None,
) -> None:
    usdc_f = float(usdc)
    market_usdc = usdc_f * ENTRY_MARKET_FRACTION
    limit_usdc = usdc_f * ENTRY_LIMIT_FRACTION
    if market_buy_max_price is not None:
        market_usdc = usdc_f
        limit_usdc = 0.0
    elif market_usdc < MIN_MARKET_BUY_USDC or (limit_usdc / ENTRY_LIMIT_PRICE) < MIN_LIMIT_SHARES:
        market_usdc = usdc_f
        limit_usdc = 0.0
    _event(
        "DEAL_START",
        label=label,
        notional=str(usdc_f),
        token=token.token_id[:16],
        start_px=f"{start_px:.10f}",
        spot_px=f"{spot_px:.10f}",
        pm_trigger_px=f"{pm_trigger_px:.10f}",
        market_notional=f"{market_usdc:.10f}",
        limit_notional=f"{limit_usdc:.10f}",
        limit_px=f"{ENTRY_LIMIT_PRICE:.2f}",
        market_px_cap=f"{float(market_buy_max_price):.2f}" if market_buy_max_price is not None else "default",
    )
    if cfg.dry_run:
        return
    assert clob is not None
    limit_error: Exception | None = None
    if limit_usdc > 0.0:
        try:
            _ = clob.limit_buy(token, price=ENTRY_LIMIT_PRICE, usdc=limit_usdc)
        except Exception as exc:  # noqa: BLE001
            limit_error = exc
            _event("DEAL_FAIL", label=label, attempt=1, leg="limit", error=str(exc))
    retries = cfg.order_retry_on_error if retry_on_error_override is None else int(retry_on_error_override)
    attempts = 1 + max(0, retries)
    for attempt in range(1, attempts + 1):
        try:
            _ = clob.market_buy_usdc(token, market_usdc, max_price=market_buy_max_price)
        except Exception as exc:  # noqa: BLE001
            _event("DEAL_FAIL", label=label, attempt=attempt, leg="market", error=str(exc))
            if attempt >= attempts:
                raise
            _event("DEAL_RETRY", label=label, attempt=attempt, error=str(exc))
            time.sleep(0.35)
            continue
        if limit_error is None:
            return
        raise RuntimeError(str(limit_error))


def _tick_runner(
    runner: WindowRunner | None,
    *,
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if runner is None:
        return
    with runner._exec_lock:
        if runner.start_px is None or runner.trade_notional_usd is None:
            return
        now = datetime.now(timezone.utc)
        spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
        if spot is None:
            return
        _append_spot_history(runner, now_ts=now.timestamp(), spot=spot)
        up_id = runner.contract.up.token_id
        dn_id = runner.contract.down.token_id
        mid_up = poly.mid_for(up_id, max_age_sec=cfg.poly_mid_max_age_sec)
        mid_dn = poly.mid_for(dn_id, max_age_sec=cfg.poly_mid_max_age_sec)
        start = float(runner.start_px)
        elapsed_ready = _window_elapsed_ready(runner, now)
        for rule in runner.rules:
            if rule.key in runner.traded_rule_keys:
                continue
            ready, trigger_px = _signal_ready(
                rule,
                spot=spot,
                start_px=start,
                mid_up=mid_up,
                mid_dn=mid_dn,
            )
            if not ready:
                continue
            if not elapsed_ready:
                _event(
                    "SIGNAL_BLOCKED",
                    label=f"{runner.pair_key}/{runner.window_minutes}m/{rule.key}/{rule.side}",
                    reason="min_window_progress",
                    start_px=f"{start:.10f}",
                    spot_px=f"{spot:.10f}",
                    pm_trigger_px=f"{float(trigger_px):.10f}",
                )
                continue
            tok = _pick_token(runner.contract, rule.side)
            label = f"{runner.pair_key}/{runner.window_minutes}m/{rule.key}/{rule.side}"
            _execute_buy(
                clob,
                cfg,
                _rule_notional_usd(rule, runner),
                tok,
                label,
                start_px=start,
                spot_px=spot,
                pm_trigger_px=float(trigger_px),
                market_buy_max_price=rule.market_buy_max_price,
                retry_on_error_override=rule.retry_on_error_override,
            )
            runner.traded_rule_keys.add(rule.key)


def _pairs_summary(cfg: KngtopConfig) -> str:
    return ",".join(f"{k}:{s}" for k, s in cfg.trading_pairs)


def _run_iteration(
    cfg: KngtopConfig,
    *,
    runners: dict[tuple[str, int], WindowRunner | None],
    poly: MarketWsFeed,
    binance: BinanceCombinedTradeFeed,
    clob: KngtopClob | None,
) -> None:
    timeout = cfg.request_timeout_sec
    sym_for_pair = dict(cfg.trading_pairs)
    available_balance_usdc: float | None = None
    if clob is not None:
        t0 = time.perf_counter()
        available_balance_usdc = clob.available_balance_usdc()
        _timing(
            "balance_fetch",
            elapsed_ms=f"{(time.perf_counter() - t0) * 1000.0:.1f}",
            available_balance="none" if available_balance_usdc is None else f"{available_balance_usdc:.6f}",
        )

    for pair_key in sym_for_pair:
        gamma_sym = pair_key.lower()
        bs_sym = sym_for_pair[pair_key]
        for wm in WINDOWS_TO_TRADE:
            t0 = time.perf_counter()
            c = discover_active_btc_window(market_symbol=gamma_sym, window_minutes=wm, timeout=timeout)
            _timing(
                "gamma_discovery",
                pair=pair_key,
                window_minutes=str(wm),
                elapsed_ms=f"{(time.perf_counter() - t0) * 1000.0:.1f}",
                found=str(c is not None).lower(),
            )
            rk = (pair_key, wm)
            if c is None:
                runners[rk] = None
                continue
            cur = runners.get(rk)
            if cur is None or cur.contract.slug != c.slug:
                runners[rk] = WindowRunner(
                    pair_key=pair_key,
                    binance_symbol=bs_sym,
                    contract=c,
                    window_minutes=wm,
                    rules=rules_for_asset(pair_key, wm),
                    trade_notional_usd=_planned_window_notional_usd(
                        cfg,
                        pair_key=pair_key,
                        window_minutes=wm,
                        available_balance_usdc=available_balance_usdc,
                    ),
                )
                rv = runners[rk]
                if rv is not None:
                    fallback_usd = float(rv.trade_notional_usd or max(1.0, float(cfg.notional_usd)))
                    for rule in rv.rules:
                        if rule.notional_fraction is None:
                            rv.rule_notional_usd[rule.key] = fallback_usd
                        else:
                            rv.rule_notional_usd[rule.key] = max(
                                1.0,
                                fallback_usd * (float(rule.notional_fraction) / BALANCE_NOTIONAL_FRACTION),
                            )

    asset_ids: list[str] = []
    for rk, rv in runners.items():
        if rv is not None:
            asset_ids.extend([rv.contract.up.token_id, rv.contract.down.token_id])
    poly.set_assets(asset_ids)

    for rv in runners.values():
        if rv is not None:
            rv.refresh_start_px(cfg)

    for pair_key in sym_for_pair:
        for wm in WINDOWS_TO_TRADE:
            try:
                _tick_runner(
                    runners.get((pair_key, wm)),
                    poly=poly,
                    binance=binance,
                    clob=clob,
                    cfg=cfg,
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
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _event("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
