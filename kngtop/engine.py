"""Multi-asset BTC/ETH/XRP Up/Down — 5m+15m parallel, WS-triggered eval + heartbeat."""

from __future__ import annotations

import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kngtop.binance_multi_ws import BinanceCombinedTradeFeed
from kngtop.binance_rest import fetch_binance_window_open_px
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.eval_coordinator import EvalCoordinator
from kngtop.gamma import ActiveContract, discover_active_btc_window, window_start_ts_from_slug
from kngtop.strategy_params import MispriceRule, rule_fires, rules_for_asset
from kngtop.rest_poll import run_ws_rest_fallback_loop
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")
BALANCE_NOTIONAL_FRACTION = 0.10
ALT_BALANCE_NOTIONAL_FRACTION = 0.05
WINDOWS_TO_TRADE: tuple[int, ...] = (5, 15, 60, 240)
ALT_BALANCE_ASSETS = frozenset({"DOGE", "BNB", "HYPE", "LINK"})
MIN_WINDOW_PROGRESS_FRACTION = 0.20
ENTRY_MARKET_FRACTION = 0.50
ENTRY_LIMIT_FRACTION = 0.50
ENTRY_LIMIT_PRICE = 0.20


@dataclass
class WindowRunner:
    pair_key: str
    binance_symbol: str
    contract: ActiveContract
    window_minutes: int
    rules: tuple[MispriceRule, ...]
    start_px: float | None = None
    trade_notional_usd: float | None = None
    traded: bool = False
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
    clob: KngtopClob | None,
    *,
    pair_key: str,
    window_minutes: int,
) -> float:
    floor_usd = max(1.0, float(cfg.notional_usd))
    if int(window_minutes) >= 60:
        return floor_usd
    if clob is None:
        return floor_usd
    avail = clob.available_balance_usdc()
    if avail is None:
        return floor_usd
    frac = ALT_BALANCE_NOTIONAL_FRACTION if pair_key.upper() in ALT_BALANCE_ASSETS else BALANCE_NOTIONAL_FRACTION
    return max(floor_usd, avail * frac)


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
) -> None:
    usdc_f = float(usdc)
    market_usdc = usdc_f * ENTRY_MARKET_FRACTION
    limit_usdc = usdc_f * ENTRY_LIMIT_FRACTION
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
    )
    if cfg.dry_run:
        return
    assert clob is not None
    attempts = 1 + int(cfg.order_retry_on_error)
    for attempt in range(1, attempts + 1):
        try:
            _ = clob.market_buy_usdc(token, market_usdc)
            _ = clob.limit_buy(token, price=ENTRY_LIMIT_PRICE, usdc=limit_usdc)
            return
        except Exception as exc:  # noqa: BLE001
            _event("DEAL_FAIL", label=label, attempt=attempt, error=str(exc))
            if attempt >= attempts:
                raise
            time.sleep(0.35)


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
        if runner.traded or runner.start_px is None or runner.trade_notional_usd is None:
            return
        now = datetime.now(timezone.utc)
        if not _window_elapsed_ready(runner, now):
            return
        remaining = (runner.contract.end_time - now).total_seconds()
        if remaining < cfg.order_cutoff_remaining_sec:
            return
        spot = binance.last_price(runner.binance_symbol, max_age_sec=cfg.binance_max_age_sec)
        if spot is None:
            return
        up_id = runner.contract.up.token_id
        dn_id = runner.contract.down.token_id
        mid_up = poly.mid_for(up_id, max_age_sec=cfg.poly_mid_max_age_sec)
        mid_dn = poly.mid_for(dn_id, max_age_sec=cfg.poly_mid_max_age_sec)
        if mid_up is None or mid_dn is None:
            return
        start = float(runner.start_px)
        for rule in runner.rules:
            if not rule_fires(rule, btc=spot, start_btc=start, mid_up=mid_up, mid_dn=mid_dn):
                continue
            tok = _pick_token(runner.contract, rule.side)
            label = f"{runner.pair_key}/{runner.window_minutes}m/{rule.key}/{rule.side}"
            trigger_px = mid_up if rule.kind == "cheap_up" else mid_dn
            _execute_buy(
                clob,
                cfg,
                runner.trade_notional_usd,
                tok,
                label,
                start_px=start,
                spot_px=spot,
                pm_trigger_px=trigger_px,
            )
            runner.traded = True
            break


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

    for pair_key in sym_for_pair:
        gamma_sym = pair_key.lower()
        bs_sym = sym_for_pair[pair_key]
        for wm in WINDOWS_TO_TRADE:
            c = discover_active_btc_window(market_symbol=gamma_sym, window_minutes=wm, timeout=timeout)
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
                        clob,
                        pair_key=pair_key,
                        window_minutes=wm,
                    ),
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
        clob = KngtopClob(
            private_key=cfg.private_key,
            funder=cfg.funder,
            signature_type=cfg.signature_type,
            relayer_api_key=cfg.relayer_api_key,
            relayer_secret=cfg.relayer_secret,
            relayer_passphrase=cfg.relayer_passphrase,
            market_buy_max_price=cfg.market_buy_max_price,
        )

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

    while True:
        try:
            coord.wait_for_turn()
            _run_iteration(cfg, runners=runners, poly=poly, binance=binance, clob=clob)
        except Exception as exc:  # noqa: BLE001
            _event("ERROR", stage="main_loop", error=str(exc))


if __name__ == "__main__":
    main()
