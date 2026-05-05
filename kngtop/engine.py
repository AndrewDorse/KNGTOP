"""Main loop: 5m + 15m windows, eight preset rules, one $1 v2 market buy per slug per window."""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kngtop.binance_rest import fetch_binance_window_open_btc
from kngtop.binance_ws import BinanceBtcWsFeed
from kngtop.clob_client import KngtopClob
from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, discover_active_btc_window, window_start_ts_from_slug
from kngtop.strategy_params import MispriceRule, RULES_15M, RULES_5M, rule_fires
from kngtop.ws_market import MarketWsFeed

LOGGER = logging.getLogger("kngtop")


@dataclass
class WindowRunner:
    contract: ActiveContract
    window_minutes: int
    rules: tuple[MispriceRule, ...]
    start_btc: float | None = None
    traded: bool = False
    logged_ready: bool = field(default=False)

    def refresh_start_btc(self, cfg: KngtopConfig) -> None:
        if self.start_btc is not None:
            return
        w0 = window_start_ts_from_slug(self.contract.slug)
        if w0 is None:
            return
        self.start_btc = fetch_binance_window_open_btc(
            symbol=cfg.btc_symbol,
            window_start_sec=w0,
            window_minutes=self.window_minutes,
            timeout=cfg.request_timeout_sec,
        )
        if self.start_btc and not self.logged_ready:
            _event(
                "INIT",
                scope="WINDOW_READY",
                window=f"{self.window_minutes}m",
                slug=self.contract.slug,
                start_btc=f"{self.start_btc:.2f}",
            )
            self.logged_ready = True


def _setup_logging(level: str) -> None:
    # Keep library chatter out of stdout; operational events are printed via _event.
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


def _pick_token(c: ActiveContract, side: str):
    return c.up if side.upper() == "UP" else c.down


def _execute_buy(
    clob: KngtopClob | None,
    cfg: KngtopConfig,
    token,
    label: str,
) -> None:
    _event("DEAL_START", label=label, notional=f"{cfg.notional_usd:.2f}", token=token.token_id[:16])
    if cfg.dry_run:
        _event("DEAL_SUCCESS", label=label, mode="DRY_RUN")
        return
    assert clob is not None
    attempts = 1 + int(cfg.order_retry_on_error)
    for attempt in range(1, attempts + 1):
        try:
            resp = clob.market_buy_usdc(token, cfg.notional_usd)
            _event(
                "DEAL_SUCCESS",
                label=label,
                mode="LIVE",
                attempt=attempt,
                resp=json.dumps(resp)[:220],
            )
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
    binance: BinanceBtcWsFeed,
    clob: KngtopClob | None,
    cfg: KngtopConfig,
) -> None:
    if runner is None or runner.traded or runner.start_btc is None:
        return
    now = datetime.now(timezone.utc)
    remaining = (runner.contract.end_time - now).total_seconds()
    if remaining < cfg.order_cutoff_remaining_sec:
        return
    btc = binance.last_price(max_age_sec=6.0)
    if btc is None:
        return
    up_id = runner.contract.up.token_id
    dn_id = runner.contract.down.token_id
    mid_up = poly.mid_for(up_id, max_age_sec=5.0)
    mid_dn = poly.mid_for(dn_id, max_age_sec=5.0)
    if mid_up is None or mid_dn is None:
        return
    start = float(runner.start_btc)
    for rule in runner.rules:
        if not rule_fires(rule, btc=btc, start_btc=start, mid_up=mid_up, mid_dn=mid_dn):
            continue
        tok = _pick_token(runner.contract, rule.side)
        label = f"{runner.window_minutes}m/{rule.key}/{rule.side}"
        _execute_buy(clob, cfg, tok, label)
        runner.traded = True
        break


def main() -> None:
    cfg = KngtopConfig.from_env()
    _setup_logging(cfg.log_level)
    _event(
        "INIT",
        scope="BOOT",
        dry_run=str(cfg.dry_run).lower(),
        notional=f"{cfg.notional_usd:.2f}",
        retry_on_error=cfg.order_retry_on_error,
    )
    poly = MarketWsFeed()
    binance = BinanceBtcWsFeed(cfg.btc_symbol.lower())
    poly.start()
    binance.start()

    clob: KngtopClob | None = None
    if not cfg.dry_run:
        clob = KngtopClob(
            private_key=cfg.private_key,
            funder=cfg.funder,
            signature_type=cfg.signature_type,
            relayer_api_key=cfg.relayer_api_key,
            relayer_secret=cfg.relayer_secret,
            relayer_passphrase=cfg.relayer_passphrase,
        )
        _event("INIT", scope="LIVE_TRADING", enabled="true")
    else:
        _event("INIT", scope="LIVE_TRADING", enabled="false")

    r5: WindowRunner | None = None
    r15: WindowRunner | None = None

    while True:
        c5 = discover_active_btc_window(
            market_symbol=cfg.market_symbol, window_minutes=5, timeout=cfg.request_timeout_sec
        )
        c15 = discover_active_btc_window(
            market_symbol=cfg.market_symbol, window_minutes=15, timeout=cfg.request_timeout_sec
        )

        assets: list[str] = []
        if c5:
            assets.extend([c5.up.token_id, c5.down.token_id])
        if c15:
            assets.extend([c15.up.token_id, c15.down.token_id])
        if assets:
            poly.set_assets(assets)

        if c5 and (r5 is None or r5.contract.slug != c5.slug):
            r5 = WindowRunner(c5, 5, RULES_5M)
            _event("INIT", scope="WINDOW_NEW", window="5m", slug=c5.slug)
        if c15 and (r15 is None or r15.contract.slug != c15.slug):
            r15 = WindowRunner(c15, 15, RULES_15M)
            _event("INIT", scope="WINDOW_NEW", window="15m", slug=c15.slug)

        if r5:
            r5.refresh_start_btc(cfg)
        if r15:
            r15.refresh_start_btc(cfg)

        try:
            _tick_runner(r5, poly=poly, binance=binance, clob=clob, cfg=cfg)
            _tick_runner(r15, poly=poly, binance=binance, clob=clob, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            _event("ERROR", stage="tick", error=str(exc))

        time.sleep(cfg.poll_interval_sec)
