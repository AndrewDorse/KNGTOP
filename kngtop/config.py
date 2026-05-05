"""Environment configuration (KNG4-style naming)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(key: str, default: bool) -> bool:
    raw = (os.environ.get(key) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True, slots=True)
class KngtopConfig:
    private_key: str
    funder: str
    signature_type: int
    relayer_api_key: str
    relayer_secret: str
    relayer_passphrase: str
    dry_run: bool
    poll_interval_sec: float
    request_timeout_sec: float
    notional_usd: float
    btc_symbol: str
    market_symbol: str
    log_level: str
    order_cutoff_remaining_sec: float
    order_retry_on_error: int

    @staticmethod
    def from_env() -> "KngtopConfig":
        pk = (os.environ.get("POLY_PRIVATE_KEY") or "").strip()
        funder = (os.environ.get("POLY_FUNDER") or "").strip()
        if not pk or not funder:
            raise RuntimeError("POLY_PRIVATE_KEY and POLY_FUNDER are required")
        return KngtopConfig(
            private_key=pk,
            funder=funder,
            signature_type=int(os.environ.get("POLY_SIGNATURE_TYPE") or "1"),
            relayer_api_key=(os.environ.get("RELAYER_API_KEY") or "").strip(),
            relayer_secret=(os.environ.get("RELAYER_SECRET") or "").strip(),
            relayer_passphrase=(os.environ.get("RELAYER_PASSPHRASE") or "").strip(),
            dry_run=_env_bool("POLY_DRY_RUN", True),
            poll_interval_sec=float(os.environ.get("KNGTOP_POLL_INTERVAL_SECONDS") or "0.35"),
            request_timeout_sec=float(os.environ.get("KNGTOP_REQUEST_TIMEOUT_SECONDS") or "12.0"),
            notional_usd=float(os.environ.get("KNGTOP_NOTIONAL_USD") or "1.0"),
            btc_symbol=(os.environ.get("KNGTOP_BTC_FEED_SYMBOL") or "BTCUSDT").strip().upper(),
            market_symbol=(os.environ.get("KNGTOP_MARKET_SYMBOL") or "BTC").strip().upper(),
            log_level=(os.environ.get("KNGTOP_LOG_LEVEL") or "INFO").strip().upper(),
            order_cutoff_remaining_sec=float(os.environ.get("KNGTOP_ORDER_CUTOFF_REMAINING_SEC") or "20.0"),
            order_retry_on_error=max(0, int(os.environ.get("KNGTOP_ORDER_RETRY_ON_ERROR") or "2")),
        )
