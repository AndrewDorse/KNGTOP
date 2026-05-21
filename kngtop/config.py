"""Environment configuration (KNG4-style naming)."""

from __future__ import annotations

import os
from dataclasses import dataclass

_ALLOWED_PAIR_KEYS = frozenset({"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"})


def _env_bool(key: str, default: bool) -> bool:
    raw = (os.environ.get(key) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on")


def parse_trading_pairs(raw: str | None) -> tuple[tuple[str, str], ...]:
    """``GAMMA_KEY:BINANCE_SYMBOL`` comma list, e.g. ``BTC:BTCUSDT``."""
    s = (raw or "").strip()
    if not s:
        s = "BTC:BTCUSDT"
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise RuntimeError(
                f"Invalid KNGTOP_PAIRS segment {part!r}; expected ASSET:SYMBOL e.g. BTC:BTCUSDT"
            )
        a, b = part.split(":", 1)
        key = a.strip().upper()
        sym = b.strip().upper().replace("/", "")
        if not key or not sym:
            raise RuntimeError(f"Invalid KNGTOP_PAIRS segment {part!r}")
        if key in seen:
            raise RuntimeError(f"Duplicate asset {key} in KNGTOP_PAIRS")
        if key not in _ALLOWED_PAIR_KEYS:
            raise RuntimeError(
                f"Unsupported asset {key!r} in KNGTOP_PAIRS (allowed: {', '.join(sorted(_ALLOWED_PAIR_KEYS))})"
            )
        seen.add(key)
        out.append((key, sym))
    if not out:
        raise RuntimeError("KNGTOP_PAIRS resolved to empty")
    return tuple(out)


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
    eval_debounce_sec: float
    request_timeout_sec: float
    notional_usd: float
    trading_pairs: tuple[tuple[str, str], ...]
    log_level: str
    order_cutoff_remaining_sec: float
    order_retry_on_error: int
    market_buy_max_price: float
    binance_max_age_sec: float
    poly_mid_max_age_sec: float
    ws_rest_poll_enabled: bool
    ws_rest_poll_interval_sec: float
    hedge_max_orders_per_side: int

    @staticmethod
    def from_env() -> "KngtopConfig":
        pk = (os.environ.get("POLY_PRIVATE_KEY") or "").strip()
        funder = (os.environ.get("POLY_FUNDER") or "").strip()
        if not pk or not funder:
            raise RuntimeError("POLY_PRIVATE_KEY and POLY_FUNDER are required")
        pairs = parse_trading_pairs(os.environ.get("KNGTOP_PAIRS"))
        return KngtopConfig(
            private_key=pk,
            funder=funder,
            signature_type=int(os.environ.get("POLY_SIGNATURE_TYPE") or "1"),
            relayer_api_key=(os.environ.get("RELAYER_API_KEY") or "").strip(),
            relayer_secret=(os.environ.get("RELAYER_SECRET") or "").strip(),
            relayer_passphrase=(os.environ.get("RELAYER_PASSPHRASE") or "").strip(),
            dry_run=_env_bool("POLY_DRY_RUN", False),
            poll_interval_sec=float(os.environ.get("KNGTOP_POLL_INTERVAL_SECONDS") or "0.2"),
            eval_debounce_sec=0.0,
            request_timeout_sec=float(os.environ.get("KNGTOP_REQUEST_TIMEOUT_SECONDS") or "5.0"),
            notional_usd=float(os.environ.get("KNGTOP_NOTIONAL_USD") or "1.0"),
            trading_pairs=pairs,
            log_level=(os.environ.get("KNGTOP_LOG_LEVEL") or "INFO").strip().upper(),
            order_cutoff_remaining_sec=float(os.environ.get("KNGTOP_ORDER_CUTOFF_REMAINING_SEC") or "20.0"),
            order_retry_on_error=max(0, int(os.environ.get("KNGTOP_ORDER_RETRY_ON_ERROR") or "2")),
            market_buy_max_price=float(os.environ.get("KNGTOP_MARKET_BUY_MAX_PRICE") or "0.85"),
            binance_max_age_sec=float(os.environ.get("KNGTOP_BINANCE_MAX_AGE_SEC") or "6.0"),
            poly_mid_max_age_sec=float(os.environ.get("KNGTOP_POLY_MID_MAX_AGE_SEC") or "5.0"),
            ws_rest_poll_enabled=_env_bool("KNGTOP_WS_REST_POLL_ENABLE", True),
            ws_rest_poll_interval_sec=max(
                0.2,
                min(
                    float(os.environ.get("KNGTOP_WS_REST_POLL_INTERVAL_SECONDS") or "1.0"),
                    120.0,
                ),
            ),
            hedge_max_orders_per_side=max(1, int(os.environ.get("KNGTOP_HEDGE_MAX_ORDERS_PER_SIDE") or "5")),
        )
