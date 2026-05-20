"""Live strategy presets for KNGTOP."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


BTC_HEDGE_START_LIMIT_5M = 0.12
BTC_HEDGE_TARGET_SUM_5M = 0.68

ENTRY_PRICE_MIN_5M = 0.01
ENTRY_PRICE_MAX_5M = 0.12
ENTRY_PRICE_MIN_15M = 0.01
ENTRY_PRICE_MAX_15M = 0.25
MARKET_BUY_MAX_PRICE = 0.85
MIN_ELAPSED_SEC_5M = 0
MIN_ELAPSED_SEC_15M = 180
MAX_ELAPSED_SEC_5M = 300
MAX_ELAPSED_SEC_15M = 900
RECLAIM_LOOKBACK_SEC_5M = 40
RECLAIM_LOOKBACK_SEC_15M = 40
RECLAIM_GAP_MIN = 0.05


RuleKind = Literal["serial_hedge"]


@dataclass(frozen=True, slots=True)
class MispriceRule:
    """One active live rule."""

    key: str
    price_min: float
    cheap_max: float
    side: Literal["UP", "DOWN", "BOTH"]
    kind: RuleKind
    min_elapsed_sec: int = MIN_ELAPSED_SEC_5M
    max_elapsed_sec: int = MAX_ELAPSED_SEC_5M
    lookback_sec: int = RECLAIM_LOOKBACK_SEC_5M
    gap_min: float = RECLAIM_GAP_MIN
    distance_bps_max: float | None = None
    momentum_lookback_sec: int | None = None
    momentum_bps_min: float | None = None
    market_buy_max_price: float | None = MARKET_BUY_MAX_PRICE
    retry_on_error_override: int | None = 0


BTC_RULES_5M: tuple[MispriceRule, ...] = (
    MispriceRule(
        "serial_hedge_12c_sum68",
        price_min=0.01,
        cheap_max=BTC_HEDGE_START_LIMIT_5M,
        side="BOTH",
        kind="serial_hedge",
        min_elapsed_sec=0,
        max_elapsed_sec=MAX_ELAPSED_SEC_5M,
    ),
)

BTC_RULES_15M: tuple[MispriceRule, ...] = ()
ETH_RULES_5M: tuple[MispriceRule, ...] = ()
ETH_RULES_15M: tuple[MispriceRule, ...] = ()

RULES_5M: tuple[MispriceRule, ...] = BTC_RULES_5M
RULES_15M: tuple[MispriceRule, ...] = BTC_RULES_15M


def rules_for_asset(pair: str, window_minutes: int) -> tuple[MispriceRule, ...]:
    """Return the active rules for a Gamma asset key and timeframe."""
    p = (pair or "").strip().upper()
    if p not in {"BTC", "ETH", "XRP", "SOL", "DOGE", "BNB", "HYPE", "LINK"}:
        raise ValueError(f"unsupported asset pair {pair!r} (expected BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, or LINK)")
    if p == "BTC":
        return BTC_RULES_5M if int(window_minutes) <= 5 else BTC_RULES_15M
    return ()


def rule_fires(rule: MispriceRule, *, btc: float, start_btc: float, mid_up: float, mid_dn: float) -> bool:
    del btc, start_btc
    if rule.kind != "serial_hedge":
        return False
    cheaper = min(float(mid_up), float(mid_dn))
    return rule.price_min <= cheaper <= rule.cheap_max
