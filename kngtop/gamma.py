"""Gamma discovery for crypto Up/Down slugs ``{asset}-updown-{tf}``. From KNG4 ``prst1/gamma_market.py``."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

LOGGER = logging.getLogger("kngtop")
GAMMA_URL = "https://gamma-api.polymarket.com"


def window_start_ts_from_slug(slug: str) -> int | None:
    m = re.search(r"-(\d+)$", (slug or "").strip())
    return int(m.group(1)) if m else None


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _json_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return v if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []


@dataclass(slots=True)
class TokenMarket:
    token_id: str
    outcome: str
    minimum_tick_size: str | None
    neg_risk: bool | None


@dataclass(slots=True)
class ActiveContract:
    slug: str
    question: str
    end_time: datetime
    up: TokenMarket
    down: TokenMarket


def _timeframe_aliases(window_minutes: int) -> tuple[str, ...]:
    wm = int(window_minutes)
    if wm <= 15:
        return (f"{wm}m",)
    if wm == 60:
        return ("1h", "60m")
    if wm == 240:
        return ("4h", "240m")
    return (f"{wm}m",)


def _active_contract_from_market(m0: dict[str, Any], fallback_slug: str, *, now: datetime) -> ActiveContract | None:
    if not m0.get("active") or m0.get("closed") or m0.get("archived"):
        return None
    end_time = _parse_dt(m0.get("endDate") or m0.get("endDateIso"))
    if end_time is None or end_time <= now:
        return None
    outcomes = _json_list(m0.get("outcomes"))
    token_ids = _json_list(m0.get("clobTokenIds"))
    if len(outcomes) != len(token_ids) or len(token_ids) < 2:
        return None
    tick = str(m0.get("minimum_tick_size") or m0.get("minimumTickSize") or "").strip() or None
    neg = m0.get("neg_risk")
    if neg is None:
        neg = m0.get("negRisk")
    up = down = None
    for name, tid in zip(outcomes, token_ids):
        td = TokenMarket(
            token_id=str(tid),
            outcome=str(name),
            minimum_tick_size=tick,
            neg_risk=bool(neg) if neg is not None else None,
        )
        u = str(name).strip().upper()
        if u == "UP":
            up = td
        elif u == "DOWN":
            down = td
    if up is None or down is None:
        return None
    return ActiveContract(
        slug=str(m0.get("slug") or fallback_slug),
        question=str(m0.get("question") or ""),
        end_time=end_time,
        up=up,
        down=down,
    )


def discover_active_updown_window(
    *,
    market_symbol: str,
    window_minutes: int,
    timeout: float,
) -> ActiveContract | None:
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    window_sec = int(window_minutes) * 60
    start = (now_ts // window_sec) * window_sec
    return discover_updown_window_by_start(
        market_symbol=market_symbol,
        window_minutes=window_minutes,
        start_sec=start,
        timeout=timeout,
    )


def discover_updown_window_by_start(
    *,
    market_symbol: str,
    window_minutes: int,
    start_sec: int,
    timeout: float,
) -> ActiveContract | None:
    now = datetime.now(timezone.utc)
    sym = market_symbol.lower()
    url = f"{GAMMA_URL}/markets"
    for tf in _timeframe_aliases(window_minutes):
        slug = f"{sym}-updown-{tf}-{int(start_sec)}"
        try:
            r = requests.get(url, params={"slug": slug}, timeout=timeout)
            r.raise_for_status()
            markets = r.json()
        except requests.RequestException as exc:
            LOGGER.debug("Gamma request failed for slug %s: %s", slug, exc)
            continue
        if not markets:
            LOGGER.debug("No market for slug %s", slug)
            continue
        contract = _active_contract_from_market(markets[0], slug, now=now)
        if contract is not None:
            return contract
    return None


# Back-compat alias
discover_active_btc_window = discover_active_updown_window
