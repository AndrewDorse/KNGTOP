"""Engine tick path with fakes (no Polymarket / Binance sockets)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kngtop.config import KngtopConfig
from kngtop.engine import WindowRunner, _tick_runner
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.strategy_params import RULES_5M


class _FakePoly:
    def __init__(self, mid_up: float, mid_dn: float) -> None:
        self._up = mid_up
        self._dn = mid_dn

    def mid_for(self, token_id: str, max_age_sec: float = 5.0) -> float | None:
        if token_id == "tid_up":
            return self._up
        if token_id == "tid_dn":
            return self._dn
        return None


class _FakeBinance:
    def __init__(self, px: float) -> None:
        self._px = px

    def last_price(self, max_age_sec: float = 6.0) -> float | None:
        return self._px


def _contract() -> ActiveContract:
    end = datetime.now(timezone.utc) + timedelta(minutes=30)
    return ActiveContract(
        slug="btc-updown-5m-1777900500",
        question="q",
        end_time=end,
        up=TokenMarket("tid_up", "UP", "0.01", False),
        down=TokenMarket("tid_dn", "DOWN", "0.01", False),
    )


def test_tick_fires_under_up_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "a" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()

    c = _contract()
    runner = WindowRunner(c, 5, RULES_5M)
    runner.start_btc = 100_000.0
    runner.traded = False
    poly = _FakePoly(mid_up=0.30, mid_dn=0.70)
    bn = _FakeBinance(100_020.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert runner.traded


def test_tick_no_fire_when_below_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "11" * 32)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "b" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true")
    cfg = KngtopConfig.from_env()
    c = _contract()
    runner = WindowRunner(c, 5, RULES_5M)
    runner.start_btc = 100_000.0
    poly = _FakePoly(mid_up=0.30, mid_dn=0.70)
    bn = _FakeBinance(100_002.0)
    _tick_runner(runner, poly=poly, binance=bn, clob=None, cfg=cfg)
    assert not runner.traded
