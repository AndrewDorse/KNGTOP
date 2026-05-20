"""Order execution behavior for single-leg limit buys."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from kngtop.config import KngtopConfig
from kngtop.engine import _execute_buy


class _FakeToken:
    token_id = "tok_1234567890abcdef"


class _FakeClobRetry:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.limit_calls = 0
        self.price: float | None = None
        self.shares: float | None = None

    def limit_buy_shares(self, token: _FakeToken, *, price: float, shares: float, post_only: bool = True):  # noqa: ANN201
        assert post_only is True
        self.limit_calls += 1
        self.price = price
        self.shares = shares
        if self.limit_calls <= self.fail_times:
            raise RuntimeError("simulated order error")
        return {"ok": True, "orderID": "buy123", "calls": self.limit_calls, "shares": shares, "token": token.token_id, "price": price}

def _cfg(monkeypatch: pytest.MonkeyPatch, *, dry_run: bool = False, retries: int = 2) -> KngtopConfig:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true" if dry_run else "false")
    monkeypatch.setenv("KNGTOP_ORDER_RETRY_ON_ERROR", str(retries))
    return KngtopConfig.from_env()


def test_execute_buy_returns_true_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    clob = _FakeClobRetry(fail_times=0)
    ok = _execute_buy(
        clob,
        cfg,
        5.0,
        1.25,
        _FakeToken(),
        "5m/close_buy_up/UP",
        start_px=100_000.0,
        spot_px=100_001.0,
        pm_trigger_px=0.30,
        limit_price=0.33,
    )
    assert ok == (True, None, "buy123")
    assert clob.limit_calls == 1
    assert clob.shares == 5.0
    assert clob.price == 0.33


def test_execute_buy_returns_false_on_error_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False, retries=2)
    clob = _FakeClobRetry(fail_times=10)
    ok = _execute_buy(
        clob,
        cfg,
        5.0,
        1.25,
        _FakeToken(),
        "5m/close_buy_up/UP",
        start_px=100_000.0,
        spot_px=100_001.0,
        pm_trigger_px=0.30,
        limit_price=0.33,
    )
    assert ok == (False, "error", None)
    assert clob.limit_calls == 1


def test_execute_buy_requires_explicit_limit_price(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    clob = _FakeClobRetry(fail_times=0)
    ok = _execute_buy(
        clob,
        cfg,
        5.0,
        1.25,
        _FakeToken(),
        "5m/close_buy_up/UP",
        start_px=100_000.0,
        spot_px=100_001.0,
        pm_trigger_px=0.30,
        limit_price=0.33,
    )
    assert ok == (True, None, "buy123")
    assert clob.limit_calls == 1
    assert clob.price == 0.33


def test_execute_buy_logs_filled_elapsed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    clob = _FakeClobRetry(fail_times=0)
    with patch("kngtop.engine._event") as event_mock:
        _execute_buy(
            clob,
            cfg,
            5.0,
            1.25,
            _FakeToken(),
            "5m/close_buy_up/UP",
            start_px=100_000.0,
            spot_px=100_001.0,
            pm_trigger_px=0.30,
            limit_price=0.33,
        )
    kinds = [call.args[0] for call in event_mock.call_args_list]
    assert "START_DEAL" in kinds


def test_execute_buy_logs_not_filled_elapsed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False)
    clob = _FakeClobRetry(fail_times=1)
    with patch("kngtop.engine._event") as event_mock:
        _execute_buy(
            clob,
            cfg,
            5.0,
            1.25,
            _FakeToken(),
            "5m/close_buy_up/UP",
            start_px=100_000.0,
            spot_px=100_001.0,
            pm_trigger_px=0.30,
            limit_price=0.33,
        )
    kinds = [call.args[0] for call in event_mock.call_args_list]
    assert "START_DEAL" in kinds
    assert "RETRY_BUY" in kinds
