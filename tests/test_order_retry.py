"""Order retry behavior for $1 market buys."""

from __future__ import annotations

import pytest

from kngtop.config import KngtopConfig
from kngtop.engine import _execute_buy


class _FakeToken:
    token_id = "tok_1234567890abcdef"


class _FakeClobRetry:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    def market_buy_usdc(self, token: _FakeToken, usdc: float):  # noqa: ANN201
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("simulated order error")
        return {"ok": True, "calls": self.calls, "usdc": usdc, "token": token.token_id}


def _cfg(monkeypatch: pytest.MonkeyPatch, *, dry_run: bool = False, retries: int = 2) -> KngtopConfig:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.setenv("POLY_DRY_RUN", "true" if dry_run else "false")
    monkeypatch.setenv("KNGTOP_ORDER_RETRY_ON_ERROR", str(retries))
    return KngtopConfig.from_env()


def test_execute_buy_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False, retries=2)
    clob = _FakeClobRetry(fail_times=2)
    _execute_buy(clob, cfg, _FakeToken(), "5m/u_up_cheap/UP")
    assert clob.calls == 3


def test_execute_buy_raises_after_exhausting_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch, dry_run=False, retries=2)
    clob = _FakeClobRetry(fail_times=10)
    with pytest.raises(RuntimeError, match="simulated order error"):
        _execute_buy(clob, cfg, _FakeToken(), "15m/u_dn_cheap/DOWN")
    assert clob.calls == 3
