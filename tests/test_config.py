"""Config validation."""

import pytest

from kngtop.config import KngtopConfig


def test_from_env_requires_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POLY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("POLY_FUNDER", raising=False)
    with pytest.raises(RuntimeError, match="POLY_PRIVATE_KEY"):
        KngtopConfig.from_env()


def test_dry_run_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("POLY_FUNDER", "0x" + "2" * 40)
    monkeypatch.delenv("POLY_DRY_RUN", raising=False)
    cfg = KngtopConfig.from_env()
    assert cfg.dry_run is True
