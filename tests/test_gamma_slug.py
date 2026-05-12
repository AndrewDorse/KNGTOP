"""Slug parsing and discovery helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from kngtop.gamma import TokenMarket, _timeframe_aliases, discover_active_updown_window, window_start_ts_from_slug


def test_window_start_from_slug() -> None:
    assert window_start_ts_from_slug("btc-updown-5m-1777900500") == 1777900500
    assert window_start_ts_from_slug("btc-updown-15m-1777900500") == 1777900500
    assert window_start_ts_from_slug("btc-updown-1h-1777900500") == 1777900500
    assert window_start_ts_from_slug("btc-updown-4h-1777900500") == 1777900500
    assert window_start_ts_from_slug("bad") is None


def test_timeframe_aliases_cover_hourly_and_4h() -> None:
    assert _timeframe_aliases(5) == ("5m",)
    assert _timeframe_aliases(15) == ("15m",)
    assert _timeframe_aliases(60) == ("1h", "60m")
    assert _timeframe_aliases(240) == ("4h", "240m")


class _FakeResp:
    def __init__(self, payload: list[dict[str, object]]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict[str, object]]:
        return self._payload


def test_discover_active_window_falls_back_to_60m_alias(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    start = (int(now.timestamp()) // 3600) * 3600
    expected_slug = f"btc-updown-60m-{start}"

    def _fake_get(_url: str, *, params: dict[str, object], timeout: float):
        slug = params["slug"]
        if slug == f"btc-updown-1h-{start}":
            return _FakeResp([])
        if slug == expected_slug:
            return _FakeResp(
                [
                    {
                        "slug": expected_slug,
                        "question": "Bitcoin Up or Down - Hourly",
                        "active": True,
                        "closed": False,
                        "archived": False,
                        "endDate": (now + timedelta(minutes=30)).isoformat(),
                        "outcomes": ["UP", "DOWN"],
                        "clobTokenIds": ["up_id", "down_id"],
                        "minimum_tick_size": "0.01",
                        "neg_risk": False,
                    }
                ]
            )
        raise AssertionError(f"unexpected slug lookup: {slug}")

    monkeypatch.setattr("kngtop.gamma.requests.get", _fake_get)

    contract = discover_active_updown_window(market_symbol="btc", window_minutes=60, timeout=5.0)

    assert contract is not None
    assert contract.slug == expected_slug
    assert contract.question == "Bitcoin Up or Down - Hourly"
    assert contract.up == TokenMarket("up_id", "UP", "0.01", False)
    assert contract.down == TokenMarket("down_id", "DOWN", "0.01", False)
