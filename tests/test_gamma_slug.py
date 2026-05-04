"""Slug parsing."""

from kngtop.gamma import window_start_ts_from_slug


def test_window_start_from_slug() -> None:
    assert window_start_ts_from_slug("btc-updown-5m-1777900500") == 1777900500
    assert window_start_ts_from_slug("btc-updown-15m-1777900500") == 1777900500
    assert window_start_ts_from_slug("bad") is None
