"""Unit tests for CLOB REST bid/ask parsing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kngtop.pm_rest import fetch_clob_best_bid_ask


def test_fetch_clob_best_bid_ask_parses_levels() -> None:
    payload = {
        "bids": [{"price": "0.45", "size": "100"}, {"price": "0.44", "size": "50"}],
        "asks": [{"price": "0.48", "size": "100"}, {"price": "0.49", "size": "50"}],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = payload
    mock_resp.raise_for_status = MagicMock()

    with patch("kngtop.pm_rest.requests.get", return_value=mock_resp):
        row = fetch_clob_best_bid_ask(token_id="tid", timeout=2.0)

    assert row == (0.45, 0.48)


def test_fetch_clob_returns_none_when_empty() -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"bids": [], "asks": []}
    mock_resp.raise_for_status = MagicMock()

    with patch("kngtop.pm_rest.requests.get", return_value=mock_resp):
        row = fetch_clob_best_bid_ask(token_id="tid", timeout=2.0)

    assert row is None
