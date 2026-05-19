from __future__ import annotations

import pytest

from kngtop.clob_client import _round_order_price, _round_order_shares


def test_round_order_price_to_two_decimals() -> None:
    assert _round_order_price(0.234) == pytest.approx(0.23)
    assert _round_order_price(0.235) == pytest.approx(0.23)


def test_round_order_shares_to_two_decimals() -> None:
    assert _round_order_shares(2.6315789) == pytest.approx(2.63)
    assert _round_order_shares(5.0) == pytest.approx(5.0)


def test_round_order_shares_rejects_zero_after_rounding() -> None:
    with pytest.raises(ValueError, match="shares must round to > 0"):
        _round_order_shares(0.001)
