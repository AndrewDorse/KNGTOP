from kngtop.btc_confirm import BtcConfirmFeed


def test_btc_confirm_fails_open_without_external_quorum() -> None:
    feed = BtcConfirmFeed()
    assert feed.side_matches(start_px=100_000.0, binance_spot=100_001.0) is True
