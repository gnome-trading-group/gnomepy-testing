from gnomepy_testing.client.exchange_parsers import KalshiParser
from gnomepy_testing.listing_resolver import ListingInfo


def _parser() -> KalshiParser:
    return KalshiParser(ListingInfo(
        listing_id=1, exchange_id=5, exchange_code="KALSHI", exchange_name="Kalshi", security_id=3,
        security_symbol="KX-T-YES", exchange_security_id="T:yes", exchange_security_symbol="T",
    ))


def test_yes_priced_no_levels_are_ascending_asks():
    parser, out = _parser(), []
    parser.parse({"type": "orderbook_snapshot", "msg": {
        "yes_dollars_fp": [["0.3100", "10.00"], ["0.3000", "20.00"]],
        "no_dollars_fp": [["0.3500", "7.00"], ["0.3300", "5.00"]],
    }}, out.append)
    parser.parse({"type": "orderbook_delta", "msg": {
        "price_dollars": "0.3300", "delta_fp": "1.00", "side": "no", "ts_ms": 1}}, out.append)

    book = out[-1]
    assert (book.bid_price(0), book.bid_size(0)) == (310_000_000, 10_000_000)
    assert (book.ask_price(0), book.ask_size(0)) == (330_000_000, 6_000_000)
    assert book.ask_price(1) == 350_000_000
