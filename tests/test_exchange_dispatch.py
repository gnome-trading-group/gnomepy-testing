import pytest

from gnomepy_testing.capture_proxy.exchange_connector import (
    KalshiConnector,
    PolymarketIntlConnector,
    PolymarketUsConnector,
    create_exchange_connector,
)
from gnomepy_testing.client.exchange_parsers import KalshiParser, PolymarketIntlParser, PolymarketUsParser, create_parser
from gnomepy_testing.listing_resolver import ListingInfo


def _listing(code: str, name: str) -> ListingInfo:
    return ListingInfo(
        listing_id=1,
        exchange_id=1,
        exchange_code=code,
        exchange_name=name,
        security_id=1,
        security_symbol="X",
        exchange_security_id="x",
        exchange_security_symbol="x",
    )


def test_parser_selected_by_exchange_code_not_display_name():
    assert isinstance(create_parser(_listing("POLYMARKET_INTL", "Polymarket (International)")), PolymarketIntlParser)
    assert isinstance(create_parser(_listing("POLYMARKET_US", "Polymarket (US)")), PolymarketUsParser)
    assert isinstance(create_parser(_listing("KALSHI", "Kalshi")), KalshiParser)
    with pytest.raises(ValueError):
        create_parser(_listing("SOMETHING_ELSE", "Polymarket"))


def test_connector_selected_by_exchange_code_not_display_name():
    on_message = lambda _: None
    assert isinstance(create_exchange_connector(_listing("POLYMARKET_INTL", "Renamed"), on_message), PolymarketIntlConnector)
    assert isinstance(create_exchange_connector(_listing("POLYMARKET_US", "Polymarket (US)"), on_message), PolymarketUsConnector)
    assert isinstance(create_exchange_connector(_listing("KALSHI", "Kalshi"), on_message), KalshiConnector)
    with pytest.raises(ValueError):
        create_exchange_connector(_listing("UNKNOWN_VENUE", "Unknown"), on_message)
