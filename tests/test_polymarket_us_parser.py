from gnomepy_testing.client.exchange_parsers import PolymarketUsParser, _rfc3339_to_nanos
from gnomepy_testing.listing_resolver import ListingInfo

PRICE = 1_000_000_000
SIZE = 1_000_000
NULL_PRICE = -(2**63)


def _parser() -> PolymarketUsParser:
    return PolymarketUsParser(ListingInfo(
        listing_id=1,
        exchange_id=6,
        exchange_code="POLYMARKET_US",
        exchange_name="Polymarket (US)",
        security_id=3,
        security_symbol="PM_US-X-SHORT",
        exchange_security_id="x:short",
        exchange_security_symbol="x",
    ))


def _run(parser: PolymarketUsParser, *messages: dict) -> list:
    out = []
    for message in messages:
        parser.parse(message, out.append)
    return out


def test_market_data_rebuilds_book():
    out = _run(_parser(), {"marketData": {
        "bids": [{"px": {"value": "0.1510", "currency": "USD"}, "qty": "14.0000"},
                 {"px": {"value": "0.1420", "currency": "USD"}, "qty": "55.5"}],
        "offers": [{"px": {"value": "0.1520", "currency": "USD"}, "qty": "1381"}],
        "transactTime": "2026-10-02T14:22:56.542213551Z",
    }})
    assert len(out) == 1
    msg = out[0]
    assert (msg.bid_price(0), msg.bid_size(0)) == (151_000_000, 14 * SIZE)
    assert (msg.bid_price(1), msg.bid_size(1)) == (142_000_000, 55_500_000)
    assert (msg.ask_price(0), msg.ask_size(0)) == (152_000_000, 1381 * SIZE)
    assert msg.timestamp_event == 1_790_950_976_542_213_551


def test_omitted_side_clears_levels():
    parser = _parser()
    out = _run(
        parser,
        {"marketData": {"bids": [{"px": {"value": "0.5", "currency": "USD"}, "qty": "1"}],
                        "offers": [{"px": {"value": "0.6", "currency": "USD"}, "qty": "1"}]}},
        {"marketData": {"bids": [{"px": {"value": "0.4", "currency": "USD"}, "qty": "2"}]}},
    )
    assert out[1].bid_price(0) == 400_000_000
    assert out[1].ask_price(0) == NULL_PRICE


def test_trade_uses_taker_side():
    out = _run(_parser(), {"trade": {
        "price": {"value": "0.555", "currency": "USD"},
        "quantity": {"value": "0.50", "currency": "USD"},
        "tradeTime": "2024-01-15T10:30:00Z",
        "taker": {"side": "ORDER_SIDE_SELL", "intent": "ORDER_INTENT_SELL_LONG"},
    }})
    assert out[0].price == 555_000_000
    assert out[0].size == 500_000
    assert out[0].timestamp_event == 1_705_314_600 * PRICE


def test_heartbeat_ignored():
    assert _run(_parser(), {"heartbeat": {}}, {"requestId": "md", "error": "bad slug"}) == []


def test_rfc3339_matches_java_parser_cases():
    assert _rfc3339_to_nanos("1970-01-01T00:00:00Z") == 0
    assert _rfc3339_to_nanos("2026-10-02T09:22:56-05:00") == _rfc3339_to_nanos("2026-10-02T14:22:56Z")
    assert _rfc3339_to_nanos("2026-10-02T14:22:56.1234567891234Z") % PRICE == 123_456_789
    assert _rfc3339_to_nanos("not a timestamp") is None
    assert _rfc3339_to_nanos(None) is None


def test_live_trade_message():
    # Captured from aec-dota2-lgd-xtreme-2026-10-02 on 2026-10-02.
    out = _run(_parser(), {"requestId": "tr", "subscriptionType": "SUBSCRIPTION_TYPE_TRADE", "trade": {
        "marketSlug": "aec-dota2-lgd-xtreme-2026-10-02", "price": {"value": "0.6200", "currency": "USD"},
        "quantity": {"value": "53.4800", "currency": "USD"}, "tradeTime": "2026-10-02T15:17:29.394621421Z",
        "maker": {"side": "ORDER_SIDE_SELL", "intent": "ORDER_INTENT_UNDEFINED"},
        "taker": {"side": "ORDER_SIDE_BUY", "intent": "ORDER_INTENT_BUY_LONG", "outcomeSide": "OUTCOME_SIDE_YES"},
        "id": "CVR3KXG46YHR", "state": "TRADE_STATE_NEW"}})
    assert (out[0].price, out[0].size, out[0].side.value) == (620_000_000, 53_480_000, "Bid")


def test_buying_short_hits_bids():
    out = _run(_parser(), {"trade": {
        "price": {"value": "0.6", "currency": "USD"}, "quantity": {"value": "1", "currency": "USD"},
        "tradeTime": "2024-01-15T10:30:00Z", "taker": {"side": "ORDER_SIDE_BUY", "intent": "ORDER_INTENT_BUY_SHORT"}}})
    assert out[0].side.value == "Ask"


def test_undefined_intent_falls_back_to_order_side():
    out = _run(_parser(), {"trade": {
        "price": {"value": "0.6", "currency": "USD"}, "quantity": {"value": "1", "currency": "USD"},
        "tradeTime": "2024-01-15T10:30:00Z", "taker": {"side": "ORDER_SIDE_SELL", "intent": "ORDER_INTENT_UNDEFINED"}}})
    assert out[0].side.value == "Ask"
