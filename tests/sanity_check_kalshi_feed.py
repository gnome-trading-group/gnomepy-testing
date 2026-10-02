"""
Sanity-check the Kalshi parser against a live feed.

Streams orderbook_delta + trade for one market, runs every message through KalshiParser (the
Python mirror of the Java KalshiInboundReader) and checks what comes out:
  - the book is never crossed and prices stay inside (0, 1)
  - each trade prints at the touch its aggressor side implies (a Bid aggressor lifts the ask,
    an Ask aggressor hits the bid), judged against the book just before the trade
  - which trade fields Kalshi actually sends (the parser relies on taker_book_side)
  - that NO-side levels arrive YES-leg priced (the subscription sets use_yes_price, and the parser
    reads NO levels as asks at the same price)

Usage:
    poetry run python -m tests.sanity_check_kalshi_feed KXATPCHALLENGERMATCH-26OCT02KYMBUT-KYM --seconds 300
"""
import argparse
import asyncio
import json
import time
from collections import Counter
from decimal import Decimal

import websockets

from gnomepy_testing.client.exchange_parsers import KalshiParser
from gnomepy_testing.listing_resolver import ListingInfo
from tests.validate_kalshi_book import KALSHI_WS_PATH, KALSHI_WS_URL, load_credentials, make_auth_headers

PRICE_SCALE = 1_000_000_000
NULL_PRICE = -(2**63)


def top(schema) -> tuple[int | None, int | None]:
    bid, ask = schema.bid_price(0), schema.ask_price(0)
    return (None if bid == NULL_PRICE else bid), (None if ask == NULL_PRICE else ask)


def name(value) -> str:
    return getattr(value, "value", value)


def fmt(price: int | None) -> str:
    return "-" if price is None else f"{Decimal(price) / PRICE_SCALE:.4f}"


async def run(ticker: str, seconds: float, dump_path: str | None) -> None:
    api_key, private_key = load_credentials()
    parser = KalshiParser(ListingInfo(
        listing_id=0, exchange_id=0, exchange_code="KALSHI", exchange_name="Kalshi",
        security_id=0, security_symbol=ticker, exchange_security_id=f"{ticker}:yes",
        exchange_security_symbol=ticker,
    ))
    outputs: list = []
    dump = open(dump_path, "w") if dump_path else None

    counts: Counter[str] = Counter()
    trade_fields: Counter[str] = Counter()
    crossed = out_of_range = 0
    trade_checks: Counter[str] = Counter()
    last_book = (None, None)
    no_leg_votes: Counter[str] = Counter()

    headers = make_auth_headers(api_key, private_key, KALSHI_WS_PATH)
    async with websockets.connect(KALSHI_WS_URL, additional_headers=list(headers.items())) as ws:
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {
            "channels": ["orderbook_delta", "trade"], "market_tickers": [ticker], "use_yes_price": True}}))

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                break
            if dump is not None:
                dump.write(f"{time.time():.3f} {raw}\n")
            data = json.loads(raw)
            msg_type = data.get("type", "other")
            counts[msg_type] += 1
            msg = data.get("msg", {})

            if msg_type == "orderbook_snapshot":
                yes = [Decimal(p) for p, _ in msg.get("yes_dollars_fp", [])]
                no = [Decimal(p) for p, _ in msg.get("no_dollars_fp", [])]
                if yes and no:
                    # YES-leg pricing: best YES bid sits below the lowest NO (ask) level.
                    no_leg_votes["yes-leg" if max(yes) < min(no) else "no-leg"] += 1
            if msg_type == "trade":
                trade_fields.update(msg.keys())
                if counts["trade"] <= 3:
                    print(f"trade sample: {json.dumps(msg)}")

            before = len(outputs)
            parser.parse(data, outputs.append)
            for schema in outputs[before:]:
                bid, ask = top(schema)
                if bid is not None and ask is not None and bid >= ask:
                    crossed += 1
                    print(f"CROSSED after {msg_type}: bid {fmt(bid)} >= ask {fmt(ask)}")
                if any(p is not None and not 0 < p < PRICE_SCALE for p in (bid, ask)):
                    out_of_range += 1
                if name(schema.action) == "Trade":
                    trade_checks[classify_trade(schema, last_book)] += 1
                    print(f"trade {name(schema.side):>3} px {fmt(schema.price)} size {schema.size / 1e6:g} "
                          f"| book before {fmt(last_book[0])} / {fmt(last_book[1])}")
                last_book = (bid, ask)

    if dump is not None:
        dump.close()
    print(f"\nmessages: {dict(counts)}")
    print(f"book: crossed {crossed}, out-of-range prices {out_of_range}")
    print(f"NO-side pricing convention (from snapshots): {dict(no_leg_votes) or 'no two-sided snapshot'}")
    print(f"trade fields seen: {dict(trade_fields) or 'no trades'}")
    print(f"trade vs book before: {dict(trade_checks) or 'no trades'}")
    print("  at_touch = Bid aggressor at the ask / Ask aggressor at the bid (what we expect)")
    print("  opposite_touch = printed at the other side's touch (aggressor side likely inverted)")


def classify_trade(schema, book: tuple[int | None, int | None]) -> str:
    bid, ask = book
    side = name(schema.side)
    if bid is None or ask is None:
        return "no_book"
    if (side == "Bid" and schema.price >= ask) or (side == "Ask" and schema.price <= bid):
        return "at_touch"
    if (side == "Bid" and schema.price <= bid) or (side == "Ask" and schema.price >= ask):
        return "opposite_touch"
    return "inside_spread"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ticker")
    parser.add_argument("--seconds", type=float, default=300.0)
    parser.add_argument("--dump", help="write every raw message, prefixed with receive time, to this file")
    args = parser.parse_args()
    asyncio.run(run(args.ticker, args.seconds, args.dump))


if __name__ == "__main__":
    main()
