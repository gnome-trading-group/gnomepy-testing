"""
Validate the Polymarket WebSocket order book parser against the live REST API.

After receiving a WebSocket book snapshot, briefly collects any immediately
following price_change events (to cover the race window while the REST call is
in flight), then compares the locally-maintained book against the CLOB REST API.

Usage:
    poetry run python tests/validate_polymarket_book.py <listing_id>

Examples:
    poetry run python tests/validate_polymarket_book.py 74712
"""
import argparse
import asyncio
import json
import os
import sys
import urllib.error
import urllib.request
from decimal import Decimal

import boto3
import websockets
from sortedcontainers import SortedList

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_REST_HOST = "https://clob.polymarket.com"
POLYMARKET_PING_INTERVAL = 10

_PRICE_SCALE = 1_000_000_000
_SIZE_SCALE = 1_000_000

# Window to collect price_change events after snapshot before querying REST.
DELTA_COLLECT_SECS = 0.1


def resolve_token_id(listing_id: int) -> tuple[str, str]:
    """Return (token_id, display_name) for a listing using the registry."""
    aws_profile = os.environ.get("AWS_PROFILE", "dev")
    import boto3 as _boto3
    session = _boto3.Session(profile_name=aws_profile)
    key_id = os.environ.get("REGISTRY_API_KEY_ID", "rb0pbivke8")
    api_key = session.client("apigateway", region_name="us-east-1").get_api_key(
        apiKey=key_id, includeValue=True
    )["value"]

    import os as _os
    _os.environ["GNOME_REGISTRY_API_KEY"] = api_key
    _os.environ.setdefault("STAGE", "dev")

    from gnomepy.registry.api import RegistryClient
    client = RegistryClient()
    results = client.get_listing(listing_id=listing_id)
    if not results:
        raise ValueError(f"Listing {listing_id} not found")
    listing = results[0]
    sec_id = listing.exchange_security_id or ""
    token_id = sec_id.split(":", 1)[1] if ":" in sec_id else sec_id
    return token_id, listing.exchange_security_symbol or sec_id


def fetch_rest_book(token_id: str) -> dict:
    url = f"{POLYMARKET_REST_HOST}/book?token_id={token_id}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; gnomepy-testing/1.0)",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"REST API {e.code}: {e.read().decode()}") from e


def to_price(s: str) -> int:
    return int(Decimal(s) * _PRICE_SCALE)


def to_size(s: str) -> int:
    return int(Decimal(s) * _SIZE_SCALE)


def update_level(bids: SortedList, asks: SortedList, is_bid: bool, price: int, size: int):
    book = bids if is_bid else asks
    for i in range(len(book)):
        if book[i]["price"] == price:
            if size == 0:
                book.remove(book[i])
            else:
                book[i]["size"] = size
            return
    if size > 0:
        book.add({"price": price, "size": size})


def apply_book_event(bids: SortedList, asks: SortedList, event: dict):
    bids.clear()
    asks.clear()
    for level in event.get("bids", []):
        update_level(bids, asks, True, to_price(level["price"]), to_size(level["size"]))
    for level in event.get("asks", []):
        update_level(bids, asks, False, to_price(level["price"]), to_size(level["size"]))


def apply_price_change(bids: SortedList, asks: SortedList, event: dict, token_id: str):
    for change in event.get("price_changes", []):
        if change.get("asset_id") != token_id:
            continue
        side = change.get("side")
        if side not in ("BUY", "SELL"):
            continue
        price = to_price(change["price"])
        size = to_size(change["size"])
        update_level(bids, asks, side == "BUY", price, size)


def iter_events(data):
    """Yield individual event dicts from a WS message (which may be a list or dict)."""
    if isinstance(data, list):
        yield from data
    elif isinstance(data, dict):
        yield data


async def capture_ws_book(token_id: str):
    """Return (bids, asks, n_deltas) after the book snapshot + brief delta window."""
    bids = SortedList(key=lambda x: -x["price"])
    asks = SortedList(key=lambda x: x["price"])
    got_snapshot = False
    n_deltas = 0

    async with websockets.connect(POLYMARKET_WS_URL) as ws:
        await ws.send(json.dumps({"type": "market", "assets_ids": [token_id]}))

        # Wait for the book snapshot.
        while not got_snapshot:
            raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
            if raw == "PONG":
                continue
            data = json.loads(raw)
            for event in iter_events(data):
                if event.get("event_type") == "book":
                    apply_book_event(bids, asks, event)
                    got_snapshot = True

        # Collect price_change events for DELTA_COLLECT_SECS before querying REST.
        deadline = asyncio.get_event_loop().time() + DELTA_COLLECT_SECS
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if raw == "PONG":
                continue
            data = json.loads(raw)
            for event in iter_events(data):
                if event.get("event_type") == "price_change":
                    apply_price_change(bids, asks, event, token_id)
                    n_deltas += 1

    return bids, asks, n_deltas


def parse_rest_book(rest_data: dict) -> tuple[SortedList, SortedList]:
    bids = SortedList(key=lambda x: -x["price"])
    asks = SortedList(key=lambda x: x["price"])
    for level in rest_data.get("bids", []):
        p, s = to_price(level["price"]), to_size(level["size"])
        if s > 0:
            bids.add({"price": p, "size": s})
    for level in rest_data.get("asks", []):
        p, s = to_price(level["price"]), to_size(level["size"])
        if s > 0:
            asks.add({"price": p, "size": s})
    return bids, asks


def compare_books(
    ws_bids: SortedList, ws_asks: SortedList,
    rest_bids: SortedList, rest_asks: SortedList,
) -> list[tuple]:
    mismatches = []

    ws_bid_map = {e["price"]: e["size"] for e in ws_bids}
    rest_bid_map = {e["price"]: e["size"] for e in rest_bids}
    for price in set(ws_bid_map) | set(rest_bid_map):
        ws_s = ws_bid_map.get(price, 0)
        rest_s = rest_bid_map.get(price, 0)
        if ws_s != rest_s:
            mismatches.append(("BID", price, ws_s, rest_s))

    ws_ask_map = {e["price"]: e["size"] for e in ws_asks}
    rest_ask_map = {e["price"]: e["size"] for e in rest_asks}
    for price in set(ws_ask_map) | set(rest_ask_map):
        ws_s = ws_ask_map.get(price, 0)
        rest_s = rest_ask_map.get(price, 0)
        if ws_s != rest_s:
            mismatches.append(("ASK", price, ws_s, rest_s))

    return mismatches


def fmt_price(p: int) -> str:
    return f"{p / _PRICE_SCALE:.4f}"


def fmt_size(s: int) -> str:
    return f"{s / _SIZE_SCALE:,.2f}"


def print_books(
    ws_bids: SortedList, ws_asks: SortedList,
    rest_bids: SortedList, rest_asks: SortedList,
    max_levels: int = 12,
):
    ws_bid_map = {e["price"]: e["size"] for e in ws_bids}
    rest_bid_map = {e["price"]: e["size"] for e in rest_bids}
    all_bid_prices = sorted(set(ws_bid_map) | set(rest_bid_map), reverse=True)

    ws_ask_map = {e["price"]: e["size"] for e in ws_asks}
    rest_ask_map = {e["price"]: e["size"] for e in rest_asks}
    all_ask_prices = sorted(set(ws_ask_map) | set(rest_ask_map))

    print(f"\n{'SIDE':4} {'PRICE':>8}  {'WS SIZE':>12}  {'REST SIZE':>12}  {'MATCH':>5}")
    print("-" * 52)

    shown = 0
    for price in all_bid_prices:
        ws_s = ws_bid_map.get(price, 0)
        rest_s = rest_bid_map.get(price, 0)
        ok = "✓" if ws_s == rest_s else "✗"
        print(f"{'BID':4} {fmt_price(price):>8}  {fmt_size(ws_s):>12}  {fmt_size(rest_s):>12}  {ok:>5}")
        shown += 1
        if shown >= max_levels:
            if len(all_bid_prices) > shown:
                print(f"     ... {len(all_bid_prices) - shown} more bid levels ...")
            break

    shown = 0
    for price in all_ask_prices:
        ws_s = ws_ask_map.get(price, 0)
        rest_s = rest_ask_map.get(price, 0)
        ok = "✓" if ws_s == rest_s else "✗"
        print(f"{'ASK':4} {fmt_price(price):>8}  {fmt_size(ws_s):>12}  {fmt_size(rest_s):>12}  {ok:>5}")
        shown += 1
        if shown >= max_levels:
            if len(all_ask_prices) > shown:
                print(f"     ... {len(all_ask_prices) - shown} more ask levels ...")
            break


async def main():
    parser = argparse.ArgumentParser(
        description="Compare WS-parsed Polymarket book against CLOB REST API"
    )
    parser.add_argument("listing_id", type=int, help="Dev registry listing ID")
    args = parser.parse_args()

    print(f"Resolving listing {args.listing_id}...")
    token_id, display_name = resolve_token_id(args.listing_id)
    print(f"  {display_name}")
    print(f"  token_id: {token_id[:20]}...")

    print(f"Subscribing via WebSocket...")
    ws_bids, ws_asks, n_deltas = await capture_ws_book(token_id)
    print(f"Snapshot received, applied {n_deltas} racing price_change event(s).")

    print(f"Querying CLOB REST API...")
    rest_data = fetch_rest_book(token_id)
    rest_bids, rest_asks = parse_rest_book(rest_data)

    print_books(ws_bids, ws_asks, rest_bids, rest_asks)

    mismatches = compare_books(ws_bids, ws_asks, rest_bids, rest_asks)
    print()
    if not mismatches:
        print("✓ PASS: WS book matches REST API exactly.")
    else:
        print(f"✗ FAIL: {len(mismatches)} level(s) differ:")
        for side, price, ws_s, rest_s in sorted(mismatches, key=lambda x: -x[1]):
            print(f"  {side} {fmt_price(price)}  WS={fmt_size(ws_s)}  REST={fmt_size(rest_s)}  diff={fmt_size(ws_s - rest_s)}")
        print()
        print("Note: residual mismatches are likely the race between delta collection")
        print("and the REST fetch, not a parser bug.")
    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
