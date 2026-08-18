"""
Validate the Kalshi WebSocket order book parser against the live REST API.

After receiving a WebSocket orderbook_snapshot, briefly collects any immediately
following deltas (to cover the race window while the REST call is in flight), then
compares the locally-maintained book against the REST orderbook.

Usage:
    poetry run python tests/validate_kalshi_book.py KXBTCD-26AUG2117-T62999.99
"""
import argparse
import asyncio
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal

import boto3
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"
KALSHI_REST_HOST = "https://api.elections.kalshi.com"
PRICE_ARRAY_SIZE = 100

# How long to keep receiving deltas after the snapshot before querying REST.
# Longer = fewer false mismatches from the race window, but the REST state also
# advances during that time.  100 ms is generally enough.
DELTA_COLLECT_SECS = 0.1


def load_credentials():
    api_key = os.environ.get("KALSHI_API_KEY")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if api_key and key_path:
        with open(key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        return api_key, private_key
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId="gnome/exchange-credentials/kalshi"
    )
    data = json.loads(secret["SecretString"])
    private_key = serialization.load_pem_private_key(
        data["privateKey"].encode(), password=None
    )
    return data["apiKey"], private_key


def make_auth_headers(api_key, private_key, path):
    ts = str(int(time.time() * 1000))
    payload = f"{ts}GET{path}".encode()
    sig = private_key.sign(
        payload,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def fetch_rest_book(ticker, api_key, private_key):
    path = f"/trade-api/v2/markets/{ticker}/orderbook"
    headers = make_auth_headers(api_key, private_key, path)
    req = urllib.request.Request(KALSHI_REST_HOST + path, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"REST API {e.code}: {body}") from e


async def capture_ws_book(ticker, api_key, private_key):
    """Return (yes_qty, no_qty, seq, n_deltas_applied) after snapshot + brief delta window."""
    ws_headers = make_auth_headers(api_key, private_key, KALSHI_WS_PATH)
    extra = list(ws_headers.items())

    async with websockets.connect(KALSHI_WS_URL, additional_headers=extra) as ws:
        await ws.send(json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": [ticker]},
        }))

        yes_qty = [0] * PRICE_ARRAY_SIZE
        no_qty = [0] * PRICE_ARRAY_SIZE
        seq = None

        # Wait for the snapshot.
        while True:
            data = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
            if data.get("type") == "orderbook_snapshot":
                seq = data.get("seq")
                msg = data["msg"]
                for price_str, qty_str in msg.get("yes_dollars_fp", []):
                    cents = int(Decimal(price_str) * 100)
                    if 0 < cents < PRICE_ARRAY_SIZE:
                        yes_qty[cents] = int(Decimal(qty_str))
                for price_str, qty_str in msg.get("no_dollars_fp", []):
                    cents = int(Decimal(price_str) * 100)
                    if 0 < cents < PRICE_ARRAY_SIZE:
                        no_qty[cents] = int(Decimal(qty_str))
                break

        # Collect deltas for DELTA_COLLECT_SECS before fetching REST.
        n_deltas = 0
        deadline = asyncio.get_event_loop().time() + DELTA_COLLECT_SECS
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                data = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
            except asyncio.TimeoutError:
                break
            if data.get("type") == "orderbook_delta":
                seq = data.get("seq", seq)
                m = data["msg"]
                cents = int(Decimal(m["price_dollars"]) * 100)
                delta = int(Decimal(m["delta_fp"]))
                side = m.get("side", "")
                if 0 < cents < PRICE_ARRAY_SIZE:
                    if side == "yes":
                        yes_qty[cents] = max(0, yes_qty[cents] + delta)
                    elif side == "no":
                        no_qty[cents] = max(0, no_qty[cents] + delta)
                n_deltas += 1

    return yes_qty, no_qty, seq, n_deltas


def parse_rest_book(rest_data):
    """
    Parse REST orderbook response.

    REST returns {"orderbook_fp": {"yes_dollars": [[price_str, qty_str], ...], ...}}
    with the same dollar-string format as the WebSocket yes_dollars_fp field.
    Quantities are truncated to integer dollars to match the WS parser.
    """
    ob = rest_data.get("orderbook_fp", {})
    yes = {}
    for price_str, qty_str in ob.get("yes_dollars", []):
        cents = int(Decimal(price_str) * 100)
        if 0 < cents < PRICE_ARRAY_SIZE:
            yes[cents] = int(Decimal(qty_str))
    no = {}
    for price_str, qty_str in ob.get("no_dollars", []):
        cents = int(Decimal(price_str) * 100)
        if 0 < cents < PRICE_ARRAY_SIZE:
            no[cents] = int(Decimal(qty_str))
    return yes, no


def compare_books(ws_yes, ws_no, rest_yes, rest_no):
    mismatches = []

    all_yes_prices = set(range(1, PRICE_ARRAY_SIZE)) & (
        {p for p, q in enumerate(ws_yes) if q > 0} | set(rest_yes)
    )
    for p in sorted(all_yes_prices, reverse=True):
        ws_q = ws_yes[p]
        rest_q = rest_yes.get(p, 0)
        if ws_q != rest_q:
            mismatches.append(("YES", p, ws_q, rest_q))

    all_no_prices = set(range(1, PRICE_ARRAY_SIZE)) & (
        {p for p, q in enumerate(ws_no) if q > 0} | set(rest_no)
    )
    for p in sorted(all_no_prices, reverse=True):
        ws_q = ws_no[p]
        rest_q = rest_no.get(p, 0)
        if ws_q != rest_q:
            mismatches.append(("NO", p, ws_q, rest_q))

    return mismatches


def print_book(ws_yes, ws_no, rest_yes, rest_no):
    print(f"\n{'SIDE':4} {'CENTS':>5}  {'WS ($)':>12}  {'REST ($)':>12}  {'MATCH':>5}")
    print("-" * 50)

    all_yes = sorted(
        set(p for p in range(1, PRICE_ARRAY_SIZE) if ws_yes[p] > 0) | set(rest_yes),
        reverse=True,
    )
    shown = 0
    for p in all_yes:
        ws_q = ws_yes[p]
        rest_q = rest_yes.get(p, 0)
        ok = "✓" if ws_q == rest_q else "✗"
        print(f"{'YES':4} {p:>4}¢  {ws_q:>12,}  {rest_q:>12,}  {ok:>5}")
        shown += 1
        if shown >= 15:
            remaining = len(all_yes) - shown
            if remaining:
                print(f"     ... {remaining} more YES levels ...")
            break

    all_no = sorted(
        set(p for p in range(1, PRICE_ARRAY_SIZE) if ws_no[p] > 0) | set(rest_no),
        reverse=True,
    )
    shown = 0
    for p in all_no:
        ws_q = ws_no[p]
        rest_q = rest_no.get(p, 0)
        ok = "✓" if ws_q == rest_q else "✗"
        ask_p = PRICE_ARRAY_SIZE - p
        print(f"{'NO':4} {p:>4}¢  {ws_q:>12,}  {rest_q:>12,}  {ok:>5}  (YES ask = {ask_p}¢)")
        shown += 1
        if shown >= 15:
            remaining = len(all_no) - shown
            if remaining:
                print(f"     ... {remaining} more NO levels ...")
            break


async def main():
    parser = argparse.ArgumentParser(
        description="Compare WS-parsed Kalshi book against REST API"
    )
    parser.add_argument("ticker", help="Market ticker, e.g. KXBTCD-26AUG2117-T62999.99")
    args = parser.parse_args()
    ticker = args.ticker

    print(f"Loading credentials...")
    api_key, private_key = load_credentials()

    print(f"Subscribing to {ticker} via WebSocket...")
    ws_yes, ws_no, seq, n_deltas = await capture_ws_book(ticker, api_key, private_key)
    print(f"Snapshot received (seq={seq}), applied {n_deltas} racing delta(s).")

    print(f"Querying REST API...")
    rest_data = fetch_rest_book(ticker, api_key, private_key)
    rest_yes, rest_no = parse_rest_book(rest_data)

    print_book(ws_yes, ws_no, rest_yes, rest_no)

    mismatches = compare_books(ws_yes, ws_no, rest_yes, rest_no)
    print()
    if not mismatches:
        print("✓ PASS: WS book matches REST API exactly.")
    else:
        print(f"✗ FAIL: {len(mismatches)} level(s) differ:")
        for side, cents, ws_q, rest_q in mismatches:
            print(f"  {side} {cents}¢  WS={ws_q:,}  REST={rest_q:,}  diff={ws_q - rest_q:+,}")
        print()
        print("Note: residual mismatches after the delta window are likely due to")
        print("the race between delta collection and the REST fetch, not a bug.")
    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
