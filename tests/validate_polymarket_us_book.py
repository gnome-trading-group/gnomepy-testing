"""
Validate the Polymarket US WebSocket feed assumptions against the live API.

The gateway treats every marketData message as a full top-of-book snapshot. This script
subscribes to one market, then for each marketData message:
  - reports whether it looks like a snapshot (both sides present, sorted best-to-worst)
  - compares it against the public REST book fetched immediately after
It also reports heartbeat cadence and any fields (e.g. sequence numbers) the docs don't mention.

Usage:
    poetry run python tests/validate_polymarket_us_book.py tec-mlb-nlchamp-2026-09-27-atl --seconds 60
"""
import argparse
import asyncio
import base64
import json
import os
import time
import urllib.request
from collections import Counter
from decimal import Decimal

import boto3
import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

WS_PATH = "/v1/ws/markets"
WS_URL = "wss://api.polymarket.us" + WS_PATH
REST_BOOK_URL = "https://gateway.polymarket.us/v1/markets/{slug}/book"
DOCUMENTED_MARKET_DATA_KEYS = {"marketSlug", "bids", "offers", "state", "stats", "transactTime"}


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("POLYMARKET_US_API_KEY")
    secret = os.environ.get("POLYMARKET_US_SECRET")
    if api_key and secret:
        return api_key, secret
    response = boto3.client("secretsmanager").get_secret_value(SecretId="gnome/exchange-credentials/polymarket-us")
    data = json.loads(response["SecretString"])
    return data["apiKey"], data["secret"]


def auth_headers(api_key: str, secret: str) -> dict[str, str]:
    private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(secret)[:32])
    ts = str(int(time.time() * 1000))
    signature = private_key.sign(f"{ts}GET{WS_PATH}".encode())
    return {"X-PM-Access-Key": api_key, "X-PM-Timestamp": ts, "X-PM-Signature": base64.b64encode(signature).decode()}


def levels(raw: list[dict]) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(level["px"]["value"]), Decimal(level["qty"])) for level in raw]


def rest_book(slug: str) -> tuple[list, list]:
    request = urllib.request.Request(REST_BOOK_URL.format(slug=slug), headers={"User-Agent": "gnome-validate"})
    with urllib.request.urlopen(request, timeout=10) as response:
        data = json.load(response)["marketData"]
    return levels(data.get("bids") or []), levels(data.get("offers") or [])


async def run(slug: str, seconds: float, dump_path: str | None) -> None:
    api_key, secret = load_credentials()
    dump = open(dump_path, "w") if dump_path else None
    counts: Counter[str] = Counter()
    unknown_keys: Counter[str] = Counter()
    heartbeat_times: list[float] = []
    matches = mismatches = unsorted = one_sided = 0

    async with websockets.connect(WS_URL, additional_headers=auth_headers(api_key, secret)) as ws:
        for request_id, sub_type in (("md", "SUBSCRIPTION_TYPE_MARKET_DATA"), ("tr", "SUBSCRIPTION_TYPE_TRADE")):
            await ws.send(json.dumps({"subscribe": {"requestId": request_id, "subscriptionType": sub_type, "marketSlugs": [slug]}}))

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                break
            message = json.loads(raw)
            if dump is not None:
                dump.write(f"{time.time():.3f} {raw}\n")
            kind = next((k for k in ("marketData", "trade", "heartbeat", "error") if k in message), "other")
            counts[kind] += 1
            if kind == "heartbeat":
                heartbeat_times.append(time.monotonic())
            elif kind in ("error", "other"):
                print(f"{kind}: {message}")
            elif kind == "marketData":
                market_data = message["marketData"]
                unknown_keys.update(set(market_data) - DOCUMENTED_MARKET_DATA_KEYS)
                bids, offers = levels(market_data.get("bids") or []), levels(market_data.get("offers") or [])
                if not bids or not offers:
                    one_sided += 1
                if [p for p, _ in bids] != sorted((p for p, _ in bids), reverse=True) or [p for p, _ in offers] != sorted(p for p, _ in offers):
                    unsorted += 1
                rest_bids, rest_offers = rest_book(slug)
                depth = min(len(bids), len(rest_bids), 10), min(len(offers), len(rest_offers), 10)
                if bids[:depth[0]] == rest_bids[:depth[0]] and offers[:depth[1]] == rest_offers[:depth[1]]:
                    matches += 1
                else:
                    mismatches += 1
                    print(f"mismatch at {market_data.get('transactTime')}: ws {len(bids)}x{len(offers)} levels, rest {len(rest_bids)}x{len(rest_offers)}")
                    for name, ws_side, rest_side in (("bid", bids, rest_bids), ("offer", offers, rest_offers)):
                        for i in range(max(len(ws_side), len(rest_side))):
                            ws_level = ws_side[i] if i < len(ws_side) else None
                            rest_level = rest_side[i] if i < len(rest_side) else None
                            if ws_level != rest_level:
                                print(f"  {name}[{i}] ws={ws_level} rest={rest_level}")

    if dump is not None:
        dump.close()
    gaps = [b - a for a, b in zip(heartbeat_times, heartbeat_times[1:])]
    print(f"\nmessages: {dict(counts)}")
    print(f"marketData vs REST: {matches} match, {mismatches} mismatch (mismatches are expected only when the book moves between the two)")
    print(f"marketData one-sided: {one_sided}, unsorted: {unsorted}")
    print(f"undocumented marketData keys: {dict(unknown_keys) or 'none'}")
    if gaps:
        print(f"heartbeat interval: min {min(gaps):.1f}s, max {max(gaps):.1f}s")
    elif heartbeat_times:
        print("heartbeat interval: only one heartbeat seen")
    else:
        print("heartbeat interval: no heartbeats seen")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("slug", help="Polymarket US market slug")
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--dump", help="write every raw message, prefixed with receive time, to this file")
    args = parser.parse_args()
    asyncio.run(run(args.slug, args.seconds, args.dump))


if __name__ == "__main__":
    main()
