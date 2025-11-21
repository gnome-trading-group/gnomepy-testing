# gnomepy-testing

This package is used internally at GTG for testing market data implementations.

## Overview

A comprehensive testing framework for validating Python market data implementations against Java implementations for crypto exchanges using a **Capture Proxy** architecture with **gnomepy** schema types.

### How It Works

1. **Capture Proxy** - Connects to the exchange and receives raw market data
2. **Both clients** (Python & Java) connect to the proxy and receive identical raw messages
3. **Schema Parsing** - Each client parses messages into gnomepy schema objects (MBP1, MBP10, etc.)
4. **Binary Output** - Clients encode schema objects to binary files
5. **Byte-level Comparison** - Binary outputs are compared to verify implementations match exactly

This ensures both implementations receive the exact same data and produce identical binary output.

## Quick Start

```bash
# Install dependencies
poetry install

# Run a quick test (proxy + Python client)
make test-proxy LISTING_ID=1 DURATION=60

# Or manually
# Terminal 1: Start proxy (listing ID 1 = Hyperliquid BTC-USD)
poetry run python -m gnomepy_testing.capture_proxy 1 --duration 60

# Terminal 2: Start Python client
poetry run python -m gnomepy_testing.client.proxy_client \
    --host localhost --port 8765 --output output/python.bin --duration 30
```

## Docker Usage

```bash
# Build
make docker-build

# Run test with default listing (ID 1)
make docker-test

# Custom test with different listing
make docker-test LISTING_ID=2 DURATION=120
```

## Architecture

```
Registry API
     ↓
Listing Resolver (listing_id → exchange/security info)
     ↓
Exchange (Hyperliquid/Lighter/Binance/...)
     ↓
Capture Proxy Server
     ↓
┌────┴────┐
↓         ↓
Python    Java
Client    Client
↓         ↓
Parse     Parse
↓         ↓
MBP1/10   MBP1/10
↓         ↓
Encode    Encode
↓         ↓
python.   java.
bin       bin
└────┬────┘
     ↓
Binary Compare
     ↓
pass or fail
```

## Project Structure

```
gnomepy-testing/
├── gnomepy_testing/
│   ├── capture_proxy/         # Proxy server and exchange connectors
│   ├── client/                # Python proxy client
│   ├── output/                # Binary output handler
│   ├── network/               # Transport/protocol abstraction
│   ├── listing_resolver.py    # Registry API integration
│   └── compare_outputs.py     # Binary output comparison
├── docker/                    # Docker configuration
├── output/                    # Test outputs (*.bin, *.log)
└── tests/                     # Unit tests
```

## Releasing a new version

The GitHub Actions workflow will automatically run with a tag matching the
pattern `v*.*.*` is released.

```commandline
poetry version patch  # or minor/major
git commit -m "Release new version"
git tag v$(poetry version -s)
git push origin main --tags
```