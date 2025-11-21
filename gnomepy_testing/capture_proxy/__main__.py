"""
Entry point for running the capture proxy server as a module.

Usage:
    python -m gnomepy_testing.capture_proxy BINANCE:BTCUSDT:MBP1 --port 8765 --duration 60
"""
import asyncio
from .server import main

if __name__ == '__main__':
    asyncio.run(main())

