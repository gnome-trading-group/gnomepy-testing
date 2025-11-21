"""
Entry point script for running the proxy client.

This separate entry point avoids the RuntimeWarning that occurs when
running proxy_client.py directly with -m.
"""
import asyncio
import logging

from gnomepy_testing.client.proxy_client import main


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())

