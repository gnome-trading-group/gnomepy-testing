"""
Proxy client - connects to the Capture Proxy and writes binary output.

This client:
1. Connects to the Capture Proxy server
2. Receives raw exchange messages
3. Parses them into gnomepy schema objects (MBP1, MBP10, etc.) using stateful parsers
4. Writes the encoded bytes to an output file
"""
import argparse
import asyncio
import json
import logging
from pathlib import Path

from gnomepy import SchemaBase

from gnomepy_testing.output import BinaryOutputHandler
from gnomepy_testing.listing_resolver import  resolve_listing
from gnomepy_testing.network import (
    Transport,
    ProtocolHandler,
    create_transport,
    create_protocol_handler
)
from .exchange_parsers import create_parser, ExchangeParser


logger = logging.getLogger(__name__)


class ProxyClient:
    """
    Client that connects to the Capture Proxy server and writes binary output.

    Uses the same layered architecture as the exchange connector:
    - Transport layer (WebSocket, TCP, etc.)
    - Protocol layer (JSON, FIX, binary, etc.)

    The transport and protocol types are determined by the exchange parser,
    which knows what the exchange requires.
    """

    def __init__(
        self,
        proxy_host: str,
        proxy_port: int,
        output_file: Path,
        listing_id: int,
    ):
        """
        Initialize the proxy client.

        Args:
            proxy_host: Proxy server host
            proxy_port: Proxy server port
            output_file: Path to write binary output
            listing_id: Listing ID to resolve
        """
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.output_file = Path(output_file)

        self.listing_info = resolve_listing(listing_id)

        self.parser: ExchangeParser = create_parser(self.listing_info)

        self.transport: Transport = create_transport(self.parser.get_transport_type())
        self.protocol: ProtocolHandler = create_protocol_handler(self.parser.get_protocol_type())

        self.output_handler: BinaryOutputHandler | None = None
        self._running = False
        self.message_count = 0
    
    def _build_connection_url(self) -> str:
        """Build the connection URL based on transport type."""
        from gnomepy_testing.network import WebSocketTransport

        if isinstance(self.transport, WebSocketTransport):
            return f"ws://{self.proxy_host}:{self.proxy_port}"
        else:
            return f"{self.proxy_host}:{self.proxy_port}"

    async def connect(self):
        """Connect to the proxy server."""
        url = self._build_connection_url()
        logger.info(f"Connecting to proxy at {url}")

        await self.transport.connect(url)
        self._running = True

        self.output_handler = BinaryOutputHandler(self.output_file)
        self.output_handler.open()

        logger.info("Connected to proxy server")
    
    async def run(self):
        """Run the client, receiving and parsing messages."""
        try:
            async for raw_data in self.transport.receive():
                if not self._running:
                    break

                try:
                    decoded_data = self.protocol.decode_message(raw_data)
                    await self._handle_message(decoded_data)
                except Exception as e:
                    logger.error(f"Error decoding message: {e}")

        except asyncio.CancelledError:
            logger.info("Client cancelled")
        except Exception as e:
            logger.error(f"Error in client: {e}")
            raise
        finally:
            self._running = False
            if self.output_handler:
                self.output_handler.close()
    
    async def _handle_message(self, message: bytes):
        """Handle a message from the proxy."""
        try:
            await self._parse_exchange_message(message)

        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {message[:100]}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _handle_metadata(self, metadata: dict):
        """Handle metadata message from proxy."""
        logger.info(f"Received metadata from proxy: {metadata}")
        logger.info(f"Using parser: {self.parser.__class__.__name__}")

    def _write_schema_object(self, schema_obj: SchemaBase):
        """Write a schema object to the output file."""
        if self.output_handler:
            self.output_handler.write(schema_obj)
            self.message_count += 1

            if self.message_count % 100 == 0:
                logger.info(f"Processed {self.message_count} messages")
    
    async def _parse_exchange_message(self, data: bytes):
        """Parse an exchange message into a gnomepy schema object."""
        if not self.parser:
            logger.warning("Received exchange message before metadata/parser initialized")
            return

        try:
            self.parser.parse(data, self._write_schema_object)
        except Exception as e:
            logger.error(f"Error parsing message: {e}")

    async def stop(self):
        """Stop the client."""
        self._running = False
        await self.transport.close()
        if self.output_handler:
            self.output_handler.close()


async def main():
    """Main entry point for running the proxy client."""
    parser = argparse.ArgumentParser(description='Proxy Client')
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Proxy server host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8765,
        help='Proxy server port (default: 8765)'
    )
    parser.add_argument(
        '--output',
        '-o',
        type=Path,
        required=True,
        help='Output file for binary data'
    )
    parser.add_argument(
        '--listing',
        '-l',
        type=int,
        default=None,
        required=True,
        help='Listing ID to resolve and subscribe'
    )

    args = parser.parse_args()

    client = ProxyClient(
        proxy_host=args.host,
        proxy_port=args.port,
        output_file=args.output,
        listing_id=args.listing,
    )

    await client.connect()

    try:
        await client.run()
    finally:
        await client.stop()
        logger.info(f"Client stopped. Processed {client.message_count} messages")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())

