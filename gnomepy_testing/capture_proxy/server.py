"""
Capture Proxy Server - Generic transport/protocol server that forwards exchange data to clients.

The server:
1. Resolves listing ID to exchange/security information
2. Connects to an exchange using the appropriate transport/protocol
3. Accepts client connections using the same transport/protocol as the exchange
4. Buffers subscription responses and market data separately
5. Replays subscription responses + market data to new clients
6. Forwards all exchange messages to all connected clients
7. Ensures all clients receive identical data
"""
import argparse
import asyncio
import logging
import signal
import sys
from typing import Any
from pathlib import Path
from collections import deque

from gnomepy_testing.listing_resolver import resolve_listing, ListingInfo
from gnomepy_testing.network import (
    TransportServer,
    create_transport_server,
    ProtocolHandler,
    create_protocol_handler, WebSocketServer
)
from .exchange_connector import create_exchange_connector, ExchangeConnector


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CaptureProxyServer:
    """
    Generic proxy server that captures exchange data and forwards to clients.

    Uses the same transport/protocol as the exchange to communicate with clients.
    """

    def __init__(
        self,
        listing_id: int,
        host: str = "0.0.0.0",
        port: int = 8765,
        log_file: Path | None = None,
        buffer_size: int = 1000,
        expected_clients: int = 2,
    ):
        """
        Initialize the capture proxy server.

        Args:
            listing_id: The listing ID to resolve and subscribe to
            host: Host to bind the server to
            port: Port to bind the server to
            log_file: Optional file to log all messages
            buffer_size: Maximum number of market data messages to buffer (default: 1000)
            expected_clients: Number of clients to wait for before connecting to exchange (default: 2)
        """
        self.listing_id = listing_id
        self.listing_info: ListingInfo | None = None
        self.host = host
        self.port = port
        self.log_file = log_file
        self.buffer_size = buffer_size
        self.expected_clients = expected_clients

        self.message_buffer: deque[bytes] = deque(maxlen=buffer_size)

        self.transport_server: TransportServer | None = None
        self.protocol: ProtocolHandler | None = None
        self.exchange_connector: ExchangeConnector | None = None

        self._running = False
        self._exchange_task: asyncio.Task | None = None
        self.message_count = 0
        self.subscription_message_count = 0
        self._log_handle = None

        self._connected_clients: set[Any] = set()
        self._all_clients_connected = asyncio.Event()
        self._client_wait_timeout = 30.0

    async def start(self):
        """Start the proxy server."""
        logger.info(f"Resolving listing ID: {self.listing_id}")
        self.listing_info = resolve_listing(self.listing_id)
        logger.info(f"Resolved to: {self.listing_info}")

        logger.info(f"Starting Capture Proxy Server for {self.listing_info}")
        logger.info(f"Server will listen on {self.host}:{self.port}")

        if self.log_file:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            self._log_handle = open(self.log_file, 'w')
            logger.info(f"Logging messages to {self.log_file}")

        self.exchange_connector = create_exchange_connector(
            self.listing_info,
            self._on_exchange_message
        )

        transport_type = self.exchange_connector.get_transport_type()
        protocol_type = self.exchange_connector.get_protocol_type()

        logger.info(f"Using transport: {transport_type.value}, protocol: {protocol_type.value}")

        self.transport_server = create_transport_server(transport_type)
        self.protocol = create_protocol_handler(protocol_type)

        await self.transport_server.start(self.host, self.port, self._handle_client)

        logger.info(f"Proxy server started on {self.host}:{self.port}")
        logger.info(f"Waiting for {self.expected_clients} client(s) to connect...")

        self._running = True

    async def _wait_for_clients_and_connect(self, shutdown_event: asyncio.Event):
        """Wait for expected clients to connect, then connect to exchange."""
        clients_task = asyncio.create_task(self._all_clients_connected.wait())
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        timeout_task = asyncio.create_task(asyncio.sleep(self._client_wait_timeout))

        try:
            done, pending = await asyncio.wait(
                {clients_task, shutdown_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

            if shutdown_task in done:
                logger.info("Shutdown signal received while waiting for clients")
                raise asyncio.CancelledError()

            if timeout_task in done:
                logger.error(f"Timeout waiting for {self.expected_clients} clients to connect (waited {self._client_wait_timeout}s)")
                logger.error(f"Only {len(self._connected_clients)} client(s) connected")
                raise RuntimeError(f"Timeout waiting for clients to connect")

            logger.info(f"All {self.expected_clients} clients connected. Connecting to exchange...")
            self._exchange_task = asyncio.create_task(self.exchange_connector.connect())
            logger.info("Exchange connector task started")

        except asyncio.CancelledError:
            for task in [clients_task, shutdown_task, timeout_task]:
                if not task.done():
                    task.cancel()
            raise

    async def _handle_client(self, client: Any):
        """
        Handle a new client connection.

        Args:
            client: The client connection object (type depends on transport)
        """
        try:
            client_addr = self._get_client_address(client)

            if len(self._connected_clients) >= self.expected_clients:
                logger.error(f"Rejecting client {client_addr}: already have {self.expected_clients} client(s) connected")
                await self.transport_server.close_client(client)
                logger.error(f"Too many clients attempted to connect. Exiting.")
                asyncio.create_task(self._shutdown_due_to_error())
                return

            self._connected_clients.add(client)
            logger.info(f"Client connected: {client_addr} ({len(self._connected_clients)}/{self.expected_clients})")

            if len(self._connected_clients) == self.expected_clients:
                logger.info(f"All {self.expected_clients} expected clients connected!")
                self._all_clients_connected.set()

            if self.message_buffer:
                logger.info(f"Replaying {len(self.message_buffer)} market data messages to {client_addr}")
                for buffered_msg in self.message_buffer:
                    encoded_msg = self.protocol.encode_message(buffered_msg)
                    await self.transport_server.send_to_client(client, encoded_msg)
                logger.info(f"Finished replaying market data to {client_addr}")

            await self._client_receive_loop(client)

        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            if client in self._connected_clients:
                self._connected_clients.remove(client)
                client_addr = self._get_client_address(client)
                logger.error(f"Client disconnected: {client_addr}")
                if self._all_clients_connected.is_set() and self._running:
                    logger.error(f"Client disconnected after exchange connection started. Exiting.")
                    asyncio.create_task(self._shutdown_due_to_error())

    async def _shutdown_due_to_error(self):
        """Shutdown the server due to an error condition."""
        await asyncio.sleep(0.1)
        await self.stop()
        sys.exit(1)

    def _get_client_address(self, client: Any) -> str:
        """Get client address string from client object."""
        if hasattr(client, 'remote_address'):
            addr = client.remote_address
            return f"{addr[0]}:{addr[1]}"
        elif hasattr(client, 'get_extra_info'):
            addr = client.get_extra_info('peername')
            return f"{addr[0]}:{addr[1]}" if addr else "unknown"
        return "unknown"

    async def _client_receive_loop(self, client: Any):
        """
        Receive loop for client messages (e.g., subscription requests).

        For now, we just log any messages from clients.
        """
        if isinstance(self.transport_server, WebSocketServer):
            try:
                async for message in client:
                    logger.info(f"Received message from client: {message}")
            except Exception as e:
                logger.debug(f"Client receive loop ended: {e}")
        else:
            pass
    
    def _on_exchange_message(self, message: bytes):
        """Handle a message received from the exchange."""
        self.message_count += 1
        self.message_buffer.append(message)

        if self.message_count % 100 == 0:
            logger.info(f"Forwarded {self.message_count} market data messages")

        if self._log_handle:
            log_message = str(message) + '\n'
            self._log_handle.write(log_message)
            self._log_handle.flush()

        asyncio.create_task(self._broadcast_message(message))
    
    async def _broadcast_message(self, message: bytes):
        """Broadcast a message to all connected clients."""
        if self.transport_server:
            try:
                await self.transport_server.broadcast(
                    self.protocol.encode_message(message)
                )
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")
    
    async def stop(self):
        """Stop the proxy server."""
        if not self._running:
            return

        logger.info("Stopping Capture Proxy Server...")
        self._running = False

        if self._exchange_task and not self._exchange_task.done():
            logger.info("Cancelling exchange connector task...")
            self._exchange_task.cancel()
            try:
                await asyncio.wait_for(self._exchange_task, timeout=2.0)
            except asyncio.CancelledError:
                logger.info("Exchange task cancelled")
            except asyncio.TimeoutError:
                logger.warning("Exchange task cancellation timed out")
            except Exception as e:
                logger.error(f"Error cancelling exchange task: {e}")

        if self.exchange_connector:
            try:
                await asyncio.wait_for(self.exchange_connector.disconnect(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Exchange disconnect timed out")
            except Exception as e:
                logger.error(f"Error disconnecting from exchange: {e}")

        if self.transport_server:
            try:
                await asyncio.wait_for(self.transport_server.stop(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Transport server stop timed out")
            except Exception as e:
                logger.error(f"Error stopping transport server: {e}")

        if self._log_handle:
            self._log_handle.close()

        logger.info(f"Proxy server stopped. Subscription messages: {self.subscription_message_count}, Market data: {self.message_count}")
    
    async def run(self, duration_seconds: int | None = None):
        """
        Run the proxy server.

        Args:
            duration_seconds: Optional duration to run after all clients connect. If None, runs until interrupted.
        """
        loop = asyncio.get_event_loop()
        shutdown_event = asyncio.Event()

        def signal_handler():
            logger.info("Received shutdown signal")
            shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)

        try:
            await self.start()

            await self._wait_for_clients_and_connect(shutdown_event)

            if duration_seconds:
                logger.info(f"Running for {duration_seconds} seconds...")
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=duration_seconds)
                    logger.info("Shutdown signal received")
                except asyncio.TimeoutError:
                    logger.info("Duration elapsed")
            else:
                logger.info("Running until interrupted (Ctrl+C)...")
                await shutdown_event.wait()

        except asyncio.CancelledError:
            logger.info("Server task cancelled")
        except Exception as e:
            logger.error(f"Error running proxy server: {e}")
            raise
        finally:
            if self._running:
                await self.stop()


async def main():
    """Main entry point for running the proxy server."""
    parser = argparse.ArgumentParser(description='Capture Proxy Server')
    parser.add_argument(
        'listing_id',
        type=int,
        help='Listing ID to resolve and subscribe to'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host to bind to (default: 0.0.0.0)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8765,
        help='Port to bind to (default: 8765)'
    )
    parser.add_argument(
        '--duration',
        '-d',
        type=int,
        default=None,
        help='Duration to run in seconds after all clients connect (default: run until interrupted)'
    )
    parser.add_argument(
        '--log-file',
        '-l',
        type=Path,
        default=None,
        help='File to log all messages'
    )
    parser.add_argument(
        '--expected-clients',
        '-c',
        type=int,
        default=2,
        help='Number of clients to wait for before connecting to exchange (default: 2)'
    )

    args = parser.parse_args()

    server = CaptureProxyServer(
        listing_id=args.listing_id,
        host=args.host,
        port=args.port,
        log_file=args.log_file,
        expected_clients=args.expected_clients
    )

    await server.run(duration_seconds=args.duration)


if __name__ == '__main__':
    asyncio.run(main())

