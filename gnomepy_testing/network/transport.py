"""
Transport layer for exchange connections.

Transports handle the low-level socket connections and provide
a common interface for sending/receiving bytes regardless of
the underlying protocol (WebSocket, TCP, UDP, etc.).
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import AsyncIterator, TypeVar, Generic, Callable, Awaitable

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.server import WebSocketServerProtocol, serve

T = TypeVar('T')

logger = logging.getLogger(__name__)


class TransportType(Enum):
    """Supported transport types."""
    WEBSOCKET = "websocket"
    TCP = "tcp"


class Transport(ABC, Generic[T]):
    """Base class for transport implementations."""

    @abstractmethod
    async def connect(self, url: str) -> None:
        """
        Connect to the remote endpoint.
        
        Args:
            url: Connection URL/address
        """
        pass

    @abstractmethod
    async def send(self, data: T) -> None:
        """
        Send bytes to the remote endpoint.

        Args:
            data: Bytes to send
        """
        pass

    @abstractmethod
    async def receive(self) -> AsyncIterator[T]:
        """
        Receive bytes from the remote endpoint.
        
        Yields:
            Bytes received from the connection
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the transport is connected."""
        pass


class WebSocketTransport(Transport[str | bytes]):
    """WebSocket transport implementation."""

    def __init__(self):
        self.ws: WebSocketClientProtocol | None = None

    async def connect(self, url: str) -> None:
        """Connect to WebSocket URL."""
        logger.info(f"Connecting to WebSocket: {url}")
        self.ws = await websockets.connect(url)
        logger.info(f"WebSocket connected: {url}")

    async def send(self, data: str | bytes) -> None:
        """Send bytes or a string over WebSocket as text or binary frame."""
        if not self.ws:
            raise RuntimeError("WebSocket not connected")
        await self.ws.send(data)

    async def receive(self) -> AsyncIterator[bytes | str]:
        """Receive messages from WebSocket."""
        if not self.ws:
            raise RuntimeError("WebSocket not connected")
        
        try:
            async for message in self.ws:
                yield message
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except asyncio.CancelledError:
            logger.info("WebSocket receive cancelled")
            raise

    async def close(self) -> None:
        """Close WebSocket connection."""
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=1.0)
            except asyncio.TimeoutError:
                logger.warning("WebSocket close timed out")
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self.ws = None

    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self.ws is not None and self.ws.open


class TcpTransport(Transport[bytes]):
    """TCP socket transport implementation."""

    def __init__(self):
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self, url: str) -> None:
        """
        Connect to TCP endpoint.
        
        Args:
            url: Format "host:port"
        """
        host, port = url.split(':')
        port = int(port)
        
        logger.info(f"Connecting to TCP: {host}:{port}")
        self.reader, self.writer = await asyncio.open_connection(host, port)
        logger.info(f"TCP connected: {host}:{port}")

    async def send(self, data: bytes) -> None:
        """Send bytes over TCP (is_text parameter ignored for TCP)."""
        if not self.writer:
            raise RuntimeError("TCP not connected")

        self.writer.write(data)
        await self.writer.drain()

    async def receive(self) -> AsyncIterator[bytes]:
        """
        Receive bytes from TCP.
        
        Note: This yields raw bytes as they arrive. The protocol handler
        is responsible for framing/message boundaries.
        """
        if not self.reader:
            raise RuntimeError("TCP not connected")
        
        try:
            while True:
                data = await self.reader.read(4096)
                if not data:
                    logger.info("TCP connection closed by remote")
                    break
                yield data
        except asyncio.CancelledError:
            logger.info("TCP receive cancelled")
            raise
        except Exception as e:
            logger.error(f"Error receiving from TCP: {e}")
            raise

    async def close(self) -> None:
        """Close TCP connection."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                logger.warning(f"Error closing TCP: {e}")
            finally:
                self.reader = None
                self.writer = None

    def is_connected(self) -> bool:
        """Check if TCP is connected."""
        return self.writer is not None and not self.writer.is_closing()


def create_transport(transport_type: TransportType) -> Transport:
    """
    Factory function to create transport instances.

    Args:
        transport_type: Type of transport

    Returns:
        Transport instance
    """
    if transport_type == TransportType.WEBSOCKET:
        return WebSocketTransport()
    elif transport_type == TransportType.TCP:
        return TcpTransport()
    else:
        raise ValueError(f"Unsupported transport type: {transport_type}")


class TransportServer(ABC):
    """Base class for server-side transport implementations."""

    @abstractmethod
    async def start(self, host: str, port: int, handler: Callable[[object], Awaitable[None]]) -> None:
        """
        Start the server and accept connections.

        Args:
            host: Host to bind to
            port: Port to bind to
            handler: Async function to handle each client connection
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the server."""
        pass

    @abstractmethod
    async def broadcast(self, data: bytes | str) -> None:
        """
        Broadcast data to all connected clients.

        Args:
            data: Data to broadcast
        """
        pass

    @abstractmethod
    async def send_to_client(self, client: object, data: bytes | str) -> None:
        """
        Send data to a specific client.

        Args:
            client: Client connection object
            data: Data to send
        """
        pass

    @abstractmethod
    async def close_client(self, client: object) -> None:
        """
        Close a specific client connection.

        Args:
            client: Client connection object
        """
        pass


class WebSocketServer(TransportServer):
    """WebSocket server implementation."""

    def __init__(self):
        self.server = None
        self.clients: set[WebSocketServerProtocol] = set()

    async def start(self, host: str, port: int, handler: Callable[[WebSocketServerProtocol], Awaitable[None]]) -> None:
        """Start WebSocket server."""
        async def connection_handler(websocket: WebSocketServerProtocol):
            self.clients.add(websocket)
            logger.info(f"Client connected: {websocket.remote_address}")
            try:
                await handler(websocket)
            finally:
                self.clients.discard(websocket)
                logger.info(f"Client disconnected: {websocket.remote_address}")

        logger.info(f"Starting WebSocket server on {host}:{port}")
        self.server = await serve(connection_handler, host, port)
        logger.info(f"WebSocket server started on ws://{host}:{port}")

    async def stop(self) -> None:
        """Stop WebSocket server."""
        if self.server:
            logger.info("Stopping WebSocket server...")
            self.server.close()
            await self.server.wait_closed()

            if self.clients:
                close_tasks = [client.close() for client in self.clients]
                await asyncio.gather(*close_tasks, return_exceptions=True)
                self.clients.clear()

            logger.info("WebSocket server stopped")

    async def broadcast(self, data: bytes | str) -> None:
        """Broadcast data to all connected clients."""
        if not self.clients:
            return

        tasks = [client.send(data) for client in self.clients]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def send_to_client(self, client: WebSocketServerProtocol, data: bytes | str) -> None:
        """Send data to a specific client."""
        await client.send(data)

    async def close_client(self, client: WebSocketServerProtocol) -> None:
        """Close a specific client connection."""
        try:
            await client.close()
        except Exception as e:
            logger.warning(f"Error closing client: {e}")


class TcpServer(TransportServer):
    """TCP server implementation."""

    def __init__(self):
        self.server: asyncio.Server | None = None
        self.clients: dict[tuple[str, int], asyncio.StreamWriter] = {}

    async def start(self, host: str, port: int, handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]) -> None:
        """Start TCP server."""
        async def connection_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            addr = writer.get_extra_info('peername')
            self.clients[addr] = writer
            logger.info(f"Client connected: {addr}")
            try:
                await handler(reader, writer)
            finally:
                if addr in self.clients:
                    del self.clients[addr]
                logger.info(f"Client disconnected: {addr}")

        logger.info(f"Starting TCP server on {host}:{port}")
        self.server = await asyncio.start_server(connection_handler, host, port)
        logger.info(f"TCP server started on {host}:{port}")

    async def stop(self) -> None:
        """Stop TCP server."""
        if self.server:
            logger.info("Stopping TCP server...")
            self.server.close()
            await self.server.wait_closed()

            for writer in self.clients.values():
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logger.warning(f"Error closing client connection: {e}")

            self.clients.clear()
            logger.info("TCP server stopped")

    async def broadcast(self, data: bytes) -> None:
        """Broadcast data to all connected clients."""
        if not self.clients:
            return

        tasks = []
        for writer in self.clients.values():
            writer.write(data)
            tasks.append(writer.drain())

        await asyncio.gather(*tasks, return_exceptions=True)

    async def send_to_client(self, client: asyncio.StreamWriter, data: bytes) -> None:
        """Send data to a specific client."""
        client.write(data)
        await client.drain()

    async def close_client(self, client: asyncio.StreamWriter) -> None:
        """Close a specific client connection."""
        try:
            client.close()
            await client.wait_closed()
        except Exception as e:
            logger.warning(f"Error closing client: {e}")


def create_transport_server(transport_type: TransportType) -> TransportServer:
    """
    Factory function to create server transport instances.

    Args:
        transport_type: Type of transport

    Returns:
        TransportServer instance
    """
    if transport_type == TransportType.WEBSOCKET:
        return WebSocketServer()
    elif transport_type == TransportType.TCP:
        return TcpServer()
    else:
        raise ValueError(f"Unsupported transport type: {transport_type}")

