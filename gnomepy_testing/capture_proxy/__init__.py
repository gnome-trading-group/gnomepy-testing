"""
Capture Proxy - Generic protocol proxy for capturing exchange data.

Supports any combination of transport (WebSocket, TCP, etc.) and
protocol (JSON, FIX, binary, etc.) through a layered architecture.

The proxy connects to exchange APIs and forwards all messages
to subscribed clients (Python, Java, etc.). This ensures all clients
receive identical data for accurate comparison testing.
"""

from .server import CaptureProxyServer
from .exchange_connector import (
    ExchangeConnector,
    HyperliquidConnector,
    ExampleFixExchangeConnector,
    create_exchange_connector
)
from gnomepy_testing.network import (
    Transport,
    TransportType,
    WebSocketTransport,
    TcpTransport,
    create_transport,
    ProtocolHandler,
    ProtocolType,
    JsonWebSocketProtocol,
    FixProtocol,
    BinaryProtocol,
    create_protocol_handler
)

__all__ = [
    'CaptureProxyServer',
    'ExchangeConnector',
    'HyperliquidConnector',
    'ExampleFixExchangeConnector',
    'create_exchange_connector',
    'Transport',
    'TransportType',
    'WebSocketTransport',
    'TcpTransport',
    'create_transport',
    'ProtocolHandler',
    'ProtocolType',
    'JsonWebSocketProtocol',
    'FixProtocol',
    'BinaryProtocol',
    'create_protocol_handler',
]

