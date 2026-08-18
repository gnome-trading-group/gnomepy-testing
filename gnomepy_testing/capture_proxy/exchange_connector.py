"""
Exchange connectors - Generic protocol and transport support.

Supports any combination of transport (WebSocket, TCP, etc.)
and protocol (JSON, FIX, binary, etc.) through composition.
"""
import asyncio
import base64
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Callable, Any

import boto3
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

POLYMARKET_PING_INTERVAL_SECONDS = 10
KALSHI_WS_PATH = "/trade-api/ws/v2"

from gnomepy_testing.listing_resolver import ListingInfo
from gnomepy_testing.network import (
    Transport,
    TransportType,
    create_transport,
    ProtocolHandler,
    ProtocolType,
    create_protocol_handler
)


logger = logging.getLogger(__name__)


class ExchangeConnector(ABC):
    """
    Base class for exchange connectors.
    
    Each connector specifies:
    - Which transport to use (WebSocket, TCP, etc.)
    - Which protocol to use (JSON, FIX, binary, etc.)
    - Connection URL/endpoint
    - Subscription messages (if any)
    """

    def __init__(self, listing_info: ListingInfo, on_message: Callable[[bytes], None]):
        """
        Initialize the exchange connector.

        Args:
            listing_info: The listing information from the resolver
            on_message: Callback function to handle received messages (as bytes)
        """
        self.listing_info = listing_info
        self.on_message = on_message
        self._running = False
        
        self.transport: Transport = create_transport(self.get_transport_type())
        self.protocol: ProtocolHandler = create_protocol_handler(self.get_protocol_type())

    @abstractmethod
    def get_transport_type(self) -> TransportType:
        """
        Get the transport type for this exchange.

        Returns:
            Transport type enum
        """
        pass

    @abstractmethod
    def get_protocol_type(self) -> ProtocolType:
        """
        Get the protocol type for this exchange.

        Returns:
            Protocol type enum
        """
        pass

    @abstractmethod
    def get_connection_url(self) -> str:
        """
        Get the connection URL/endpoint for this exchange.
        
        Returns:
            URL or connection string (format depends on transport)
        """
        pass

    @abstractmethod
    def get_subscribe_messages(self) -> list[Any] | None:
        """
        Get the subscription messages for this exchange.

        Returns:
            List of subscription messages (format depends on protocol), or None
        """
        pass

    def get_connection_headers(self) -> dict[str, str] | None:
        """
        Get extra HTTP headers to send during the WebSocket handshake.

        Returns:
            Dict of header name → value, or None
        """
        return None

    async def connect(self):
        """Connect to the exchange."""
        url = self.get_connection_url()
        logger.info(f"Connecting to {self.listing_info.exchange_name} at {url}")

        await self.transport.connect(url, extra_headers=self.get_connection_headers())
        self._running = True

        subscribe_msgs = self.get_subscribe_messages()
        if subscribe_msgs:
            for msg in subscribe_msgs:
                encoded = self.protocol.encode_message(msg)
                logger.info(f"Sending subscription message: {msg}")
                await self.transport.send(encoded)
            logger.info(f"Subscribed to {self.listing_info} ({len(subscribe_msgs)} message(s))")
        else:
            logger.info(f"Connected to {self.listing_info} (no subscription needed)")

        await self._receive_loop()

    async def _receive_loop(self):
        """Receive messages from the exchange and forward them."""
        try:
            async for raw_data in self.transport.receive():
                if not self._running:
                    break
                
                try:
                    decoded_data = self.protocol.decode_message(raw_data)
                    self.on_message(decoded_data)
                except Exception as e:
                    logger.error(f"Error decoding message: {e}")

        except asyncio.CancelledError:
            logger.info(f"Receive loop cancelled for {self.listing_info.exchange_name}")
        except Exception as e:
            logger.error(f"Error in receive loop: {e}")
        finally:
            self._running = False

    async def disconnect(self):
        """Disconnect from the exchange."""
        self._running = False
        await self.transport.close()
        logger.info(f"Disconnected from {self.listing_info.exchange_name}")


class HyperliquidConnector(ExchangeConnector):
    """Hyperliquid WebSocket connector (JSON over WebSocket)."""

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def get_connection_url(self) -> str:
        return "wss://api.hyperliquid.xyz/ws"

    def get_subscribe_messages(self) -> list[dict] | None:
        return [
            {
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book",
                    "coin": self.listing_info.exchange_security_symbol
                }
            },
            {
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": self.listing_info.exchange_security_symbol
                }
            },
        ]


class LighterConnector(ExchangeConnector):
    """Lighter WebSocket connector (JSON over WebSocket)."""
    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def get_connection_url(self) -> str:
        return "wss://mainnet.zklighter.elliot.ai/stream"

    def get_subscribe_messages(self) -> list[dict] | None:
        # { "type": "subscribe", "channel": "order_book/{MARKET_INDEX}"}
        # { "type": "subscribe", "channel": "trade/{MARKET_INDEX}" }
        return [
            {
                "type": "subscribe",
                "channel": f"order_book/{self.listing_info.exchange_security_id}"
            },
            {
                "type": "subscribe",
                "channel": f"trade/{self.listing_info.exchange_security_id}"
            },
        ]


class BinanceConnector(ExchangeConnector):
    """Binance WebSocket connector (JSON over WebSocket)."""

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def get_connection_url(self) -> str:
        return "wss://stream.binance.com:9443/ws"

    def get_subscribe_messages(self) -> list[dict] | None:
        symbol = self.listing_info.exchange_security_symbol.lower()
        return [
            {
                "method": "SUBSCRIBE",
                "params": [f"{symbol}@depth@100ms", f"{symbol}@trade"],
                "id": 1,
            }
        ]


class PolymarketConnector(ExchangeConnector):
    """Polymarket WebSocket connector (JSON over WebSocket) with PING/PONG keepalive."""

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def get_connection_url(self) -> str:
        return "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def get_subscribe_messages(self) -> list[dict] | None:
        exchange_security_id = self.listing_info.exchange_security_id
        token_id = exchange_security_id.split(":", 1)[1] if ":" in exchange_security_id else exchange_security_id
        return [
            {
                "type": "market",
                "assets_ids": [token_id],
                "custom_feature_enabled": True,
            }
        ]

    async def connect(self):
        url = self.get_connection_url()
        logger.info(f"Connecting to {self.listing_info.exchange_name} at {url}")
        await self.transport.connect(url)
        self._running = True

        subscribe_msgs = self.get_subscribe_messages()
        if subscribe_msgs:
            for msg in subscribe_msgs:
                encoded = self.protocol.encode_message(msg)
                logger.info(f"Sending subscription message: {msg}")
                await self.transport.send(encoded)
            logger.info(f"Subscribed to {self.listing_info} ({len(subscribe_msgs)} message(s))")

        ping_task = asyncio.ensure_future(self._ping_loop())
        try:
            await self._receive_loop()
        finally:
            ping_task.cancel()

    async def _ping_loop(self):
        while self._running:
            await asyncio.sleep(POLYMARKET_PING_INTERVAL_SECONDS)
            if self._running and self.transport.is_connected():
                await self.transport.send("PING")

    async def _receive_loop(self):
        try:
            async for raw_data in self.transport.receive():
                if not self._running:
                    break
                if raw_data == "PONG":
                    continue
                try:
                    decoded_data = self.protocol.decode_message(raw_data)
                    self.on_message(decoded_data)
                except Exception as e:
                    logger.error(f"Error decoding message: {e}")
        except asyncio.CancelledError:
            logger.info(f"Receive loop cancelled for {self.listing_info.exchange_name}")
        except Exception as e:
            logger.error(f"Error in receive loop: {e}")
        finally:
            self._running = False


class KalshiConnector(ExchangeConnector):
    """Kalshi WebSocket connector with RSA-PSS authentication."""

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def get_connection_url(self) -> str:
        return "wss://external-api-ws.kalshi.com" + KALSHI_WS_PATH

    def _load_credentials(self):
        api_key = os.environ.get("KALSHI_API_KEY")
        key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        if api_key and key_path:
            with open(key_path, "rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            return api_key, private_key

        try:
            secret = boto3.client("secretsmanager").get_secret_value(
                SecretId="gnome/exchange-credentials/kalshi"
            )
            data = json.loads(secret["SecretString"])
            private_key = serialization.load_pem_private_key(
                data["privateKey"].encode(), password=None
            )
            return data["apiKey"], private_key
        except Exception as e:
            raise RuntimeError(
                "Kalshi credentials not found. Set KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH "
                "env vars, or configure AWS credentials for Secrets Manager access."
            ) from e

    def get_connection_headers(self) -> dict[str, str]:
        api_key, private_key = self._load_credentials()

        timestamp_ms = str(int(time.time() * 1000))
        payload = f"{timestamp_ms}GET{KALSHI_WS_PATH}".encode()
        signature = private_key.sign(
            payload,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        }

    def get_subscribe_messages(self) -> list[dict] | None:
        exchange_security_id = self.listing_info.exchange_security_id or ""
        ticker = exchange_security_id.split(":", 1)[0] if ":" in exchange_security_id else exchange_security_id
        return [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta", "trade"],
                    "market_tickers": [ticker],
                },
            }
        ]


class ExampleFixExchangeConnector(ExchangeConnector):
    """
    Example connector for an exchange using FIX protocol over TCP.

    This is a template showing how to implement a FIX-based exchange.
    """

    def get_transport_type(self) -> TransportType:
        return TransportType.TCP

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.FIX

    def get_connection_url(self) -> str:
        return "fix.example-exchange.com:9876"

    def get_subscribe_messages(self) -> list[dict] | None:
        return [{
            "35": "V",
            "262": "MARKET_DATA_REQUEST",
            "263": "1",
            "264": "0",
            "265": "1",
            "146": "1",
            "55": self.listing_info.exchange_security_symbol,
            "267": "2",
            "269": "0",
        }]


def create_exchange_connector(
    listing_info: ListingInfo,
    on_message: Callable[[bytes], None]
) -> ExchangeConnector:
    """
    Factory function to create the appropriate exchange connector.

    Args:
        listing_info: The listing information from the resolver
        on_message: Callback for received messages (as bytes)

    Returns:
        ExchangeConnector instance for the specified exchange
    """
    exchange_name = listing_info.exchange_name.upper()

    if exchange_name == "HYPERLIQUID":
        return HyperliquidConnector(listing_info, on_message)
    elif exchange_name == "LIGHTER":
        return LighterConnector(listing_info, on_message)
    elif exchange_name == "BINANCE":
        return BinanceConnector(listing_info, on_message)
    elif exchange_name == "POLYMARKET":
        return PolymarketConnector(listing_info, on_message)
    elif exchange_name == "KALSHI":
        return KalshiConnector(listing_info, on_message)
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

