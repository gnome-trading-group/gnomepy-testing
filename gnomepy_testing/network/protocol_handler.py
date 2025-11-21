"""
Protocol handlers for different exchange protocols.

Protocol handlers are responsible for:
- Encoding outgoing messages (e.g., subscription requests)
- Decoding incoming messages into a common format
- Protocol-specific framing and message boundaries
"""
import json
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import TypeVar, Generic

I = TypeVar('I')
O = TypeVar('O')

logger = logging.getLogger(__name__)


class ProtocolType(Enum):
    """Supported protocol types."""
    JSON_WS = "json_ws"
    FIX = "fix"
    BINARY = "binary"


class ProtocolHandler(ABC, Generic[I, O]):
    """Base class for protocol handlers."""

    @abstractmethod
    def encode_message(self, message: I) -> O:
        """
        Encode a message for sending to the exchange.

        Args:
            message: Message to encode (format depends on protocol)

        Returns:
            Encoded bytes ready to send
        """
        pass

    @abstractmethod
    def decode_message(self, data: O) -> I:
        """
        Decode a message received from the exchange.

        Args:
            data: Raw bytes received from transport

        Returns:
            Decoded message bytes (in a normalized format for forwarding)
        """
        pass


class JsonWebSocketProtocol(ProtocolHandler[dict | str, str]):
    """Protocol handler for JSON over WebSocket."""

    def encode_message(self, message: dict | str) -> str:
        """Encode a dict as JSON string."""
        if isinstance(message, dict):
            return json.dumps(message)
        return message

    def decode_message(self, data: str) -> dict:
        """
        Decode and validate JSON, then return as bytes.

        For WebSocket JSON protocols, we just validate the JSON
        and return the original bytes for forwarding.
        """
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON received: {e}")
            raise

class FixProtocol(ProtocolHandler[bytes, dict]):
    """Protocol handler for FIX protocol."""

    SOH = b'\x01'

    def encode_message(self, message: dict) -> bytes:
        """
        Encode a FIX message.
        
        Args:
            message: Dict with FIX tag->value pairs
            
        Returns:
            FIX-formatted message bytes
        """
        fields = []
        for tag, value in message.items():
            fields.append(f"{tag}={value}".encode('ascii'))
        
        body = self.SOH.join(fields)
        
        checksum = sum(body) % 256
        checksum_field = f"10={checksum:03d}".encode('ascii')
        
        return body + self.SOH + checksum_field + self.SOH

    def decode_message(self, data: bytes) -> dict:
        """
        Decode and validate FIX message.
        
        Returns the original bytes after validation.
        """
        if not data.endswith(self.SOH):
            logger.warning("FIX message doesn't end with SOH")
        
        return data


class BinaryProtocol(ProtocolHandler[bytes, bytes]):
    """Protocol handler for raw binary protocols (e.g., SBE, custom binary)."""

    def encode_message(self, message: bytes) -> bytes:
        """For binary protocols, message is already bytes."""
        return message

    def decode_message(self, data: bytes) -> bytes:
        """For binary protocols, just pass through the bytes."""
        return data


def create_protocol_handler(protocol_type: ProtocolType) -> ProtocolHandler:
    """
    Factory function to create protocol handlers.

    Args:
        protocol_type: Type of protocol

    Returns:
        ProtocolHandler instance
    """
    if protocol_type == ProtocolType.JSON_WS:
        return JsonWebSocketProtocol()
    elif protocol_type == ProtocolType.FIX:
        return FixProtocol()
    elif protocol_type == ProtocolType.BINARY:
        return BinaryProtocol()
    else:
        raise ValueError(f"Unsupported protocol type: {protocol_type}")

