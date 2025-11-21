"""
Exchange-specific parsers for converting exchange messages to gnomepy schema objects.

Each parser maintains state between messages as needed for the specific exchange.
Each parser also knows which transport and protocol it needs.
"""
import logging
import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Callable, Any

from gnomepy import MBP1, MBP10, BidAskPair, SchemaBase, FIXED_PRICE_SCALE, FIXED_SIZE_SCALE
from gnomepy_testing.listing_resolver import ListingInfo
from gnomepy_testing.network import TransportType, ProtocolType


logger = logging.getLogger(__name__)


class ExchangeParser(ABC):
    """Base class for exchange-specific parsers."""

    def __init__(self, listing_info: ListingInfo):
        """
        Initialize the parser.

        Args:
            listing_info: Listing information from the proxy
        """
        self.listing_info = listing_info

    @abstractmethod
    def get_transport_type(self) -> TransportType:
        """
        Get the transport type required for this exchange.

        Returns:
            Transport type enum
        """
        pass

    @abstractmethod
    def get_protocol_type(self) -> ProtocolType:
        """
        Get the protocol type required for this exchange.

        Returns:
            Protocol type enum
        """
        pass

    @abstractmethod
    def parse(self, data: Any, write_message: Callable[[SchemaBase], None]) -> None:
        """
        Parse an exchange message into a gnomepy schema object.

        Args:
            data: Decoded data
            write_message: Callback to write the parsed schema object
        """
        pass

    def reset(self):
        """Reset parser state. Override if needed."""
        pass


class HyperliquidParser(ExchangeParser):
    """Parser for Hyperliquid L2 book messages."""

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.last_trade_price = None
        self.last_trade_size = None
        self.levels = [BidAskPair(0, 0, 0, 0, 0, 0)] * 10

    def get_transport_type(self) -> TransportType:
        """Hyperliquid uses WebSocket."""
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        """Hyperliquid uses JSON over WebSocket."""
        return ProtocolType.JSON_WS

    def parse(self, data: dict, write_message: Callable[[MBP1 | MBP10], None]) -> None:
        """Parse Hyperliquid L2 book message into MBP10."""
        if 'channel' not in data:
            return

        if data['channel'] == 'l2Book':
            self._parse_l2_book(data['data'], write_message)
        elif data['channel'] == 'trades':
            self._parse_trade(data['data'], write_message)

    def _update_levels(self, data: list[dict]) -> int | None:
        """Update levels with new data and return the lowest depth level of an update."""
        lowest_level = None
        for i in range(10):
            new_level = BidAskPair(
                bid_px=int(Decimal(data[0][i]['px']) * Decimal(FIXED_PRICE_SCALE)),
                ask_px=int(Decimal(data[1][i]['px']) * Decimal(FIXED_PRICE_SCALE)),
                bid_sz=int(Decimal(data[0][i]['sz']) * Decimal(FIXED_SIZE_SCALE)),
                ask_sz=int(Decimal(data[1][i]['sz']) * Decimal(FIXED_SIZE_SCALE)),
                bid_ct=data[0][i]['n'],
                ask_ct=data[1][i]['n'],
            )
            if new_level != self.levels[i]:
                lowest_level = i if lowest_level is None else lowest_level
                self.levels[i] = new_level
        return lowest_level

    def _parse_l2_book(self, data: dict, write_message: Callable[[MBP1 | MBP10], None]):
        """Parse Hyperliquid L2 book message into MBP10."""
        depth = self._update_levels(data['levels'])
        _time = int(Decimal(data['time']) * Decimal(1_000_000))

        output = MBP10(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=_time,
            sequence=_time,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=self.last_trade_price,
            size=self.last_trade_size,
            action="Modify",
            side="None",
            flags=["marketByPrice"],
            depth=depth,
            levels=self.levels,
        )
        write_message(output)

    def _parse_trade(self, data: list[dict], write_message: Callable[[MBP1 | MBP10], None]):
        """Parse Hyperliquid trade message into MBP10."""
        for trade in data:
            self.last_trade_price = int(Decimal(trade['px']) * Decimal(FIXED_PRICE_SCALE))
            self.last_trade_size = int(Decimal(trade['sz']) * Decimal(FIXED_SIZE_SCALE))
            _time = int(Decimal(trade['time']) * Decimal(1_000_000))

            output = MBP10(
                exchange_id=self.listing_info.exchange_id,
                security_id=self.listing_info.security_id,
                timestamp_event=_time,
                sequence=_time,
                timestamp_sent=None,
                timestamp_recv=time.time_ns(),
                price=self.last_trade_price,
                size=self.last_trade_size,
                action="Trade",
                side="Bid" if trade['side'] == 'B' else "Ask",
                flags=["marketByPrice"],
                depth=None,
                levels=self.levels,
            )
            write_message(output)


class BinanceParser(ExchangeParser):
    """Parser for Binance depth messages."""

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.last_update_id = 0

    def get_transport_type(self) -> TransportType:
        """Binance uses WebSocket."""
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        """Binance uses JSON over WebSocket."""
        return ProtocolType.JSON_WS

    def parse(self, data: bytes, write_message: Callable[[MBP1 | MBP10], None]) -> None:
        """Parse Binance depth message into MBP10."""
        try:
            # Binance depth10 format:
            # {
            #   "lastUpdateId": 160,
            #   "bids": [["0.0024", "10"]],
            #   "asks": [["0.0026", "100"]]
            # }
            
            timestamp_ns = int(time.time() * 1_000_000_000)

            update_id = data.get('lastUpdateId', 0)
            if update_id <= self.last_update_id:
                logger.warning(f"Out of order update: {update_id} <= {self.last_update_id}")
            self.last_update_id = update_id

            levels = []
            for i in range(10):
                bid_px = 0
                bid_sz = 0
                ask_px = 0
                ask_sz = 0

                if i < len(data.get('bids', [])):
                    bid = data['bids'][i]
                    bid_px = int(float(bid[0]) * 1_000_000_000)
                    bid_sz = int(float(bid[1]) * 1_000_000)

                if i < len(data.get('asks', [])):
                    ask = data['asks'][i]
                    ask_px = int(float(ask[0]) * 1_000_000_000)
                    ask_sz = int(float(ask[1]) * 1_000_000)
                
                levels.append(BidAskPair(
                    bid_px=bid_px,
                    ask_px=ask_px,
                    bid_sz=bid_sz,
                    ask_sz=ask_sz,
                    bid_ct=1 if bid_px > 0 else 0,
                    ask_ct=1 if ask_px > 0 else 0
                ))
            
            return MBP10(
                exchange_id=self.listing_info.exchange_id,
                security_id=self.listing_info.security_id,
                timestamp_event=timestamp_ns,
                timestamp_sent=None,
                timestamp_recv=timestamp_ns,
                price=None,
                size=None,
                action="None",
                side="None",
                flags=[],
                sequence=update_id,
                depth=10,
                levels=levels
            )
            
        except Exception as e:
            logger.error(f"Error parsing Binance message: {e}")
            return None
    
    def reset(self):
        """Reset parser state."""
        self.last_update_id = 0


class CoinbaseParser(ExchangeParser):
    """Parser for Coinbase level2 messages."""

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.sequence = 0

    def get_transport_type(self) -> TransportType:
        """Coinbase uses WebSocket."""
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        """Coinbase uses JSON over WebSocket."""
        return ProtocolType.JSON_WS

    def parse(self, data: bytes, write_message: Callable[[MBP1 | MBP10], None]) -> None:
        """Parse Coinbase level2 message into MBP10."""
        # TODO: Implement Coinbase parsing based on actual message format
        # Coinbase sends:
        # 1. "snapshot" messages with full book
        # 2. "l2update" messages with changes
        logger.debug(f"Coinbase message: {data}")
        return None
    
    def reset(self):
        """Reset parser state."""
        self.bids.clear()
        self.asks.clear()
        self.sequence = 0


def create_parser(listing_info: ListingInfo) -> ExchangeParser:
    """
    Factory function to create the appropriate parser for an exchange.
    
    Args:
        listing_info: Listing information from the proxy
        
    Returns:
        ExchangeParser instance for the exchange
    """
    exchange_name = listing_info.exchange_name.upper()
    
    if exchange_name == "HYPERLIQUID":
        return HyperliquidParser(listing_info)
    elif exchange_name == "BINANCE":
        return BinanceParser(listing_info)
    elif exchange_name == "COINBASE":
        return CoinbaseParser(listing_info)
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

