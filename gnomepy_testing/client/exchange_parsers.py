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
from sortedcontainers import SortedList

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


class LighterParser(ExchangeParser):
    """Parser for Lighter L2 book messages."""

    MAX_LEVELS = 10

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.last_trade_price = None
        self.last_trade_size = None
        self.last_sequence_number = None
        self.bids = SortedList(key=lambda x: -x['price'])
        self.asks = SortedList(key=lambda x: x['price'])

    def get_transport_type(self) -> TransportType:
        """Lighter uses WebSocket."""
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        """Lighter uses JSON over WebSocket."""
        return ProtocolType.JSON_WS

    def parse(self, data: dict, write_message: Callable[[MBP1 | MBP10], None]) -> None:
        """Parse Lighter L2 book message into MBP10."""
        message_type = data['type']
        if message_type == "subscribed/order_book":
            self.handle_subscribed_order_book(data, write_message)
        elif message_type == "update/order_book":
            self.handle_update_order_book(data, write_message)
        elif message_type in ("subscribed/trade", "update/trade"):
            self.handle_trades(data, write_message)
        # elif message_type == "ping":
        #     logging.info("ping")
        #     # Respond to ping with pong
        #     ws.send(json.dumps({"type": "pong"}))
        return None

    def handle_subscribed_order_book(self, data: dict, write_message: Callable[[MBP1 | MBP10], None]):
        """Handle subscribed/order_book message."""
        self.last_sequence_number = data['offset']
        for ask in data['order_book']['asks']:
            self.asks.add(self._convert_to_fixed_length(ask))
        for bid in data['order_book']['bids']:
            self.bids.add(self._convert_to_fixed_length(bid))

        _time = int(Decimal(data['timestamp']) * Decimal(1_000_000))
        output = MBP10(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=_time,
            sequence=self.last_sequence_number,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=self.last_trade_price,
            size=self.last_trade_size,
            action="Modify",
            side="None",
            flags=["marketByPrice"],
            depth=0,
            levels=self.get_levels(),
        )
        write_message(output)

    def _convert_to_fixed_length(self, level: dict):
        """Convert price and size to fixed-length format."""
        return {
            "price": int(Decimal(level['price']) * Decimal(FIXED_PRICE_SCALE)),
            "size": int(Decimal(level['size']) * Decimal(FIXED_SIZE_SCALE)),
        }

    def get_levels(self) -> list[BidAskPair]:
        """Get levels from sorted lists."""
        levels = []
        for i in range(self.MAX_LEVELS):
            if i < len(self.bids) and i < len(self.asks):
                levels.append(BidAskPair(
                    bid_px=self.bids[i]['price'],
                    ask_px=self.asks[i]['price'],
                    bid_sz=self.bids[i]['size'],
                    ask_sz=self.asks[i]['size'],
                    bid_ct=1,
                    ask_ct=1,
                ))
            else:
                levels.append(BidAskPair(None, None, None, None, None, None))
        return levels

    def handle_update_order_book(self, message: dict, write_message: Callable[[MBP1 | MBP10], None]):
        """Handle update/order_book message."""
        self.last_sequence_number = message['offset']

        _time = int(Decimal(message['timestamp']) * Decimal(1_000_000))

        ask_depth = self.update_orders(message['order_book']['asks'] or [], self.asks)
        bid_depth = self.update_orders(message['order_book']['bids'] or [], self.bids)

        depth = min(ask_depth, bid_depth)
        if depth >= self.MAX_LEVELS:
            return

        output = MBP10(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=_time,
            sequence=self.last_sequence_number,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=self.last_trade_price,
            size=self.last_trade_size,
            action="Modify",
            side="None",
            flags=["marketByPrice"],
            depth=depth,
            levels=self.get_levels(),
        )
        write_message(output)

    def handle_trades(self, message: dict, write_message: Callable[[MBP1 | MBP10], None]):
        """Handle trade message."""
        for trade in message['trades']:
            self.last_trade_price = int(Decimal(trade['price']) * Decimal(FIXED_PRICE_SCALE))
            self.last_trade_size = int(Decimal(trade['size']) * Decimal(FIXED_SIZE_SCALE))
            _time = int(Decimal(trade['timestamp']) * Decimal(1_000_000))
            side = "Ask" if not trade['is_maker_ask'] else "Bid"

            output = MBP10(
                exchange_id=self.listing_info.exchange_id,
                security_id=self.listing_info.security_id,
                timestamp_event=_time,
                sequence=self.last_sequence_number,
                timestamp_sent=None,
                timestamp_recv=time.time_ns(),
                price=self.last_trade_price,
                size=self.last_trade_size,
                action="Trade",
                side=side,
                flags=["marketByPrice"],
                depth=None,
                levels=self.get_levels(),
            )
            write_message(output)

    def update_orders(self, new_orders: list[dict], existing_orders: SortedList) -> int | None:
        depth = 99
        for new_order in new_orders:
            new_order = self._convert_to_fixed_length(new_order)
            is_new_order = True
            for i in range(len(existing_orders)):
                existing_order = existing_orders[i]
                if new_order["price"] == existing_order["price"]:
                    is_new_order = False
                    existing_order["size"] = new_order["size"]
                    depth = min(depth, i)
                    if new_order["size"] == Decimal(0):
                        existing_orders.remove(existing_order)
                    break
            if is_new_order and new_order["size"] > 0:
                existing_orders.add(new_order)
                depth = min(depth, existing_orders.index(new_order))

        return depth


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
    elif exchange_name == "LIGHTER":
        return LighterParser(listing_info)
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

