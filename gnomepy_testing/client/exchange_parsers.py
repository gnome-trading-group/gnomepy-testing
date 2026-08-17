"""
Exchange-specific parsers for converting exchange messages to gnomepy schema objects.

Each parser maintains state between messages as needed for the specific exchange.
Each parser also knows which transport and protocol it needs.
"""
import json
import logging
import time
import urllib.request
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Callable, Any

from gnomepy.java.schemas import Mbp10Schema, Schema
from sortedcontainers import SortedList

from gnomepy_testing.listing_resolver import ListingInfo
from gnomepy_testing.network import TransportType, ProtocolType

_PRICE_SCALE = 1_000_000_000
_SIZE_SCALE = 1_000_000


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
    def parse(self, data: Any, write_message: Callable[[Schema], None]) -> None:
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
        self.initial_trades_batch_received = False
        self.levels = [{'bid_price': 0, 'ask_price': 0, 'bid_size': 0, 'ask_size': 0, 'bid_count': 0, 'ask_count': 0}] * 10

    def get_transport_type(self) -> TransportType:
        """Hyperliquid uses WebSocket."""
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        """Hyperliquid uses JSON over WebSocket."""
        return ProtocolType.JSON_WS

    def parse(self, data: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
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
            new_level = {
                'bid_price': int(Decimal(data[0][i]['px']) * Decimal(_PRICE_SCALE)),
                'ask_price': int(Decimal(data[1][i]['px']) * Decimal(_PRICE_SCALE)),
                'bid_size': int(Decimal(data[0][i]['sz']) * Decimal(_SIZE_SCALE)),
                'ask_size': int(Decimal(data[1][i]['sz']) * Decimal(_SIZE_SCALE)),
                'bid_count': data[0][i]['n'],
                'ask_count': data[1][i]['n'],
            }
            if new_level != self.levels[i]:
                lowest_level = i if lowest_level is None else lowest_level
                self.levels[i] = new_level
        return lowest_level

    def _level_kwargs(self) -> dict:
        kwargs = {}
        for i, lvl in enumerate(self.levels):
            kwargs[f'bid_price_{i}'] = lvl['bid_price']
            kwargs[f'ask_price_{i}'] = lvl['ask_price']
            kwargs[f'bid_size_{i}'] = lvl['bid_size']
            kwargs[f'ask_size_{i}'] = lvl['ask_size']
            kwargs[f'bid_count_{i}'] = lvl['bid_count']
            kwargs[f'ask_count_{i}'] = lvl['ask_count']
        return kwargs

    def _parse_l2_book(self, data: dict, write_message: Callable[[Mbp10Schema], None]):
        """Parse Hyperliquid L2 book message into MBP10."""
        depth = self._update_levels(data['levels'])
        _time = int(Decimal(data['time']) * Decimal(1_000_000))

        write_message(Mbp10Schema(
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
            **self._level_kwargs(),
        ))

    def _parse_trade(self, data: list[dict], write_message: Callable[[Mbp10Schema], None]):
        """Parse Hyperliquid trade message into MBP10."""
        emit = self.initial_trades_batch_received
        self.initial_trades_batch_received = True
        for trade in data:
            self.last_trade_price = int(Decimal(trade['px']) * Decimal(_PRICE_SCALE))
            self.last_trade_size = int(Decimal(trade['sz']) * Decimal(_SIZE_SCALE))
            if not emit:
                continue
            _time = int(Decimal(trade['time']) * Decimal(1_000_000))
            write_message(Mbp10Schema(
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
                **self._level_kwargs(),
            ))


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

    def parse(self, data: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
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

    def handle_subscribed_order_book(self, data: dict, write_message: Callable[[Mbp10Schema], None]):
        """Handle subscribed/order_book message."""
        self.last_sequence_number = data['offset']
        for ask in data['order_book']['asks']:
            self.asks.add(self._convert_to_fixed_length(ask))
        for bid in data['order_book']['bids']:
            self.bids.add(self._convert_to_fixed_length(bid))

        _time = int(Decimal(data['timestamp']) * Decimal(1_000_000))
        write_message(Mbp10Schema(
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
            **self.get_levels(),
        ))

    def _convert_to_fixed_length(self, level: dict):
        """Convert price and size to fixed-length format."""
        return {
            "price": int(Decimal(level['price']) * Decimal(_PRICE_SCALE)),
            "size": int(Decimal(level['size']) * Decimal(_SIZE_SCALE)),
        }

    def get_levels(self) -> dict:
        """Get level kwargs for Mbp10Schema from sorted lists."""
        kwargs = {}
        for i in range(self.MAX_LEVELS):
            if i < len(self.bids):
                kwargs[f'bid_price_{i}'] = self.bids[i]['price']
                kwargs[f'bid_size_{i}'] = self.bids[i]['size']
                kwargs[f'bid_count_{i}'] = 1
            if i < len(self.asks):
                kwargs[f'ask_price_{i}'] = self.asks[i]['price']
                kwargs[f'ask_size_{i}'] = self.asks[i]['size']
                kwargs[f'ask_count_{i}'] = 1
        return kwargs

    def handle_update_order_book(self, message: dict, write_message: Callable[[Mbp10Schema], None]):
        """Handle update/order_book message."""
        self.last_sequence_number = message['offset']

        _time = int(Decimal(message['timestamp']) * Decimal(1_000_000))

        ask_depth = self.update_orders(message['order_book']['asks'] or [], self.asks)
        bid_depth = self.update_orders(message['order_book']['bids'] or [], self.bids)

        depth = min(ask_depth, bid_depth)
        if depth >= self.MAX_LEVELS:
            return

        write_message(Mbp10Schema(
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
            **self.get_levels(),
        ))

    def handle_trades(self, message: dict, write_message: Callable[[Mbp10Schema], None]):
        """Handle trade message."""
        for trade in message['trades']:
            self.last_trade_price = int(Decimal(trade['price']) * Decimal(_PRICE_SCALE))
            self.last_trade_size = int(Decimal(trade['size']) * Decimal(_SIZE_SCALE))
            _time = int(Decimal(trade['timestamp']) * Decimal(1_000_000))
            side = "Ask" if not trade['is_maker_ask'] else "Bid"

            write_message(Mbp10Schema(
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
                **self.get_levels(),
            ))

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


class BinanceParser(ExchangeParser):
    """Parser for Binance incremental depth and trade messages."""

    MAX_LEVELS = 10

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.last_trade_price = None
        self.last_trade_size = None
        self.last_sequence_number = None
        self.bids = SortedList(key=lambda x: -x["price"])
        self.asks = SortedList(key=lambda x: x["price"])
        self.snapshot_fetched = False
        self.snapshot_last_update_id = None

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def parse(self, data: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        if "result" in data:
            return
        event = data.get("e")
        if event == "depthUpdate":
            self._handle_depth_update(data, write_message)
        elif event == "trade":
            self._handle_trade(data, write_message)

    def _fetch_snapshot(self) -> None:
        symbol = self.listing_info.exchange_security_symbol.upper()
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=100"
        with urllib.request.urlopen(url) as resp:
            snapshot = json.loads(resp.read())
        self.snapshot_last_update_id = snapshot["lastUpdateId"]
        for bid in snapshot["bids"]:
            self.bids.add(self._to_fixed(bid))
        for ask in snapshot["asks"]:
            self.asks.add(self._to_fixed(ask))
        self.snapshot_fetched = True

    def _to_fixed(self, level: list[str]) -> dict:
        return {
            "price": int(Decimal(level[0]) * Decimal(_PRICE_SCALE)),
            "size": int(Decimal(level[1]) * Decimal(_SIZE_SCALE)),
        }

    def _handle_depth_update(self, data: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        if not self.snapshot_fetched:
            self._fetch_snapshot()

        if data["u"] <= self.snapshot_last_update_id:
            return

        self.last_sequence_number = data["u"]
        bid_depth = self._update_orders([self._to_fixed(l) for l in data["b"]], self.bids)
        ask_depth = self._update_orders([self._to_fixed(l) for l in data["a"]], self.asks)
        min_depth = min(bid_depth, ask_depth)

        if min_depth >= self.MAX_LEVELS:
            return

        write_message(Mbp10Schema(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=data["E"] * 1_000_000,
            sequence=self.last_sequence_number,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=self.last_trade_price,
            size=self.last_trade_size,
            action="Modify",
            side="None",
            flags=["marketByPrice"],
            depth=min_depth,
            **self._get_levels(),
        ))

    def _handle_trade(self, data: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        self.last_trade_price = int(Decimal(data["p"]) * Decimal(_PRICE_SCALE))
        self.last_trade_size = int(Decimal(data["q"]) * Decimal(_SIZE_SCALE))
        side = "Ask" if data["m"] else "Bid"

        write_message(Mbp10Schema(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=data["T"] * 1_000_000,
            sequence=self.last_sequence_number,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=self.last_trade_price,
            size=self.last_trade_size,
            action="Trade",
            side=side,
            flags=["marketByPrice"],
            depth=None,
            **self._get_levels(),
        ))

    def _get_levels(self) -> dict:
        kwargs = {}
        for i in range(self.MAX_LEVELS):
            if i < len(self.bids):
                kwargs[f'bid_price_{i}'] = self.bids[i]["price"]
                kwargs[f'bid_size_{i}'] = self.bids[i]["size"]
                kwargs[f'bid_count_{i}'] = 1
            if i < len(self.asks):
                kwargs[f'ask_price_{i}'] = self.asks[i]["price"]
                kwargs[f'ask_size_{i}'] = self.asks[i]["size"]
                kwargs[f'ask_count_{i}'] = 1
        return kwargs

    def _update_orders(self, new_orders: list[dict], book: SortedList) -> int:
        depth = 99
        for new_order in new_orders:
            is_new = True
            for i in range(len(book)):
                if book[i]["price"] == new_order["price"]:
                    is_new = False
                    depth = min(depth, i)
                    if new_order["size"] == 0:
                        book.remove(book[i])
                    else:
                        book[i]["size"] = new_order["size"]
                    break
            if is_new and new_order["size"] > 0:
                book.add(new_order)
                depth = min(depth, book.index(new_order))
        return depth


class PolymarketParser(ExchangeParser):
    """Parser for Polymarket WebSocket messages."""

    MAX_LEVELS = 10

    def __init__(self, listing_info: ListingInfo):
        super().__init__(listing_info)
        self.last_trade_price = None
        self.last_trade_size = None
        self.bids = SortedList(key=lambda x: -x["price"])
        self.asks = SortedList(key=lambda x: x["price"])
        exchange_security_id = listing_info.exchange_security_id
        self.token_id = exchange_security_id.split(":", 1)[1] if ":" in exchange_security_id else exchange_security_id

    def get_transport_type(self) -> TransportType:
        return TransportType.WEBSOCKET

    def get_protocol_type(self) -> ProtocolType:
        return ProtocolType.JSON_WS

    def parse(self, data: dict | list, write_message: Callable[[Schema], None]) -> None:
        if isinstance(data, list):
            for event in data:
                self._parse_event(event, write_message)
        else:
            self._parse_event(data, write_message)

    def _parse_event(self, event: dict, write_message: Callable[[Schema], None]) -> None:
        event_type = event.get("event_type")
        if event_type == "book":
            self._handle_book(event, write_message)
        elif event_type == "price_change":
            self._handle_price_change(event, write_message)
        elif event_type == "last_trade_price":
            self._handle_trade(event, write_message)

    def _handle_book(self, event: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        self.bids.clear()
        self.asks.clear()

        for level in event.get("bids", []):
            price = int(Decimal(level["price"]) * Decimal(_PRICE_SCALE))
            size = int(Decimal(level["size"]) * Decimal(_SIZE_SCALE))
            self._update_level(True, price, size)

        for level in event.get("asks", []):
            price = int(Decimal(level["price"]) * Decimal(_PRICE_SCALE))
            size = int(Decimal(level["size"]) * Decimal(_SIZE_SCALE))
            self._update_level(False, price, size)

        last_trade_price_str = event.get("last_trade_price")
        if last_trade_price_str is not None:
            self.last_trade_price = int(Decimal(last_trade_price_str) * Decimal(_PRICE_SCALE))

        timestamp = int(event["timestamp"]) * 1_000_000
        self._emit(timestamp, "Modify", "None", self.last_trade_price, self.last_trade_size, write_message)

    def _handle_price_change(self, event: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        for change in event.get("price_changes", []):
            if change.get("asset_id") != self.token_id:
                continue
            side_str = change.get("side")
            if side_str not in ("BUY", "SELL"):
                continue
            price = int(Decimal(change["price"]) * Decimal(_PRICE_SCALE))
            size = int(Decimal(change["size"]) * Decimal(_SIZE_SCALE))
            self._update_level(side_str == "BUY", price, size)

        timestamp = int(event["timestamp"]) * 1_000_000
        self._emit(timestamp, "Modify", "None", self.last_trade_price, self.last_trade_size, write_message)

    def _handle_trade(self, event: dict, write_message: Callable[[Mbp10Schema], None]) -> None:
        price = int(Decimal(event["price"]) * Decimal(_PRICE_SCALE))
        size = int(Decimal(event["size"]) * Decimal(_SIZE_SCALE))
        self.last_trade_price = price
        self.last_trade_size = size
        side = "Bid" if event["side"] == "BUY" else "Ask"
        timestamp = int(event["timestamp"]) * 1_000_000
        self._emit(timestamp, "Trade", side, price, size, write_message)

    def _update_level(self, is_bid: bool, price: int, size: int) -> None:
        book = self.bids if is_bid else self.asks
        for i in range(len(book)):
            if book[i]["price"] == price:
                if size == 0:
                    book.remove(book[i])
                else:
                    book[i]["size"] = size
                return
        if size > 0:
            book.add({"price": price, "size": size})

    def _get_levels(self) -> dict:
        kwargs = {}
        for i in range(self.MAX_LEVELS):
            if i < len(self.bids):
                kwargs[f'bid_price_{i}'] = self.bids[i]["price"]
                kwargs[f'bid_size_{i}'] = self.bids[i]["size"]
                kwargs[f'bid_count_{i}'] = 1
            if i < len(self.asks):
                kwargs[f'ask_price_{i}'] = self.asks[i]["price"]
                kwargs[f'ask_size_{i}'] = self.asks[i]["size"]
                kwargs[f'ask_count_{i}'] = 1
        return kwargs

    def _emit(self, timestamp_event: int, action: str, side: str, price: int | None,
              size: int | None, write_message: Callable[[Mbp10Schema], None]) -> None:
        write_message(Mbp10Schema(
            exchange_id=self.listing_info.exchange_id,
            security_id=self.listing_info.security_id,
            timestamp_event=timestamp_event,
            sequence=None,
            timestamp_sent=None,
            timestamp_recv=time.time_ns(),
            price=price,
            size=size,
            action=action,
            side=side,
            flags=["marketByPrice"],
            depth=None,
            **self._get_levels(),
        ))


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
    elif exchange_name == "BINANCE":
        return BinanceParser(listing_info)
    elif exchange_name == "POLYMARKET":
        return PolymarketParser(listing_info)
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

