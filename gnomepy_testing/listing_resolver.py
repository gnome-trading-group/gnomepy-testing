"""
Listing resolver - uses gnomepy RegistryClient to fetch listing metadata.

This module provides a simple interface to resolve listing IDs into all the
information needed to connect to exchanges and parse market data.
"""
import logging
from dataclasses import dataclass
from gnomepy import RegistryClient


logger = logging.getLogger(__name__)


@dataclass
class ListingInfo:
    """
    Complete information about a listing.

    This contains everything needed to:
    - Connect to the exchange WebSocket
    - Subscribe to the correct data stream
    - The WebSocket implementation will determine the schema type
    """
    listing_id: int
    exchange_id: int
    exchange_name: str
    security_id: int
    security_symbol: str
    exchange_security_symbol: str

    def __str__(self) -> str:
        return f"{self.exchange_name}:{self.security_symbol} (Listing ID: {self.listing_id})"


class ListingResolver:
    """
    Resolves listing IDs to complete listing information using the Registry API.
    """

    def __init__(self, api_key: str | None = None):
        """
        Initialize the listing resolver.

        Args:
            api_key: Optional API key for the Registry. If not provided,
                    will look for GNOME_REGISTRY_API_KEY environment variable.
        """
        self.registry = RegistryClient(api_key=api_key)
    
    def resolve(self, listing_id: int) -> ListingInfo:
        """
        Resolve a listing ID to complete listing information.

        Args:
            listing_id: The listing ID to resolve

        Returns:
            ListingInfo with all metadata

        Raises:
            ValueError: If listing not found or invalid
            RuntimeError: If Registry API call fails
        """
        logger.info(f"Resolving listing ID: {listing_id}")

        try:
            listing_results = self.registry.get_listing(listing_id=listing_id)

            if not listing_results or len(listing_results) == 0:
                raise ValueError(f"Listing ID {listing_id} not found in registry")

            listing_data = listing_results[0]

            exchange_id = listing_data.exchange_id
            security_id = listing_data.security_id
            exchange_security_symbol = listing_data.exchange_security_symbol

            if not all([exchange_id, security_id]):
                raise ValueError(f"Incomplete listing data for ID {listing_id}: {listing_data}")

            exchange_results = self.registry.get_exchange(exchange_id=exchange_id)
            if not exchange_results or len(exchange_results) == 0:
                raise ValueError(f"Exchange ID {exchange_id} not found in registry")

            exchange_data = exchange_results[0]
            exchange_name = exchange_data.exchange_name

            if not exchange_name:
                raise ValueError(f"Exchange name not found for ID {exchange_id}")

            security_results = self.registry.get_security(security_id=security_id)
            if not security_results or len(security_results) == 0:
                raise ValueError(f"Security ID {security_id} not found in registry")

            security_data = security_results[0]
            security_symbol = security_data.symbol

            if not security_symbol:
                raise ValueError(f"Security symbol not found for ID {security_id}")

            listing_info = ListingInfo(
                listing_id=listing_id,
                exchange_id=exchange_id,
                exchange_name=exchange_name.upper(),
                security_id=security_id,
                security_symbol=security_symbol,
                exchange_security_symbol=exchange_security_symbol
            )

            logger.info(f"Resolved listing {listing_id}: {listing_info}")
            return listing_info

        except Exception as e:
            logger.error(f"Failed to resolve listing {listing_id}: {e}")
            raise RuntimeError(f"Failed to resolve listing {listing_id}: {e}") from e
    



# Singleton instance for convenience
_resolver: ListingResolver | None = None


def get_resolver(api_key: str | None = None) -> ListingResolver:
    """
    Get or create the global ListingResolver instance.

    Args:
        api_key: Optional API key. Only used on first call.

    Returns:
        ListingResolver instance
    """
    global _resolver
    if _resolver is None:
        _resolver = ListingResolver(api_key=api_key)
    return _resolver


def resolve_listing(listing_id: int, api_key: str | None = None) -> ListingInfo:
    """
    Convenience function to resolve a listing ID.

    Args:
        listing_id: The listing ID to resolve
        api_key: Optional API key

    Returns:
        ListingInfo with all metadata
    """
    resolver = get_resolver(api_key=api_key)
    return resolver.resolve(listing_id)

