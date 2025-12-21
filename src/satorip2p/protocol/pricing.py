"""
satorip2p/protocol/pricing.py

Price feed providers for EVR and SATORI tokens.

Uses SafeTrade exchange API to get real-time pricing:
- https://safe.trade/api/v2/trade/public/tickers/evrusdt
- https://safe.trade/api/v2/trade/public/tickers/satoriusdt

The EVR/SATORI exchange rate is calculated from the two USDT pairs.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# SafeTrade API endpoints
SAFETRADE_BASE_URL = "https://safe.trade/api/v2/trade/public"
SAFETRADE_EVR_USDT = f"{SAFETRADE_BASE_URL}/tickers/evrusdt"
SAFETRADE_SATORI_USDT = f"{SAFETRADE_BASE_URL}/tickers/satoriusdt"

# Cache duration in seconds
PRICE_CACHE_TTL = 60  # 1 minute cache

# Request timeout in seconds
REQUEST_TIMEOUT = 10

# Default fallback prices if API unavailable
DEFAULT_EVR_USD = 0.002     # $0.002 per EVR
DEFAULT_SATORI_USD = 0.10   # $0.10 per SATORI


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class TickerData:
    """SafeTrade ticker response data."""
    ticker_id: str           # e.g., "evrusdt"
    last_price: float        # Last trade price
    base_volume: float       # 24h volume in base currency
    quote_volume: float      # 24h volume in quote currency
    high: float              # 24h high
    low: float               # 24h low
    change: float            # 24h change percentage
    timestamp: int           # Ticker timestamp


@dataclass
class PriceQuote:
    """Price quote with metadata."""
    evr_usdt: float          # EVR price in USDT
    satori_usdt: float       # SATORI price in USDT
    evr_satori_ratio: float  # How many SATORI per EVR
    timestamp: int           # When quote was fetched
    source: str              # "safetrade", "cache", or "fallback"


# ============================================================================
# SAFETRADE PRICE PROVIDER
# ============================================================================

class SafeTradePriceProvider:
    """
    Fetches real-time EVR and SATORI prices from SafeTrade exchange.

    Usage:
        provider = SafeTradePriceProvider()
        evr_usd, satori_usd = await provider.get_exchange_rates()

        # Or get full quote with metadata
        quote = await provider.get_price_quote()
        print(f"EVR/SATORI ratio: {quote.evr_satori_ratio}")
    """

    def __init__(
        self,
        cache_ttl: int = PRICE_CACHE_TTL,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """
        Initialize SafeTrade price provider.

        Args:
            cache_ttl: How long to cache prices (seconds)
            timeout: Request timeout (seconds)
        """
        self._cache_ttl = cache_ttl
        self._timeout = timeout
        self._cached_quote: Optional[PriceQuote] = None
        self._cache_time: int = 0

    async def get_exchange_rates(self) -> Tuple[float, float]:
        """
        Get current exchange rates (EVR/USD, SATORI/USD).

        This is the callback format expected by DonationManager.

        Returns:
            Tuple of (evr_usd_rate, satori_usd_rate)
        """
        quote = await self.get_price_quote()
        return (quote.evr_usdt, quote.satori_usdt)

    async def get_price_quote(self) -> PriceQuote:
        """
        Get full price quote with all data.

        Returns:
            PriceQuote with EVR/SATORI prices and ratio
        """
        # Check cache first
        now = int(time.time())
        if self._cached_quote and (now - self._cache_time) < self._cache_ttl:
            return PriceQuote(
                evr_usdt=self._cached_quote.evr_usdt,
                satori_usdt=self._cached_quote.satori_usdt,
                evr_satori_ratio=self._cached_quote.evr_satori_ratio,
                timestamp=self._cached_quote.timestamp,
                source="cache"
            )

        # Fetch fresh prices
        try:
            evr_ticker = await self._fetch_ticker(SAFETRADE_EVR_USDT)
            satori_ticker = await self._fetch_ticker(SAFETRADE_SATORI_USDT)

            evr_usdt = evr_ticker.last_price if evr_ticker else DEFAULT_EVR_USD
            satori_usdt = satori_ticker.last_price if satori_ticker else DEFAULT_SATORI_USD

            # Calculate EVR/SATORI ratio
            # How many SATORI does 1 EVR buy?
            if satori_usdt > 0:
                evr_satori_ratio = evr_usdt / satori_usdt
            else:
                evr_satori_ratio = DEFAULT_EVR_USD / DEFAULT_SATORI_USD

            quote = PriceQuote(
                evr_usdt=evr_usdt,
                satori_usdt=satori_usdt,
                evr_satori_ratio=evr_satori_ratio,
                timestamp=now,
                source="safetrade"
            )

            # Update cache
            self._cached_quote = quote
            self._cache_time = now

            logger.debug(
                f"Price update: EVR=${evr_usdt:.6f}, SATORI=${satori_usdt:.6f}, "
                f"ratio={evr_satori_ratio:.6f}"
            )

            return quote

        except Exception as e:
            logger.warning(f"Failed to fetch prices from SafeTrade: {e}")
            return self._get_fallback_quote()

    async def _fetch_ticker(self, url: str) -> Optional[TickerData]:
        """Fetch ticker data from SafeTrade API."""
        try:
            # Use aiohttp if available, otherwise fall back to requests in thread
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=self._timeout) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._parse_ticker(data)
                        else:
                            logger.warning(f"SafeTrade API returned {response.status}")
                            return None
            except ImportError:
                # Fallback to requests in thread pool
                import requests
                import concurrent.futures
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    response = await loop.run_in_executor(
                        pool,
                        lambda: requests.get(url, timeout=self._timeout)
                    )
                    if response.status_code == 200:
                        data = response.json()
                        return self._parse_ticker(data)
                    else:
                        logger.warning(f"SafeTrade API returned {response.status_code}")
                        return None

        except asyncio.TimeoutError:
            logger.warning(f"SafeTrade API timeout for {url}")
            return None
        except Exception as e:
            logger.warning(f"SafeTrade API error: {e}")
            return None

    def _parse_ticker(self, data: dict) -> Optional[TickerData]:
        """Parse SafeTrade ticker response.

        Expected format from SafeTrade API v2:
        {
            "ticker_id": "evrusdt",
            "base_currency": "evr",
            "target_currency": "usdt",
            "last_price": "0.00123",
            "base_volume": "12345.67",
            "target_volume": "15.18",
            "bid": "0.00122",
            "ask": "0.00124",
            "high": "0.00130",
            "low": "0.00120",
            "change": "2.5"
        }
        """
        try:
            return TickerData(
                ticker_id=data.get('ticker_id', ''),
                last_price=float(data.get('last_price', 0)),
                base_volume=float(data.get('base_volume', 0)),
                quote_volume=float(data.get('target_volume', 0)),
                high=float(data.get('high', 0)),
                low=float(data.get('low', 0)),
                change=float(data.get('change', 0)),
                timestamp=int(time.time())
            )
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse ticker data: {e}")
            return None

    def _get_fallback_quote(self) -> PriceQuote:
        """Return fallback quote when API is unavailable."""
        return PriceQuote(
            evr_usdt=DEFAULT_EVR_USD,
            satori_usdt=DEFAULT_SATORI_USD,
            evr_satori_ratio=DEFAULT_EVR_USD / DEFAULT_SATORI_USD,
            timestamp=int(time.time()),
            source="fallback"
        )

    def get_evr_to_satori(self, evr_amount: float, slippage: float = 0.10) -> float:
        """
        Calculate how much SATORI an EVR donation is worth.

        Args:
            evr_amount: Amount of EVR being donated
            slippage: Slippage buffer (default 10% = 0.10)

        Returns:
            SATORI amount (after slippage deduction)
        """
        if self._cached_quote:
            ratio = self._cached_quote.evr_satori_ratio
        else:
            ratio = DEFAULT_EVR_USD / DEFAULT_SATORI_USD

        fair_value = evr_amount * ratio
        return fair_value * (1 - slippage)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

# Global singleton provider
_provider: Optional[SafeTradePriceProvider] = None


def get_price_provider() -> SafeTradePriceProvider:
    """Get or create the global price provider instance."""
    global _provider
    if _provider is None:
        _provider = SafeTradePriceProvider()
    return _provider


async def get_exchange_rates() -> Tuple[float, float]:
    """
    Convenience function to get exchange rates.

    This can be passed directly to DonationManager as exchange_rate_provider.

    Returns:
        Tuple of (evr_usd_rate, satori_usd_rate)
    """
    provider = get_price_provider()
    return await provider.get_exchange_rates()


async def get_evr_satori_ratio() -> float:
    """
    Get current EVR/SATORI exchange ratio.

    Returns:
        How many SATORI 1 EVR is worth
    """
    provider = get_price_provider()
    quote = await provider.get_price_quote()
    return quote.evr_satori_ratio


def calculate_satori_reward(evr_amount: float, slippage: float = 0.10) -> float:
    """
    Calculate SATORI reward for an EVR donation (synchronous).

    Uses cached prices. Call get_exchange_rates() first to update cache.

    Args:
        evr_amount: Amount of EVR being donated
        slippage: Slippage buffer (default 10%)

    Returns:
        SATORI reward amount
    """
    provider = get_price_provider()
    return provider.get_evr_to_satori(evr_amount, slippage)
