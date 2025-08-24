from abc import ABC, abstractmethod
from typing import Any

from market_data.models import Candle, Ticker


class TickersFetcher(ABC):
  """Abstract base class for ticker fetching functionality."""

  @abstractmethod
  def get_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of stock tickers.

    Args:
      **kwargs: Provider-specific arguments (e.g., exchange, market)

    Returns:
      List of Ticker objects
    """
    pass


class CandlesFetcher(ABC):
  """Abstract base class for candle data fetching functionality."""

  @abstractmethod
  def get_candles(self, **kwargs: Any) -> list[Candle]:
    """Fetches candle (OHLCV) data for a ticker.

    Args:
      **kwargs: Provider-specific arguments (e.g., ticker, from_date, to_date)

    Returns:
      List of Candle objects
    """
    pass


class OptionableFetcher(ABC):
  """Abstract base class for optionable ticker fetching functionality."""

  @abstractmethod
  def get_optionable_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of optionable stock tickers.

    Args:
      **kwargs: Provider-specific arguments (e.g., type, exchange)

    Returns:
      List of Ticker objects with optionable=True
    """
    pass
