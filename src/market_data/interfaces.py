from abc import ABC, abstractmethod
from typing import Any

from market_data.models import Candle, Ticker


class TickersFetcher(ABC):
  @abstractmethod
  def get_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of stock tickers."""
    pass


class CandlesFetcher(ABC):
  @abstractmethod
  def get_candles(self, **kwargs: Any) -> list[Candle]:
    """Fetches candle (OHLCV) data for a ticker."""
    pass


class OptionableFetcher(ABC):
  @abstractmethod
  def get_optionable_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of optionable stock tickers."""
    pass
