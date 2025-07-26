# stock_fetcher/strategies/base.py

from abc import ABC, abstractmethod
from typing import Any


class DataFetchingStrategy(ABC):
  """
  The Strategy interface declares operations common to all supported versions
  of a data-fetching algorithm.
  """

  @abstractmethod
  def fetch(self, ticker: str) -> dict[str, Any]:
    """
    Fetch stock data for a given ticker.

    Args:
        ticker: The stock symbol.

    Returns:
        A dictionary containing the stock data.
    """
    pass
