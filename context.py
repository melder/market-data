# context.py

from typing import Any

from strategies.base import DataFetchingStrategy


class StockFetcher:
  """
  The Context defines the interface of interest to clients. It maintains a
  reference to a strategy object and delegates the actual work to it.
  """

  def __init__(self, strategy: DataFetchingStrategy) -> None:
    self._strategy = strategy

  @property
  def strategy(self) -> DataFetchingStrategy:
    """Gets the current strategy."""
    return self._strategy

  @strategy.setter
  def strategy(self, strategy: DataFetchingStrategy) -> None:
    """Allows replacing the strategy object at runtime."""
    self._strategy = strategy

  def fetch_stock_data(self, ticker: str) -> dict[str, Any]:
    """
    Delegates the fetching work to the held strategy object. The context
    itself does not know the implementation details.
    """
    print(f"Context: Using {self._strategy.__class__.__name__} to fetch data.")
    result = self._strategy.fetch(ticker)
    return result

  def fetch_candles(self, ticker, start, end):
    return self._strategy.candles(ticker, start, end)
