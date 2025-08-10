from typing import Any, Protocol

from market_data.models import Candle, Ticker


class DataProvider(Protocol):
  """
  A protocol defining the interface for all data provider strategies.

  This acts as a contract, ensuring that any class that implements this
  protocol will have the required methods for fetching financial data.
  """

  def __init__(self, api_key: str | None = None) -> None:
    """Initializes the provider, handling the API key if required."""
    ...

  def get_candles(self, **kwargs: Any) -> list[Candle]: ...

  def get_tickers(self, **kwargs: Any) -> list[Ticker]: ...
