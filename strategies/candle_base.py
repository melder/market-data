# strategies/candle_base.py

from abc import ABC, abstractmethod


class CandleFetchingStrategy(ABC):
  """
  The Strategy interface for fetching candle (OHLC) data.
  This is the contract all concrete candle strategies must follow.
  """

  @abstractmethod
  def fetch_candles(
    self, ticker: str, timespan: str, from_date: str, to_date: str
  ) -> list[dict]:
    """
    Fetch candle data for a given ticker and date range.

    Args:
        ticker: The stock symbol.
        timespan: The size of each candle (e.g., 'day', 'hour', 'minute').
        from_date: The start of the aggregate time window (YYYY-MM-DD).
        to_date: The end of the aggregate time window (YYYY-MM-DD).

    Returns:
        A list of dictionaries, where each dictionary represents one candle.
    """
    pass
