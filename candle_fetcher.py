# candle_fetcher.py


from strategies.candle_base import CandleFetchingStrategy


class CandleFetcher:
  """
  The Context for fetching candle data. It maintains a reference to a
  candle-fetching strategy and delegates the work to it.
  """

  def __init__(self, strategy: CandleFetchingStrategy) -> None:
    self._strategy = strategy

  @property
  def strategy(self) -> CandleFetchingStrategy:
    return self._strategy

  @strategy.setter
  def strategy(self, strategy: CandleFetchingStrategy) -> None:
    """Allows replacing the strategy object at runtime."""
    self._strategy = strategy

  def get_candle_data(
    self, ticker: str, interval: str, from_date: str, to_date: str
  ) -> list[dict]:
    """
    Delegates the fetching work to the held strategy object.
    """
    return self._strategy.fetch_candles(
      ticker=ticker, interval=interval, from_date=from_date, to_date=to_date
    )
