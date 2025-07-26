# strategies/polygon_candles.py


from polygon import RESTClient

from .candle_base import CandleFetchingStrategy


class PolygonCandleStrategy(CandleFetchingStrategy):
  """
  The concrete strategy for fetching candle data from Polygon.io.
  """

  def __init__(self, api_key: str, limit: int = 50000):
    self.client = RESTClient(api_key)
    self.limit = limit

  def fetch_candles(
    self, ticker: str, interval: str, from_date: str, to_date: str
  ) -> list[dict]:
    """
    Implements the fetching logic by calling the polygon-python-client.
    """
    try:
      # Note: We are mapping our generic 'interval' parameter to
      # Polygon's specific 'timespan' parameter.
      resp = self.client.get_aggs(
        ticker=ticker,
        multiplier=1,  # Hardcoded as per our minimalist contract
        timespan=interval,
        from_=from_date,
        to=to_date,
        limit=self.limit,
      )

      # Convert the client's response objects to dictionaries
      # to fulfill our contract's return type.
      return [
        {
          "open": agg.open,
          "high": agg.high,
          "low": agg.low,
          "close": agg.close,
          "volume": agg.volume,
          "timestamp": agg.timestamp,
        }
        for agg in resp
      ]
    except Exception as e:
      print(f"An error occurred with Polygon.io: {e}")
      return []
