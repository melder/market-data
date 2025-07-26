# strategies/polygon_io.py

from typing import Any

from polygon import RESTClient

from .base import DataFetchingStrategy


class PolygonIOStrategy(DataFetchingStrategy):
  """
  A concrete strategy for fetching data from Polygon.io.
  """

  def __init__(self, api_key: str):
    """Initializes the strategy with the necessary API key."""
    self.client = RESTClient(api_key)

  def fetch(self, ticker: str) -> dict[str, Any]:
    """
    Implements the fetching logic for Polygon.io.
    This uses the "Previous Day Close" endpoint.
    """
    try:
      # The get_previous_close_agg returns a list of aggregates
      resp = self.client.get_previous_close_agg(ticker)

      if not resp:
        return {"source": "Polygon.io", "error": f"No data found for ticker '{ticker}'"}

      # The price is in the 'close' attribute of the first result
      price = resp[0].close

      return {
        "source": "Polygon.io",
        "ticker": ticker,
        "price": f"{price:.2f}",
        "currency": "USD",  # Polygon API generally returns USD
      }
    except Exception as e:
      # Catch potential API errors (e.g., invalid ticker, auth issues)
      return {"source": "Polygon.io", "error": str(e)}
