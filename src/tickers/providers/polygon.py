# providers/polygon.py
import logging
import time
from typing import Any

from polygon import RESTClient
from requests.exceptions import HTTPError

from tickers.models import Candle, Ticker


class PolygonProvider:
  """Polygon.io data provider strategy."""

  # Constants for fetching tickers to avoid magic numbers
  _MAX_CANDLE_LIMIT = 50000
  _TICKERS_PAGE_LIMIT = 1000
  _RATE_LIMIT_PAUSE_SECONDS = 12

  def __init__(self, api_key: str | None):
    """
    Initializes the Polygon.io provider.

    Args:
        api_key: The API key for Polygon.io.

    Raises:
        ValueError: If the API key is not provided.
    """
    if not api_key:
      raise ValueError("Polygon provider requires an API key.")
    self.client = RESTClient(api_key)

  def get_candles(self, **kwargs: Any) -> list[Candle]:
    """
    Fetches candle data specifically from the Polygon.io API.

    Expected kwargs:
    - ticker (str)
    - from_date (str)
    - to_date (str)
    - timespan (str, optional)
    - multiplier (int, optional)
    """
    try:
      resp = self.client.get_aggs(
        ticker=kwargs.get("ticker"),
        multiplier=kwargs.get("multiplier", 1),
        timespan=kwargs.get("timespan", "day"),
        from_=kwargs.get("from_date"),
        to=kwargs.get("to_date"),
        limit=kwargs.get("limit", self._MAX_CANDLE_LIMIT),
      )

      return [
        Candle(
          open=agg.open,
          high=agg.high,
          low=agg.low,
          close=agg.close,
          volume=agg.volume,
          timestamp=agg.timestamp,
        )
        for agg in resp
      ]
    except Exception as e:
      logging.error(f"An error occurred with Polygon.io get_candles: {e}")
      return []

  def get_tickers(self, **kwargs: Any) -> list[Ticker]:
    """
    Fetches all available tickers from Polygon.io using the paginated iterator.
    It pauses between pages to respect the free tier's 5 calls/minute rate limit.
    """
    all_tickers = []
    tickers_processed_since_pause = 0

    try:
      # client.list_tickers() returns an iterator that handles pagination automatically.
      # The SDK will fetch new pages as needed when the loop continues.
      for t in self.client.list_tickers(
        limit=self._TICKERS_PAGE_LIMIT, market="stocks"
      ):
        all_tickers.append(
          Ticker(
            ticker=t.ticker,
            name=t.name,
            market=t.market,
            locale=t.locale,
            primary_exchange=t.primary_exchange,
            type=t.type,
            active=t.active,
            currency_name=t.currency_name,
            cik=t.cik,
            last_updated_utc=t.last_updated_utc,
          )
        )
        tickers_processed_since_pause += 1

        # After processing a full page of results, pause before the iterator
        # makes the next underlying API call.
        if tickers_processed_since_pause % self._TICKERS_PAGE_LIMIT == 0:
          logging.info(
            f"Processed {len(all_tickers)} tickers. Pausing for {self._RATE_LIMIT_PAUSE_SECONDS} seconds..."
          )
          time.sleep(self._RATE_LIMIT_PAUSE_SECONDS)
          tickers_processed_since_pause = 0

    except HTTPError as e:
      # This will catch rate limit errors (429) if our proactive pause isn't enough.
      logging.error(
        f"An HTTP error occurred while fetching tickers from Polygon.io: {e}"
      )
      return []
    except Exception as e:
      # Catches other unexpected issues.
      logging.error(
        f"An unexpected error occurred while fetching tickers from Polygon.io: {e}"
      )
      return []

    logging.info(f"Finished fetching. Total tickers found: {len(all_tickers)}")
    return all_tickers
