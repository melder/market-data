# providers/alpha_vantage.py
import io
import logging
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from alpha_vantage.timeseries import TimeSeries
from pandas.errors import ParserError

from tickers.models import Candle, Ticker


def _transform_ticker_data(df: pd.DataFrame) -> list[Ticker]:
  """
  Transforms a raw DataFrame from the Alpha Vantage listing status endpoint
  into a list of Ticker data objects.
  """
  if df.empty:
    return []

  # Standardize column names to match our Ticker model
  df.rename(
    columns={
      "symbol": "ticker",
      "assetType": "type",
      "exchange": "primary_exchange",
    },
    inplace=True,
  )
  df["active"] = df["status"] == "Active"

  # Use a list comprehension with itertuples for efficiency and readability.
  # itertuples is much faster than creating dicts with to_dict("records").
  tickers = [
    Ticker(
      ticker=row.ticker,
      name=row.name,
      active=row.active,
      primary_exchange=getattr(row, "primary_exchange", None),
      type=getattr(row, "type", None),
      # These fields are not provided by this endpoint
      market=None,
      locale=None,
      currency_name=None,
      cik=None,
      last_updated_utc=None,
    )
    for row in df.itertuples(index=False)
  ]

  logging.info(
    f"Successfully transformed {len(tickers)} active tickers from Alpha Vantage."
  )
  return tickers


class AlphaVantageProvider:
  """Alpha Vantage data provider strategy."""

  def __init__(self, api_key: str | None):
    """
    Initializes the Alpha Vantage provider.

    Args:
        api_key: The API key for Alpha Vantage.

    Raises:
        ValueError: If the API key is not provided.
    """
    if not api_key:
      raise ValueError("Alpha Vantage provider requires an API key.")
    self.api_key = api_key
    self.ts_client = TimeSeries(key=self.api_key, output_format="json")

  def get_candles(self, **kwargs: Any) -> list[Candle]:
    """
    Fetches daily candle data specifically from the Alpha Vantage API.
    Note: The free tier has a strict rate limit.
    """
    try:
      data, _ = self.ts_client.get_daily_adjusted(
        symbol=kwargs.get("ticker"), outputsize=kwargs.get("outputsize", "compact")
      )

      candles = []
      for date_str, daily_data in data.items():
        # Convert date string to a Unix timestamp in milliseconds for consistency.
        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
        timestamp_ms = int(dt_obj.timestamp() * 1000)

        candles.append(
          Candle(
            open=float(daily_data["1. open"]),
            high=float(daily_data["2. high"]),
            low=float(daily_data["3. low"]),
            close=float(daily_data["4. close"]),
            volume=int(daily_data["6. volume"]),
            timestamp=timestamp_ms,
          )
        )
      return candles

    except Exception as e:
      # Catch potential rate limit errors or other issues.
      logging.error(f"An error occurred with Alpha Vantage get_candles: {e}")
      return []

  def get_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of active stock tickers from Alpha Vantage."""
    # The API defaults to state=active if the parameter is not provided.
    url = (
      f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={self.api_key}"
    )
    try:
      logging.info("Downloading active ticker list from Alpha Vantage...")
      response = requests.get(url, timeout=30)
      response.raise_for_status()

      try:
        df = pd.read_csv(io.StringIO(response.text))
      except ParserError:
        logging.error(
          "Failed to parse CSV from Alpha Vantage. The response may be an error message (e.g., API limit reached)."
        )
        logging.error(f"Response content: {response.text}")
        return []

      return _transform_ticker_data(df)

    except requests.exceptions.RequestException as e:
      logging.error(
        f"An HTTP error occurred while fetching tickers from Alpha Vantage: {e}"
      )
      return []
    except Exception as e:
      logging.error(
        f"An unexpected error occurred while fetching tickers from Alpha Vantage: {e}"
      )
      return []
