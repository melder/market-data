from __future__ import annotations

import functools
import io
import logging
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from alpha_vantage.timeseries import TimeSeries
from pandas.errors import ParserError
from pydantic import ValidationError

from market_data.interfaces import CandlesFetcher, TickersFetcher
from market_data.models import Candle, Ticker
from market_data.utils.parsers import read_csv_from_buffer


def _parse_tickers_from_raw(df: pd.DataFrame) -> list[Ticker]:
  """Transforms and validates a raw DataFrame into a list of Ticker objects."""
  if df.empty:
    return []
  df = df.rename(
    columns={"symbol": "ticker", "assetType": "type", "exchange": "primary_exchange"}
  )
  df["active"] = df["status"] == "Active"

  valid_tickers = []
  for row in df.itertuples(index=False):
    try:
      # Convert the row to a dictionary before validation.
      valid_tickers.append(Ticker.model_validate(row._asdict()))
    except ValidationError as e:
      ticker_symbol = getattr(row, "ticker", "UNKNOWN")
      logging.warning(f"Skipping ticker '{ticker_symbol}' due to validation error: {e}")

  return valid_tickers


def _map_api_candle_to_candle(date_str: str, daily_data: dict) -> Candle:
  """Maps a daily data entry from Alpha Vantage to a Candle model."""
  dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
  return Candle(
    open=float(daily_data["1. open"]),
    high=float(daily_data["2. high"]),
    low=float(daily_data["3. low"]),
    close=float(daily_data["4. close"]),
    volume=int(daily_data["6. volume"]),
    timestamp=int(dt_obj.timestamp() * 1000),
  )


def _get_tickers_impl(api_key: str, **kwargs: Any) -> list[Ticker]:
  url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}"
  try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    df = read_csv_from_buffer(io.StringIO(response.text))
    return _parse_tickers_from_raw(df)
  except ParserError:
    logging.error(f"Failed to parse CSV from Alpha Vantage: {response.text}")
    return []
  except requests.exceptions.RequestException as e:
    logging.error(f"HTTP error fetching Alpha Vantage tickers: {e}", exc_info=True)
    return []


def _get_candles_impl(ts_client: TimeSeries, **kwargs: Any) -> list[Candle]:
  """Fetches daily candle data from the Alpha Vantage API."""
  try:
    data, _ = ts_client.get_daily_adjusted(
      symbol=kwargs["ticker"], outputsize=kwargs.get("outputsize", "compact")
    )
    return [_map_api_candle_to_candle(d, v) for d, v in data.items()]
  except ValidationError as e:
    # This is an expected error if the API returns malformed data.
    logging.warning(f"Alpha Vantage data failed validation for {kwargs['ticker']}: {e}")
    return []
  except Exception as e:
    # This is for unexpected errors (e.g., rate limits, network issues).
    logging.error(
      f"An unexpected error occurred with Alpha Vantage get_candles: {e}", exc_info=True
    )
    return []


class AlphaVantageProvider:
  def __init__(self, api_key: str):
    if not api_key:
      raise ValueError("Alpha Vantage provider requires an API key.")

    ts_client = TimeSeries(key=api_key, output_format="json")

    self._capabilities = {
      TickersFetcher: functools.partial(_get_tickers_impl, api_key=api_key),
      CandlesFetcher: functools.partial(_get_candles_impl, ts_client=ts_client),
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
