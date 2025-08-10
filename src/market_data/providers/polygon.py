from __future__ import annotations

import functools
import logging
import time
from typing import Any

from polygon import RESTClient
from pydantic import ValidationError
from requests.exceptions import HTTPError

from market_data.interfaces import CandlesFetcher, TickersFetcher
from market_data.models import Candle, Ticker

# --- Module-level Constants ---
_MAX_CANDLE_LIMIT = 50000
_TICKERS_PAGE_LIMIT = 1000
_RATE_LIMIT_PAUSE_SECONDS = 12

# --- Private Fetcher Implementations ---


def _get_candles_impl(client: RESTClient, **kwargs: Any) -> list[Candle]:
  """Fetches candle data from the Polygon.io API."""
  try:
    resp = client.get_aggs(
      ticker=kwargs["ticker"],
      multiplier=kwargs.get("multiplier", 1),
      timespan=kwargs.get("timespan", "day"),
      from_=kwargs["from_date"],
      to=kwargs["to_date"],
      limit=_MAX_CANDLE_LIMIT,
    )

    valid_candles = []
    for agg in resp:
      try:
        # Pydantic can validate directly from the SDK's object attributes.
        valid_candles.append(Candle.model_validate(agg))
      except ValidationError as e:
        logging.warning(
          f"Skipping polygon candle for {kwargs['ticker']} due to validation error: {e}"
        )
    return valid_candles
  except Exception as e:
    logging.error(f"Error with Polygon.io get_candles: {e}", exc_info=True)
    return []


def _get_tickers_impl(client: RESTClient, **kwargs: Any) -> list[Ticker]:
  """Fetches all available tickers from Polygon.io with rate-limiting."""
  all_tickers = []
  tickers_processed = 0

  # Get the type to exclude from the arguments
  exclude_type = kwargs.get("exclude_type")
  if exclude_type:
    logging.info(f"Polygon provider will exclude tickers of type '{exclude_type}'.")

  try:
    for t in client.list_tickers(limit=_TICKERS_PAGE_LIMIT, market="stocks"):
      try:
        validated_ticker = Ticker.model_validate(t)

        # This is the new filtering logic
        if exclude_type and validated_ticker.type == exclude_type:
          continue  # Skip this ticker

        all_tickers.append(validated_ticker)
      except ValidationError as e:
        ticker_symbol = getattr(t, "ticker", "UNKNOWN")
        logging.warning(
          f"Skipping polygon ticker '{ticker_symbol}' due to validation error: {e}"
        )

      tickers_processed += 1
      if tickers_processed > 0 and tickers_processed % _TICKERS_PAGE_LIMIT == 0:
        logging.info(f"Processed {len(all_tickers)} tickers. Pausing for rate limit...")
        time.sleep(_RATE_LIMIT_PAUSE_SECONDS)
  except HTTPError as e:
    logging.error(f"HTTP error fetching tickers from Polygon.io: {e}", exc_info=True)
    return []
  except Exception as e:
    logging.error(
      f"Unexpected error fetching tickers from Polygon.io: {e}", exc_info=True
    )
    return []

  logging.info(f"Finished fetching. Total tickers found: {len(all_tickers)}")
  return all_tickers


# --- Public Provider Class ---


class PolygonProvider:
  def __init__(self, api_key: str):
    if not api_key:
      raise ValueError("Polygon provider requires an API key.")

    client = RESTClient(api_key)

    self._capabilities = {
      TickersFetcher: functools.partial(_get_tickers_impl, client=client),
      CandlesFetcher: functools.partial(_get_candles_impl, client=client),
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
