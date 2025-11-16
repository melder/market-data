from __future__ import annotations

import functools
import logging
import time
from typing import Any

from polygon import RESTClient
from pydantic import ValidationError
from requests.exceptions import HTTPError

from market_data.interfaces import (
  CandlesFetcher,
  MetadataFetcher,
  OptionableFetcher,
  TickersFetcher,
)
from market_data.models import Candle, Ticker

# --- Module-level Constants ---
_MAX_CANDLE_LIMIT = 50000
_TICKERS_PAGE_LIMIT = 1000
_RATE_LIMIT_PAUSE_SECONDS = 12
_OPTIONS_CONTRACTS_LIMIT = 1000  # Max per request for options contracts
_MAX_RETRIES = 5  # Max retry attempts for rate limited requests

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


def _get_optionable_tickers_impl(client: RESTClient, **kwargs: Any) -> list[Ticker]:
  """Fetches optionable ticker symbols from Polygon.io options contracts API.

  Args:
    client: Polygon RESTClient instance
    **kwargs: Additional keyword arguments

  Returns:
    List of Ticker objects with optionable=True
  """
  contracts_processed = 0
  unique_tickers = set()

  # TODO: Handle rate limiting during pagination - currently if rate limit occurs
  # during SDK's internal pagination, the entire operation fails and we lose all progress
  for contract in client.list_options_contracts(limit=_OPTIONS_CONTRACTS_LIMIT):
    underlying = getattr(contract, "underlying_ticker", None)
    if underlying:
      unique_tickers.add(underlying)

    contracts_processed += 1
    if contracts_processed > 0 and contracts_processed % _TICKERS_PAGE_LIMIT == 0:
      logging.info(
        f"Processed {contracts_processed} contracts. Pausing for rate limit..."
      )
      time.sleep(_RATE_LIMIT_PAUSE_SECONDS)

  logging.info(f"Found {len(unique_tickers)} unique optionable tickers")

  valid_tickers = []
  for ticker_symbol in sorted(unique_tickers):
    try:
      ticker_obj = Ticker(
        ticker=ticker_symbol,
        name=None,  # Options contracts don't include company names
        active=True,
        optionable=True,
      )
      valid_tickers.append(ticker_obj)
    except ValidationError as e:
      logging.warning(f"Skipping ticker '{ticker_symbol}' due to validation error: {e}")

  logging.info(f"Successfully processed {len(valid_tickers)} optionable tickers")
  return valid_tickers


def _get_ticker_metadata_impl(client: RESTClient, **kwargs: Any) -> list[Ticker]:
  """Fetches enriched metadata for one or more tickers from Polygon.io.

  This implementation iterates through each ticker and makes a separate API call.
  It respects the delay parameter to avoid hitting rate limits.

  Args:
    client: The Polygon RESTClient.
    **kwargs: Expects 'tickers' (a list of strings) and 'delay' (float).

  Returns:
    A list of Ticker objects with enriched metadata.
  """
  tickers_param = kwargs.get("tickers", [])
  delay = kwargs.get("delay", _RATE_LIMIT_PAUSE_SECONDS)

  if not tickers_param:
    logging.warning("No tickers provided for Polygon metadata fetch.")
    return []

  logging.info(
    "Fetching Polygon metadata for %d tickers (delay %.2fs)",
    len(tickers_param),
    delay,
  )

  metadata_list = []
  for i, ticker_symbol in enumerate(tickers_param):
    try:
      logging.debug(f"Fetching metadata for {ticker_symbol} from Polygon.")
      # The get_ticker_details is available through the client's reference attribute
      details = client.get_ticker_details(ticker_symbol)

      # Manually construct the Ticker object from the details response
      ticker_obj = Ticker(
        ticker=details.ticker,
        name=details.name,
        market=details.market,
        locale=details.locale,
        primary_exchange=details.primary_exchange,
        type=details.type,
        active=details.active,
        currency_name=details.currency_name,
        cik=details.cik,
        market_cap=int(details.market_cap) if details.market_cap is not None else None,
      )
      metadata_list.append(ticker_obj)

    except HTTPError as e:
      if e.response.status_code == 404:
        logging.warning(f"Ticker {ticker_symbol} not found on Polygon.io.")
      else:
        logging.error(
          f"HTTP error fetching metadata for {ticker_symbol} from Polygon: {e}",
          exc_info=True,
        )
    except ValidationError as e:
      logging.warning(
        f"Skipping Polygon metadata for {ticker_symbol} due to validation error: {e}"
      )
    except Exception as e:
      logging.error(
        f"An unexpected error occurred fetching metadata for {ticker_symbol}: {e}",
        exc_info=True,
      )

    # Apply delay between requests, but not after the last one
    if i < len(tickers_param) - 1:
      time.sleep(delay)

  return metadata_list


# --- Public Provider Class ---


class PolygonProvider:
  def __init__(self, api_key: str):
    if not api_key:
      raise ValueError("Polygon provider requires an API key.")

    client = RESTClient(api_key, retries=_MAX_RETRIES)

    self._capabilities = {
      TickersFetcher: functools.partial(_get_tickers_impl, client=client),
      CandlesFetcher: functools.partial(_get_candles_impl, client=client),
      OptionableFetcher: functools.partial(_get_optionable_tickers_impl, client=client),
      MetadataFetcher: functools.partial(_get_ticker_metadata_impl, client=client),
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
