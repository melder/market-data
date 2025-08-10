import functools
import logging
import time
from typing import Any

from polygon import RESTClient
from requests.exceptions import HTTPError

from market_data.interfaces import CandlesFetcher, TickersFetcher
from market_data.models import Candle, Ticker

# --- Module-level Constants ---
_MAX_CANDLE_LIMIT = 50000
_TICKERS_PAGE_LIMIT = 1000
_RATE_LIMIT_PAUSE_SECONDS = 12

# --- Private Helper Functions for Data Mapping ---


def _map_api_agg_to_candle(agg: Any) -> Candle:
  """Maps a Polygon.io aggregate object to a Candle model."""
  return Candle(
    open=agg.open,
    high=agg.high,
    low=agg.low,
    close=agg.close,
    volume=agg.volume,
    timestamp=agg.timestamp,
  )


def _map_api_ticker_to_ticker(t: Any) -> Ticker:
  """Maps a Polygon.io ticker object to a Ticker model."""
  return Ticker(
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
    return [_map_api_agg_to_candle(agg) for agg in resp]
  except Exception as e:
    logging.error(f"Error with Polygon.io get_candles: {e}", exc_info=True)
    return []


def _get_tickers_impl(client: RESTClient, **kwargs: Any) -> list[Ticker]:
  """Fetches all available tickers from Polygon.io with rate-limiting."""
  all_tickers = []
  tickers_processed = 0
  try:
    for t in client.list_tickers(limit=_TICKERS_PAGE_LIMIT, market="stocks"):
      all_tickers.append(_map_api_ticker_to_ticker(t))
      tickers_processed += 1
      if tickers_processed % _TICKERS_PAGE_LIMIT == 0:
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

    # Use functools.partial to "pre-load" the client into the fetcher functions.
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
