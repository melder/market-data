from __future__ import annotations

import json
import logging
import time
from typing import Any

import pandas as pd
import yfinance as yf
from yfinance.data import YfData
from yfinance.scrapers import quote

try:
  from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - fallback for environments without curl_cffi
  import requests as curl_requests  # type: ignore[assignment]

from pydantic import ValidationError

from market_data.interfaces import (
  CandlesFetcher,
  MetadataFetcher,
  OptionableFetcher,
  TickersFetcher,
)
from market_data.models import Candle, Ticker
from market_data.utils.parsers import read_csv_with_conventions

# --- Module-level Constants ---
_YFINANCE_DEFAULT_DELAY = 2.0

# --- Ticker Fetching Logic ---

_YFINANCE_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
}

_YF_DATA_CLIENT: YfData | None = None


def _get_yf_data_client() -> YfData:
  global _YF_DATA_CLIENT
  if _YF_DATA_CLIENT is None:
    session = curl_requests.Session(impersonate="chrome")
    session.headers.update(_YFINANCE_HEADERS)
    _YF_DATA_CLIENT = YfData(session=session)
  return _YF_DATA_CLIENT


def _chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
  if chunk_size <= 0:
    chunk_size = 1
  return [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]


def _build_ticker_from_quote(raw_quote: dict[str, Any]) -> Ticker | None:
  symbol = raw_quote.get("symbol")
  if not symbol:
    return None

  ticker_payload = {
    "ticker": symbol,
    "name": raw_quote.get("longName") or raw_quote.get("shortName"),
    "active": True,
    "optionable": raw_quote.get("hasOptions"),
    "market_cap": raw_quote.get("marketCap"),
    "market": raw_quote.get("market"),
    "locale": raw_quote.get("region"),
    "type": raw_quote.get("quoteType"),
    "primary_exchange": raw_quote.get("fullExchangeName"),
    "currency_name": raw_quote.get("currency"),
  }

  try:
    return Ticker.model_validate(ticker_payload)
  except ValidationError as exc:
    logging.debug(
      "Skipping yfinance metadata for %s due to validation error: %s",
      symbol,
      exc,
    )
    return None


_EXCHANGE_SOURCES = {
  "nasdaq": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
    "ticker_col": "Symbol",
    "name_col": "Security Name",
  },
  "other": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
    "ticker_col": "ACT Symbol",
    "name_col": "Security Name",
  },
}


def _get_tickers_impl(**kwargs: Any) -> list[Ticker]:
  exchange = kwargs.get("exchange")

  if exchange:
    # If a specific exchange is requested, put it in a list to be processed.
    exchanges_to_fetch = [exchange]
  else:
    # If no exchange is specified, fetch from all configured sources.
    logging.info(
      f"No exchange specified for yfinance. Fetching from all sources: {list(_EXCHANGE_SOURCES.keys())}"
    )
    exchanges_to_fetch = _EXCHANGE_SOURCES.keys()

  all_tickers = []
  for exch_name in exchanges_to_fetch:
    source_info = _EXCHANGE_SOURCES.get(exch_name)
    if not source_info:
      logging.warning(
        f"yfinance exchange '{exch_name}' not found in configuration. Skipping."
      )
      continue

    try:
      logging.info(f"Fetching yfinance tickers for exchange: {exch_name}")
      df = read_csv_with_conventions(source_info["url"], sep="|")
      tickers = _process_ticker_dataframe(
        df, source_info["ticker_col"], source_info["name_col"]
      )
      all_tickers.extend(tickers)
    except Exception as e:
      logging.error(
        f"Failed to fetch tickers for yfinance exchange '{exch_name}': {e}",
        exc_info=True,
      )
      # Continue to the next exchange even if one fails
      continue

  return all_tickers


def _process_ticker_dataframe(
  df: pd.DataFrame, ticker_col: str, name_col: str
) -> list[Ticker]:
  if df.empty:
    return []

  # This line removes the footer from the raw data.
  df = df.iloc[:-1]

  df = df.rename(columns={ticker_col: "ticker", name_col: "name"})
  df["active"] = (
    (df["Financial Status"] == "N") if "Financial Status" in df.columns else True
  )

  valid_tickers = []
  for row in df.itertuples(index=False):
    try:
      # Convert row to dict and validate with Pydantic
      valid_tickers.append(Ticker.model_validate(row._asdict()))
    except ValidationError as e:
      ticker_symbol = getattr(row, "ticker", "UNKNOWN")
      logging.warning(
        f"Skipping yfinance ticker '{ticker_symbol}' due to validation error: {e}"
      )
  return valid_tickers


# --- Optionable Tickers Fetching Logic ---


def _check_ticker_has_options(ticker_symbol: str) -> bool:
  """Check if a ticker has options available using yfinance.

  Args:
    ticker_symbol: The stock ticker symbol to check

  Returns:
    True if the ticker has options, False otherwise
  """
  try:
    ticker = yf.Ticker(ticker_symbol)
    options = ticker.options
    return len(options) > 0
  except Exception as e:
    logging.debug(f"Error checking options for {ticker_symbol}: {e}")
    return False


def _get_optionable_tickers_impl(**kwargs: Any) -> list[Ticker]:
  """Get optionable tickers by checking each ticker from the regular ticker list.

  This implementation:
  1. Gets all tickers using the existing ticker fetching logic
  2. Checks each ticker individually for options availability
  3. Applies rate limiting to avoid Yahoo Finance throttling
  4. Returns only tickers that have options available

  Args:
    **kwargs: Same arguments as _get_tickers_impl (exchange, etc.)
    plus optional:
      - max_tickers: Maximum number of tickers to check (for testing)
      - delay: Delay in seconds between requests

  Returns:
    List of Ticker objects with optionable=True
  """
  # Get configuration
  max_tickers = kwargs.get("max_tickers")
  delay = kwargs.get("delay", _YFINANCE_DEFAULT_DELAY)

  logging.info("Starting optionable tickers fetch using yfinance")
  logging.info(f"Rate limiting: {delay} seconds between requests")

  # Get all tickers first
  all_tickers = _get_tickers_impl(**kwargs)

  if max_tickers:
    all_tickers = all_tickers[:max_tickers]
    logging.info(f"Limited to first {max_tickers} tickers for testing")

  logging.info(f"Checking {len(all_tickers)} tickers for options availability")

  optionable_tickers = []
  checked_count = 0

  for ticker in all_tickers:
    checked_count += 1

    if checked_count % 100 == 0:
      logging.info(
        f"Progress: {checked_count}/{len(all_tickers)} tickers checked, {len(optionable_tickers)} optionable found"
      )

    try:
      if _check_ticker_has_options(ticker.ticker):
        # Create a new ticker object with optionable=True
        optionable_ticker = ticker.model_copy()
        optionable_ticker.optionable = True
        optionable_tickers.append(optionable_ticker)
        logging.debug(f"Found optionable ticker: {ticker.ticker}")

    except Exception as e:
      logging.warning(f"Error processing ticker {ticker.ticker}: {e}")
      continue

    # Rate limiting to avoid getting blocked
    if checked_count < len(all_tickers):  # Don't sleep after the last request
      time.sleep(delay)

  logging.info(
    f"Completed optionable tickers scan: {len(optionable_tickers)} optionable tickers found out of {checked_count} checked"
  )
  return optionable_tickers


# --- Metadata Fetching Logic ---


def _get_ticker_metadata_impl(**kwargs: Any) -> list[Ticker]:
  tickers_param = kwargs.get("tickers")
  if not tickers_param:
    logging.warning("No tickers provided for yfinance metadata fetch.")
    return []

  if isinstance(tickers_param, str):
    raw_tickers = [segment.strip() for segment in tickers_param.split(",")]
  else:
    raw_tickers = []
    for value in tickers_param:
      if isinstance(value, str):
        raw_tickers.extend(segment.strip() for segment in value.split(","))

  tickers_normalized = [ticker for ticker in (t.upper() for t in raw_tickers) if ticker]
  if not tickers_normalized:
    logging.warning("yfinance metadata received tickers but all were empty after normalization.")
    return []

  unique_tickers = list(dict.fromkeys(tickers_normalized))

  try:
    chunk_size = int(kwargs.get("chunk_size", 50))
  except (TypeError, ValueError):
    chunk_size = 50
  if chunk_size <= 0:
    chunk_size = 1

  delay = kwargs.get("delay", _YFINANCE_DEFAULT_DELAY)

  logging.info(
    "Fetching yfinance metadata for %d tickers (chunk size %d, delay %.2fs)",
    len(unique_tickers),
    chunk_size,
    delay,
  )

  quote_url = f"{quote._QUERY1_URL_}/v7/finance/quote"
  data_client = _get_yf_data_client()
  metadata_map: dict[str, Ticker] = {}

  chunks = _chunk_symbols(unique_tickers, chunk_size)
  for index, chunk in enumerate(chunks, start=1):
    params = {"symbols": ",".join(chunk)}
    try:
      payload = data_client.get_raw_json(quote_url, params=params)
      logging.debug("yfinance metadata payload: %s", json.dumps(payload, indent=2))
    except Exception as exc:  # noqa: BLE001 - propagate log but continue
      logging.error("Failed to fetch yfinance metadata for chunk %s: %s", chunk, exc, exc_info=True)
      continue

    quotes = payload.get("quoteResponse", {}).get("result", [])
    if not quotes:
      logging.debug("yfinance metadata chunk %s returned no results", chunk)
      continue

    for raw_quote in quotes:
      ticker_obj = _build_ticker_from_quote(raw_quote)
      if ticker_obj:
        metadata_map[ticker_obj.ticker] = ticker_obj

    if delay and index < len(chunks):
      time.sleep(delay)

  missing = [ticker for ticker in unique_tickers if ticker not in metadata_map]
  if missing:
    logging.warning(
      "yfinance metadata missing for tickers: %s",
      ", ".join(missing),
    )

  return [metadata_map[ticker] for ticker in unique_tickers if ticker in metadata_map]


# --- Candle Fetching Logic ---


def _map_to_yfinance_interval(timespan: str, multiplier: int) -> str:
  span_map = {"minute": "m", "hour": "h", "day": "d", "week": "wk", "month": "mo"}
  interval_char = span_map.get(timespan)
  if not interval_char:
    raise ValueError(f"Unsupported timespan for yfinance: '{timespan}'")
  return f"{multiplier}{interval_char}"


def _get_candles_impl(**kwargs: Any) -> list[Candle]:
  try:
    interval = _map_to_yfinance_interval(kwargs["timespan"], kwargs["multiplier"])
    stock = yf.Ticker(kwargs["ticker"])
    df = stock.history(
      start=kwargs["from_date"], end=kwargs["to_date"], interval=interval
    )
    if df.empty:
      return []

    valid_candles = []
    for row in df.itertuples():
      try:
        candle_data = {
          "open": row.Open,
          "high": row.High,
          "low": row.Low,
          "close": row.Close,
          "volume": row.Volume,
          "timestamp": int(row.Index.timestamp() * 1000),
        }
        valid_candles.append(Candle.model_validate(candle_data))
      except (ValidationError, AttributeError) as e:
        logging.warning(
          f"Skipping yfinance candle for {kwargs['ticker']} due to validation error: {e}"
        )
    return valid_candles
  except Exception as e:
    logging.error(f"Error with yfinance get_candles: {e}", exc_info=True)
    return []


# --- Public Provider Class ---


class YFinanceProvider:
  def __init__(self):
    self._capabilities = {
      TickersFetcher: _get_tickers_impl,
      CandlesFetcher: _get_candles_impl,
      OptionableFetcher: _get_optionable_tickers_impl,
      MetadataFetcher: _get_ticker_metadata_impl,
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]