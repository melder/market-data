from __future__ import annotations

import logging
from collections.abc import Callable
from io import StringIO
from typing import Any

import pandas as pd
import requests
from pydantic import ValidationError

from market_data.interfaces import OptionableFetcher
from market_data.models import Ticker
from market_data.utils.parsers import read_csv_with_conventions

# --- Module Constants ---
_CBOE_URLS = {
  "all": "https://www.cboe.com/us/options/symboldir/?download=csv",
  "weeklies": "https://www.cboe.com/us/options/symboldir/weeklys_options/?download=csv",
  "quarterlies": "https://www.cboe.com/us/options/symboldir/quarterlys_options/?download=csv",
}

_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36"
}

# --- Private Fetcher Implementation ---


def _get_optionable_tickers_impl(**kwargs: Any) -> list[Ticker]:
  """Fetches optionable ticker symbols from CBOE directory.

  Args:
    **kwargs: Keyword arguments including:
      type (str): Symbol type ('all', 'weeklies', 'quarterlies')

  Returns:
    List of Ticker objects with optionable=True

  Raises:
    requests.RequestException: If HTTP request fails
    ValueError: If CSV parsing fails
  """

  # Determine which CBOE endpoint to use
  symbol_type = kwargs.get("type", "all")
  url = _CBOE_URLS.get(symbol_type)

  if not url:
    logging.error(
      f"Invalid CBOE symbol type: {symbol_type}. Valid types: {list(_CBOE_URLS.keys())}"
    )
    return []

  logging.info(f"Fetching CBOE {symbol_type} optionable symbols from: {url}")

  try:
    response = requests.get(url, timeout=30, headers=_HEADERS)
    response.raise_for_status()

    # Parse CSV content directly from response
    csv_content = StringIO(response.text)
    df = read_csv_with_conventions(csv_content)

    if df.empty:
      logging.warning("CBOE returned empty CSV data")
      return []

    # Check for required columns
    if "Stock Symbol" not in df.columns:
      logging.error(
        f"CBOE CSV missing required 'Stock Symbol' column. Available columns: {list(df.columns)}"
      )
      return []

    logging.info(f"CBOE CSV loaded with {len(df)} rows and columns: {list(df.columns)}")

    valid_tickers = []
    for _, row in df.iterrows():
      try:
        # Map CBOE CSV columns to our Ticker model
        mapped_data = {
          "ticker": str(row.get("Stock Symbol", "")).strip(),
          "name": str(row.get("Company Name", "")).strip() or None,
          "active": True,
          "optionable": True,
        }

        # Skip empty or truly missing symbols (pandas NaN or empty strings)
        if pd.isna(row.get("Stock Symbol")) or not mapped_data["ticker"]:
          continue

        valid_tickers.append(Ticker.model_validate(mapped_data))

      except ValidationError as e:
        ticker_symbol = row.get("Stock Symbol")
        logging.warning(
          f"Skipping CBOE ticker '{ticker_symbol}' due to validation error: {e}"
        )
      except Exception as e:
        logging.warning(f"Error processing CBOE row: {e}")
        continue

    logging.info(
      f"Successfully parsed {len(valid_tickers)} optionable tickers from CBOE {symbol_type} data"
    )
    return valid_tickers

  except requests.exceptions.RequestException as e:
    logging.error(f"HTTP error fetching CBOE data: {e}", exc_info=True)
    return []
  except Exception as e:
    logging.error(f"Unexpected error processing CBOE data: {e}", exc_info=True)
    return []


# --- Public Provider Class ---


class CboeProvider:
  """CBOE (Chicago Board Options Exchange) data provider.

  Provides access to optionable ticker symbols from CBOE's public directory.
  Supports fetching all optionable symbols, weekly options, and quarterly options.

  This is a free-tier provider that doesn't require API keys.
  """

  def __init__(self):
    self._capabilities = {
      OptionableFetcher: _get_optionable_tickers_impl,
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type) -> Callable[..., list[Ticker]]:
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
