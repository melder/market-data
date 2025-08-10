from __future__ import annotations

import json
import logging
from typing import Any

import requests
from pydantic import ValidationError

from market_data.interfaces import TickersFetcher
from market_data.models import Ticker

# --- Module Constants ---
_SEC_URL = "https://www.sec.gov/files/company_tickers.json"
_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0"
}

# --- Private Fetcher Implementation ---


def _get_tickers_impl(**kwargs: Any) -> list[Ticker]:
  """Fetches company ticker data from the SEC's public JSON file."""
  logging.info(f"Downloading ticker list from SEC: {_SEC_URL}")

  try:
    # Pass the headers with the request.
    response = requests.get(_SEC_URL, timeout=30, headers=_HEADERS)
    response.raise_for_status()
    data = response.json()
  except requests.exceptions.RequestException as e:
    logging.error(f"HTTP error fetching SEC tickers: {e}", exc_info=True)
    return []
  except json.JSONDecodeError as e:
    logging.error(f"Failed to parse JSON from SEC response: {e}", exc_info=True)
    return []

  valid_tickers = []
  for company_data in data.values():
    try:
      mapped_data = {
        "ticker": company_data.get("ticker"),
        "name": company_data.get("title"),
        "cik": str(company_data.get("cik_str")),
        "active": True,
      }
      valid_tickers.append(Ticker.model_validate(mapped_data))
    except ValidationError as e:
      ticker_symbol = company_data.get("ticker", "UNKNOWN")
      logging.warning(
        f"Skipping SEC ticker '{ticker_symbol}' due to validation error: {e}"
      )

  logging.info(f"Successfully parsed {len(valid_tickers)} tickers from SEC data.")
  return valid_tickers


# --- Public Provider Class ---


class SecProvider:
  def __init__(self):
    self._capabilities = {
      TickersFetcher: _get_tickers_impl,
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
