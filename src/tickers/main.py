# main.py
import logging
import os
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import date
from functools import partial

import click
from dotenv import load_dotenv

from tickers.models import Candle, Ticker
from tickers.provider import Provider
from tickers.utils.savers import save_to_csv

# --- Setup Logging ---
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  stream=sys.stdout,
)


class APIKeyNotFoundError(Exception):
  """Custom exception for missing API keys."""

  pass


def get_api_key(provider_name: str) -> str | None:
  key_name_map = {
    "polygon": "POLYGON_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
  }
  if provider_name not in key_name_map:
    return None
  key_env_var = key_name_map[provider_name]
  api_key = os.getenv(key_env_var)
  if not api_key:
    raise APIKeyNotFoundError(
      f"Required API key '{key_env_var}' for provider '{provider_name}' not found. "
      "Please set it in your .env file or as an environment variable."
    )
  return api_key


def _save_polygon_tickers(tickers: list[Ticker]) -> None:
  tickers_by_type = defaultdict(list)
  for ticker in tickers:
    ticker_type = ticker.type or "UNKNOWN"
    tickers_by_type[ticker_type].append(ticker)
  for ticker_type, ticker_list in tickers_by_type.items():
    filename = f"polygon_tickers_{ticker_type}.csv"
    logging.info(
      f"Saving {len(ticker_list)} tickers of type '{ticker_type}' to {filename}..."
    )
    save_to_csv([asdict(t) for t in ticker_list], filename)


def _save_alpha_vantage_tickers(tickers: list[Ticker]) -> None:
  filename = "alpha_vantage_tickers_active.csv"
  logging.info(f"Saving {len(tickers)} active tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def _save_yfinance_tickers(tickers: list[Ticker], exchange: str | None) -> None:
  if exchange == "nasdaq":
    filename = "yfinance_nasdaq_tickers_all.csv"
  elif exchange == "other":
    filename = "yfinance_other_exchanges_tickers_all.csv"
  else:
    exchange_part = f"_{exchange}" if exchange else ""
    filename = f"yfinance{exchange_part}_tickers.csv"
  logging.info(f"Saving {len(tickers)} tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def _save_generic_tickers(tickers: list[Ticker], provider_name: str) -> None:
  filename = f"{provider_name}_tickers.csv"
  logging.info(f"Saving {len(tickers)} tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def save_fetched_tickers(
  tickers: list[Ticker], provider_name: str, exchange: str | None = None
) -> None:
  logging.info(f"Fetched {len(tickers)} tickers successfully.")
  saver_dispatch: dict[str, callable] = {
    "polygon": _save_polygon_tickers,
    "alpha_vantage": _save_alpha_vantage_tickers,
    "yfinance": partial(_save_yfinance_tickers, exchange=exchange),
  }
  saver_func = saver_dispatch.get(provider_name)
  if saver_func:
    saver_func(tickers)
  else:
    _save_generic_tickers(tickers, provider_name)


@click.group()
def cli():
  """A CLI for fetching financial data."""
  load_dotenv()


@cli.command()
@click.option(
  "--provider",
  type=click.Choice(["polygon", "alpha_vantage", "yfinance"]),
  required=True,
  help="The data provider to use.",
)
@click.option(
  "--exchange",
  type=click.Choice(["nasdaq", "other"]),
  default=None,
  help="The exchange to filter by (used by yfinance provider).",
)
def fetch_tickers(provider, exchange):
  """Fetch a list of tickers from a provider."""
  logging.info(f"Executing 'fetch-tickers' for provider: {provider}")
  if exchange:
    logging.info(f"Filtering by exchange: {exchange}")

  try:
    api_key = get_api_key(provider)
    data_provider = Provider(provider=provider, api_key=api_key)

    if provider == "yfinance" and not exchange:
      logging.info("Defaulting to all exchanges for yfinance: [nasdaq, other]")
      exchanges_to_fetch = ["nasdaq", "other"]
      for exch in exchanges_to_fetch:
        logging.info(f"--- Fetching yfinance tickers for exchange: {exch} ---")
        tickers = data_provider.fetch_tickers(exchange=exch)
        if not tickers:
          logging.warning(f"No tickers were fetched for exchange: {exch}")
          continue
        save_fetched_tickers(tickers, provider, exchange=exch)
    else:
      provider_kwargs = {}
      if exchange:
        provider_kwargs["exchange"] = exchange
      tickers = data_provider.fetch_tickers(**provider_kwargs)
      if not tickers:
        logging.warning("No tickers were fetched.")
        return
      save_fetched_tickers(tickers, provider, exchange=exchange)

  except (APIKeyNotFoundError, ValueError) as e:
    logging.error(e)
    sys.exit(1)
  except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1)


@cli.command()
@click.option(
  "--provider",
  type=click.Choice(["polygon", "alpha_vantage", "yfinance"]),
  required=True,
  help="The data provider to use.",
)
@click.option("--ticker", required=True, help="The stock ticker symbol (e.g., AAPL).")
@click.option("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
@click.option(
  "--to-date",
  default=date.today().isoformat(),
  help="End date in YYYY-MM-DD format. Defaults to today.",
)
@click.option(
  "--timespan",
  default="day",
  help="The size of the time window (e.g., day, hour, minute).",
)
@click.option(
  "--multiplier", type=int, default=1, help="The multiplier for the timespan."
)
def fetch_candles(provider, ticker, from_date, to_date, timespan, multiplier):
  """Fetch candle (OHLCV) data for a ticker."""
  logging.info(f"Executing 'fetch-candles' for {ticker} on provider: {provider}")

  try:
    api_key = get_api_key(provider)
    data_provider = Provider(provider=provider, api_key=api_key)

    candle_kwargs = {
      "ticker": ticker,
      "from_date": from_date,
      "to_date": to_date,
      "timespan": timespan,
      "multiplier": multiplier,
    }

    candles: list[Candle] = data_provider.fetch_candles(**candle_kwargs)

    if not candles:
      logging.warning("No candle data was fetched.")
      return

    filename = f"{provider}_{ticker}_candles_{from_date}_to_{to_date}.csv"
    logging.info(f"Saving {len(candles)} candles to {filename}...")
    save_to_csv([asdict(c) for c in candles], filename)

  except (APIKeyNotFoundError, ValueError) as e:
    logging.error(e)
    sys.exit(1)
  except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1)


if __name__ == "__main__":
  cli()
