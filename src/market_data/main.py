from __future__ import annotations

import functools
import logging
import sys
from datetime import date

import click
from dotenv import load_dotenv

from market_data.factory import ProviderFactory
from market_data.interfaces import CandlesFetcher, TickersFetcher
from market_data.utils.savers import save_to_csv

# --- Setup ---
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  stream=sys.stdout,
)

# --- Error Handling Decorator ---


def cli_error_handler(func):
  """Decorator to handle common CLI errors, log them, and exit."""

  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except (ValueError, TypeError) as e:
      logging.error(f"Error: {e}")
      sys.exit(1)
    except Exception as e:
      logging.error(f"An unexpected error occurred: {e}", exc_info=True)
      sys.exit(1)

  return wrapper


# --- Private Helper ---


def _get_fetcher(provider_name: str, interface_class: type):
  """Helper to create a provider and get a specific fetcher component."""
  factory = ProviderFactory()
  data_provider = factory.create(provider_name)

  if not data_provider.supports(interface_class):
    capability_name = interface_class.__name__.replace("Fetcher", "")
    raise TypeError(
      f"Provider '{provider_name}' does not support fetching {capability_name}s."
    )

  return data_provider.get_fetcher(interface_class)


# --- CLI Commands ---


@click.group()
def cli():
  """A CLI for fetching financial market data."""
  load_dotenv()


@cli.command()
@click.option(
  "--provider", required=True, help="The data provider to use (e.g., yfinance)."
)
@click.option(
  "--exchange", help="The exchange to filter by (for providers that support it)."
)
@cli_error_handler  # Apply the decorator
def fetch_tickers(provider, exchange):
  """Fetch a list of tickers from a provider."""
  logging.info(f"Executing 'fetch-tickers' for provider: {provider}")

  get_tickers_func = _get_fetcher(provider, TickersFetcher)
  tickers = get_tickers_func(exchange=exchange)

  if not tickers:
    logging.warning("No tickers were fetched.")
    return

  filename = f"{provider}_{exchange or 'all'}_tickers.csv"
  logging.info(f"Saving {len(tickers)} tickers to {filename}...")
  save_to_csv([t.model_dump() for t in tickers], filename)


@cli.command()
@click.option("--provider", required=True, help="The data provider to use.")
@click.option("--ticker", required=True, help="The stock ticker symbol (e.g., AAPL).")
@click.option("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
@click.option(
  "--to-date", default=date.today().isoformat(), help="End date (YYYY-MM-DD)."
)
@click.option("--timespan", default="day", help="e.g., day, hour, minute.")
@click.option("--multiplier", type=int, default=1, help="Multiplier for the timespan.")
@cli_error_handler  # Apply the decorator
def fetch_candles(provider, ticker, from_date, to_date, timespan, multiplier):
  """Fetch candle (OHLCV) data for a ticker."""
  logging.info(f"Executing 'fetch-candles' for {ticker} on provider: {provider}")

  get_candles_func = _get_fetcher(provider, CandlesFetcher)
  candles = get_candles_func(
    ticker=ticker,
    from_date=from_date,
    to_date=to_date,
    timespan=timespan,
    multiplier=multiplier,
  )

  if not candles:
    logging.warning("No candle data was fetched.")
    return

  filename = f"{provider}_{ticker}_candles_{from_date}_to_{to_date}.csv"
  logging.info(f"Saving {len(candles)} candles to {filename}...")
  save_to_csv([c.model_dump() for c in candles], filename)


if __name__ == "__main__":
  cli()
