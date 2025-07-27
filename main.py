# main.py
import argparse
import logging
import os
import sys
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict
from datetime import date
from functools import partial

from dotenv import load_dotenv

from models import Candle, Ticker
from provider import Provider
from utils.savers import save_to_csv

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
  """
  Retrieves the API key for a given provider from environment variables.

  Args:
      provider_name: The name of the provider (e.g., 'polygon').

  Returns:
      The API key string, or None if not required.

  Raises:
      APIKeyNotFoundError: If a required API key is not set.
  """
  key_name_map = {
    "polygon": "POLYGON_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
  }
  # yfinance does not require a key
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


def handle_fetch_tickers(args: argparse.Namespace) -> None:
  """
  Handles the 'fetch-tickers' command logic.
  Fetches tickers and saves them to CSV files.
  """
  provider_name = args.provider
  logging.info(f"Executing 'fetch-tickers' for provider: {provider_name}")
  if args.exchange:
    logging.info(f"Filtering by exchange: {args.exchange}")

  try:
    api_key = get_api_key(provider_name)
    data_provider = Provider(provider=provider_name, api_key=api_key)

    # If yfinance is chosen without a specific exchange, fetch all defaults.
    if provider_name == "yfinance" and not args.exchange:
      logging.info("Defaulting to all exchanges for yfinance: [nasdaq, other]")
      exchanges_to_fetch = ["nasdaq", "other"]
      for exchange in exchanges_to_fetch:
        logging.info(f"--- Fetching yfinance tickers for exchange: {exchange} ---")
        tickers = data_provider.fetch_tickers(exchange=exchange)
        if not tickers:
          logging.warning(f"No tickers were fetched for exchange: {exchange}")
          continue
        save_fetched_tickers(tickers, provider_name, exchange=exchange)
    else:
      # Standard logic for a single fetch operation.
      provider_kwargs = {}
      if args.exchange:
        provider_kwargs["exchange"] = args.exchange

      tickers = data_provider.fetch_tickers(**provider_kwargs)
      if not tickers:
        logging.warning("No tickers were fetched.")
        return

      save_fetched_tickers(tickers, provider_name, exchange=args.exchange)

  except (APIKeyNotFoundError, ValueError) as e:
    logging.error(e)
    sys.exit(1)
  except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1)


def handle_fetch_candles(args: argparse.Namespace) -> None:
  """
  Handles the 'fetch-candles' command logic.
  Fetches candle data and saves it to a CSV file.
  """
  provider_name = args.provider
  logging.info(
    f"Executing 'fetch-candles' for {args.ticker} on provider: {provider_name}"
  )

  try:
    api_key = get_api_key(provider_name)
    data_provider = Provider(provider=provider_name, api_key=api_key)

    # Collect keyword arguments for the provider's get_candles function
    candle_kwargs = {
      "ticker": args.ticker,
      "from_date": args.from_date,
      "to_date": args.to_date,
      "timespan": args.timespan,
      "multiplier": args.multiplier,
    }

    candles: list[Candle] = data_provider.fetch_candles(**candle_kwargs)

    if not candles:
      logging.warning("No candle data was fetched.")
      return

    filename = (
      f"{provider_name}_{args.ticker}_candles_{args.from_date}_to_{args.to_date}.csv"
    )
    logging.info(f"Saving {len(candles)} candles to {filename}...")
    save_to_csv([asdict(c) for c in candles], filename)

  except (APIKeyNotFoundError, ValueError) as e:
    logging.error(e)
    sys.exit(1)
  except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1)


def _save_polygon_tickers(tickers: list[Ticker]) -> None:
  """Saves Polygon tickers, grouped by type."""
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
  """Saves Alpha Vantage tickers to a specifically named file."""
  filename = "alpha_vantage_tickers_active.csv"
  logging.info(f"Saving {len(tickers)} active tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def _save_yfinance_tickers(tickers: list[Ticker], exchange: str | None) -> None:
  """Saves Yahoo Finance tickers to exchange-specific files."""
  if exchange == "nasdaq":
    filename = "yfinance_nasdaq_tickers_all.csv"
  elif exchange == "other":
    filename = "yfinance_other_exchanges_tickers_all.csv"
  else:
    # This case handles yfinance without an exchange, or an unknown one.
    exchange_part = f"_{exchange}" if exchange else ""
    filename = f"yfinance{exchange_part}_tickers.csv"
  logging.info(f"Saving {len(tickers)} tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def _save_generic_tickers(tickers: list[Ticker], provider_name: str) -> None:
  """Saves tickers to a generically named file for a given provider."""
  filename = f"{provider_name}_tickers.csv"
  logging.info(f"Saving {len(tickers)} tickers to {filename}...")
  save_to_csv([asdict(t) for t in tickers], filename)


def save_fetched_tickers(
  tickers: list[Ticker], provider_name: str, exchange: str | None = None
) -> None:
  """
  Saves the fetched tickers to one or more CSV files by dispatching to a
  provider-specific saving function.
  """
  logging.info(f"Fetched {len(tickers)} tickers successfully.")

  # A dispatch table mapping provider names to their specific saving functions.
  saver_dispatch: dict[str, Callable[[list[Ticker]], None]] = {
    "polygon": _save_polygon_tickers,
    "alpha_vantage": _save_alpha_vantage_tickers,
    "yfinance": partial(_save_yfinance_tickers, exchange=exchange),
  }

  # Get the specific saver function, or use the generic one as a default.
  saver_func = saver_dispatch.get(provider_name)
  if saver_func:
    saver_func(tickers)
  else:
    _save_generic_tickers(tickers, provider_name)


def create_parser() -> argparse.ArgumentParser:
  """Creates and configures the argument parser for the CLI."""
  parser = argparse.ArgumentParser(description="A CLI for fetching financial data.")
  subparsers = parser.add_subparsers(
    dest="command", required=True, help="Available commands"
  )

  # --- Parent parser for shared arguments ---
  # Use add_help=False to avoid conflicts with subcommand help messages.
  parent_parser = argparse.ArgumentParser(add_help=False)
  parent_parser.add_argument(
    "--provider",
    choices=["polygon", "alpha_vantage", "yfinance"],
    required=True,
    help="The data provider to use.",
  )

  # --- Parser for the "fetch-tickers" command ---
  parser_fetch_tickers = subparsers.add_parser(
    "fetch-tickers",
    help="Fetch a list of tickers from a provider.",
    parents=[parent_parser],
  )
  parser_fetch_tickers.add_argument(
    "--exchange",
    choices=["nasdaq", "other"],
    default=None,
    help="The exchange to filter by (used by yfinance provider).",
  )
  parser_fetch_tickers.set_defaults(func=handle_fetch_tickers)

  # --- Parser for the "fetch-candles" command ---
  parser_fetch_candles = subparsers.add_parser(
    "fetch-candles",
    help="Fetch candle (OHLCV) data for a ticker.",
    parents=[parent_parser],
  )
  parser_fetch_candles.add_argument(
    "--ticker", required=True, help="The stock ticker symbol (e.g., AAPL)."
  )
  parser_fetch_candles.add_argument(
    "--from-date", required=True, help="Start date in YYYY-MM-DD format."
  )
  parser_fetch_candles.add_argument(
    "--to-date",
    default=date.today().isoformat(),
    help="End date in YYYY-MM-DD format. Defaults to today.",
  )
  parser_fetch_candles.add_argument(
    "--timespan",
    default="day",
    help="The size of the time window (e.g., day, hour, minute).",
  )
  parser_fetch_candles.add_argument(
    "--multiplier", type=int, default=1, help="The multiplier for the timespan."
  )
  parser_fetch_candles.set_defaults(func=handle_fetch_candles)

  return parser


def main() -> None:
  """Main entry point for the data fetching application."""
  load_dotenv()

  parser = create_parser()
  args = parser.parse_args()
  args.func(args)


if __name__ == "__main__":
  main()
