from __future__ import annotations

import functools
import logging
import sys
from datetime import date

import click
from dotenv import load_dotenv

from market_data.factory import ProviderFactory
from market_data.interfaces import (
  CandlesFetcher,
  MetadataFetcher,
  OptionableFetcher,
  TickersFetcher,
)
from market_data.utils.savers import save_to_csv

# --- Setup ---
logging.basicConfig(
  level=logging.DEBUG,
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


def _get_fetcher(provider_name: str, interface_class: type) -> callable:
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


@cli.command()
@click.option(
  "--provider", required=True, help="The data provider to use (e.g., yfinance)."
)
@click.option(
  "--ticker",
  "tickers",
  multiple=True,
  help="Ticker symbol(s). If not provided, fetches metadata for all available tickers.",
)
@click.option(
  "--chunk-size",
  type=int,
  default=50,
  show_default=True,
  help="Number of tickers to fetch per request batch.",
)
@click.option(
  "--delay",
  type=float,
  default=None,
  help="Delay in seconds between batch requests. Defaults to provider-specific value.",
)
@cli_error_handler
def fetch_metadata(
  provider: str, tickers: tuple[str, ...], chunk_size: int, delay: float
) -> None:
  """Fetch metadata such as market cap for one or more tickers."""
  logging.info(f"Executing 'fetch-metadata' for provider: {provider}")

  metadata_fetcher = _get_fetcher(provider, MetadataFetcher)

  symbols_to_fetch: list[str] = []
  if tickers:
    # If tickers are provided, normalize them
    for value in tickers:
      symbols_to_fetch.extend(
        segment.strip().upper() for segment in value.split(",") if segment.strip()
      )
  else:
    # If no tickers are provided, fetch all tickers from the provider
    logging.warning(
      f"No tickers provided. Fetching all available tickers from '{provider}' first. This may be very slow."
    )
    tickers_fetcher = _get_fetcher(provider, TickersFetcher)
    all_provider_tickers = tickers_fetcher(exchange=None)
    symbols_to_fetch = [t.ticker for t in all_provider_tickers if t.ticker]

  if not symbols_to_fetch:
    logging.warning("No valid ticker symbols were provided or found.")
    return

  unique_tickers = list(dict.fromkeys(symbols_to_fetch))
  if not unique_tickers:
    logging.warning("No unique tickers remained after normalization.")
    return

  fetcher_kwargs = {"tickers": unique_tickers, "chunk_size": chunk_size}
  if delay is not None:
    fetcher_kwargs["delay"] = delay

  metadata = metadata_fetcher(**fetcher_kwargs)

  if not metadata:
    logging.warning("No metadata was fetched.")
    return

  suffix = (
    "all"
    if not tickers
    else (
      unique_tickers[0].lower()
      if len(unique_tickers) == 1
      else f"{len(unique_tickers)}"
    )
  )
  filename = f"{provider}_{suffix}_metadata.csv"
  logging.info(f"Saving metadata for {len(metadata)} tickers to {filename}...")
  save_to_csv([item.model_dump() for item in metadata], filename)


@cli.command()
@click.option(
  "--provider", required=True, help="The data provider to use (e.g., yfinance)."
)
@click.option(
  "--exchange",
  default=None,
  help="The exchange to filter by (for providers that support it).",
)
@click.option(
  "--limit",
  type=int,
  default=0,
  show_default=True,
  help="Maximum number of tickers to enrich (0 means all).",
)
@click.option(
  "--chunk-size",
  type=int,
  default=50,
  show_default=True,
  help="Number of tickers to fetch per request batch.",
)
@click.option(
  "--delay",
  type=float,
  default=None,
  help="Delay in seconds between batch requests. Defaults to provider-specific value.",
)
@cli_error_handler
def fetch_tickers_metadata(
  provider: str, exchange: str | None, limit: int, chunk_size: int, delay: float
) -> None:
  """Fetch tickers, then enrich them with metadata in one pass."""
  logging.info(f"Executing 'fetch-tickers-metadata' for provider: {provider}")

  tickers_fetcher = _get_fetcher(provider, TickersFetcher)
  metadata_fetcher = _get_fetcher(provider, MetadataFetcher)

  tickers = tickers_fetcher(exchange=exchange)
  if not tickers:
    logging.warning("No tickers were fetched.")
    return

  symbol_order: list[str] = []
  base_map: dict[str, dict] = {}
  for ticker in tickers:
    if limit and len(symbol_order) >= limit:
      break
    symbol = ticker.ticker.upper() if ticker.ticker else None
    if not symbol or symbol in base_map:
      continue
    symbol_order.append(symbol)
    base_map[symbol] = ticker.model_dump()

  if not symbol_order:
    logging.warning("Fetched tickers but no symbols were usable after normalization.")
    return

  fetcher_kwargs = {"tickers": symbol_order, "chunk_size": chunk_size}
  if delay is not None:
    fetcher_kwargs["delay"] = delay

  metadata = metadata_fetcher(**fetcher_kwargs)
  if not metadata:
    logging.warning("No metadata was fetched for the retrieved tickers.")
    return

  metadata_map = {item.ticker.upper(): item for item in metadata if item.ticker}
  missing = [symbol for symbol in symbol_order if symbol not in metadata_map]
  if missing:
    logging.warning(
      "Metadata missing for tickers: %s",
      ", ".join(missing[:10]) + ("..." if len(missing) > 10 else ""),
    )

  rows: list[dict] = []
  for symbol in symbol_order:
    base = base_map.get(symbol)
    if not base:
      continue
    enriched = base.copy()
    enriched_meta = metadata_map.get(symbol)
    if enriched_meta:
      meta_payload = enriched_meta.model_dump()
      for key, value in meta_payload.items():
        if value is not None:
          enriched[key] = value
    rows.append(enriched)

  if not rows:
    logging.warning("No rows remained after combining ticker metadata.")
    return

  filename = f"{provider}_{(exchange or 'all').lower()}_tickers_metadata.csv"
  logging.info(
    "Saving metadata-enriched tickers (%d rows) to %s...",
    len(rows),
    filename,
  )
  save_to_csv(rows, filename)


@cli.command()
@click.option(
  "--provider", required=True, help="The data provider to use (e.g., cboe, yfinance)."
)
@click.option(
  "--type",
  "option_type",
  default="all",
  help="Type of options symbols: all, weeklies, quarterlies (CBOE only).",
)
@click.option(
  "--exchange", help="The exchange to filter by (for providers that support it)."
)
@click.option(
  "--max-tickers",
  type=int,
  help="Maximum number of tickers to check (for testing with yfinance).",
)
@click.option(
  "--delay",
  type=float,
  default=None,
  help="Delay in seconds between requests. Defaults to provider-specific value.",
)
@cli_error_handler
def fetch_optionable_tickers(
  provider: str, option_type: str, exchange: str, max_tickers: int, delay: float
) -> None:
  """Fetch a list of optionable tickers from a provider."""
  logging.info(f"Executing 'fetch-optionable-tickers' for provider: {provider}")

  get_optionable_func = _get_fetcher(provider, OptionableFetcher)

  # Build kwargs based on provider
  kwargs = {"type": option_type}
  if exchange:
    kwargs["exchange"] = exchange
  if max_tickers:
    kwargs["max_tickers"] = max_tickers
  if delay is not None:
    kwargs["delay"] = delay

  tickers = get_optionable_func(**kwargs)

  if not tickers:
    logging.warning("No optionable tickers were fetched.")
    return

  suffix = f"_{max_tickers}" if max_tickers else ""
  filename = f"{provider}_{option_type}_optionable_tickers{suffix}.csv"
  logging.info(f"Saving {len(tickers)} optionable tickers to {filename}...")
  save_to_csv([t.model_dump() for t in tickers], filename)


if __name__ == "__main__":
  cli()
