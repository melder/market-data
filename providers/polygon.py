# providers/polygon.py
import time

from polygon import RESTClient
from requests.exceptions import HTTPError

MAX_LIMIT = 50000
# Constants for fetching tickers to avoid magic numbers
TICKERS_PAGE_LIMIT = 1000
RATE_LIMIT_PAUSE_SECONDS = 12


def get_candles(
  api_key: str,
  ticker: str,
  from_date: str,
  to_date: str,
  timespan: str = "day",
  multiplier: int = 1,
  limit: int = MAX_LIMIT,
) -> list[dict]:
  """
  Fetches candle data specifically from the Polygon.io API.
  This function is tailored to Polygon's needs.
  """
  try:
    client = RESTClient(api_key)
    resp = client.get_aggs(
      ticker=ticker,
      multiplier=multiplier,
      timespan=timespan,
      from_=from_date,
      to=to_date,
      limit=limit,
    )

    return [
      {
        "open": agg.open,
        "high": agg.high,
        "low": agg.low,
        "close": agg.close,
        "volume": agg.volume,
        "timestamp": agg.timestamp,
      }
      for agg in resp
    ]
  except Exception as e:
    print(f"An error occurred with Polygon.io: {e}")
    return []


def get_tickers(api_key: str) -> list[dict]:
  """
  Fetches all available tickers from Polygon.io using the paginated iterator.
  It pauses between pages to respect the free tier's 5 calls/minute rate limit.
  """
  client = RESTClient(api_key)
  all_tickers = []
  tickers_processed_since_pause = 0

  try:
    # client.list_tickers() returns an iterator that handles pagination automatically.
    # The SDK will fetch new pages as needed when the loop continues.
    for t in client.list_tickers(limit=TICKERS_PAGE_LIMIT, market="stocks"):
      all_tickers.append(
        {
          "ticker": t.ticker,
          "name": t.name,
          "market": t.market,
          "locale": t.locale,
          "primary_exchange": t.primary_exchange,
          "type": t.type,
          "active": t.active,
          "currency_name": t.currency_name,
          "cik": t.cik,
          "last_updated_utc": t.last_updated_utc,
        }
      )
      tickers_processed_since_pause += 1

      # After processing a full page of results, pause before the iterator
      # makes the next underlying API call.
      if tickers_processed_since_pause % TICKERS_PAGE_LIMIT == 0:
        print(
          f"Processed {len(all_tickers)} tickers. Pausing for {RATE_LIMIT_PAUSE_SECONDS} seconds..."
        )
        time.sleep(RATE_LIMIT_PAUSE_SECONDS)
        tickers_processed_since_pause = 0

  except HTTPError as e:
    # This will catch rate limit errors (429) if our proactive pause isn't enough.
    print(f"An HTTP error occurred while fetching tickers from Polygon.io: {e}")
    return []
  except Exception as e:
    # Catches other unexpected issues.
    print(f"An unexpected error occurred while fetching tickers from Polygon.io: {e}")
    return []

  print(f"Finished fetching. Total tickers found: {len(all_tickers)}")
  return all_tickers
