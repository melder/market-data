# providers/polygon.py

from polygon import RESTClient

MAX_LIMIT = 50000


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
