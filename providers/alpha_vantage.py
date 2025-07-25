# providers/alpha_vantage.py

from alpha_vantage.timeseries import TimeSeries


def get_candles(api_key: str, ticker: str, outputsize: str = "compact") -> list[dict]:
  """
  Fetches daily candle data specifically from the Alpha Vantage API.
  Note: The free tier has a strict rate limit.
  """
  try:
    ts = TimeSeries(key=api_key, output_format="json")
    data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize=outputsize)

    # Alpha Vantage returns a dictionary of dates. We must transform
    # it into a list of dictionaries, our standard format.
    candles = []
    for date, daily_data in data.items():
      candles.append(
        {
          "date": date,
          "open": float(daily_data["1. open"]),
          "high": float(daily_data["2. high"]),
          "low": float(daily_data["3. low"]),
          "close": float(daily_data["4. close"]),
          "volume": int(daily_data["6. volume"]),
        }
      )
    return candles

  except Exception as e:
    # Catch potential rate limit errors or other issues.
    print(f"An error occurred with Alpha Vantage: {e}")
    return []
