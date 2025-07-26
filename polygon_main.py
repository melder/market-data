# main.py
import os
from collections import defaultdict

from provider import Provider  # Import the new Provider class
from utils.savers import save_to_csv


def main():
  """
  Client code that uses the Provider facade to fetch data.
  """
  api_key = os.getenv("POLYGON_API_KEY", "YOUR_API_KEY")

  if api_key == "YOUR_API_KEY":
    print("ERROR: Please set your Polygon.io API key.")
    return

  # 1. Create an instance of the Provider, telling it to use 'polygon'.
  data_provider = Provider(provider="polygon", api_key=api_key)

  print("Fetching tickers from Polygon.io...")
  tickers = data_provider.fetch_tickers()
  if tickers:
    # Group tickers by their 'type'
    tickers_by_type = defaultdict(list)
    for ticker in tickers:
      # Use 'UNKNOWN' for tickers that might not have a type specified.
      ticker_type = ticker.get("type", "UNKNOWN")
      tickers_by_type[ticker_type].append(ticker)

    # Save each group of tickers to a separate CSV file.
    for ticker_type, ticker_list in tickers_by_type.items():
      filename = f"polygon_tickers_{ticker_type}.csv"
      print(f"\nSaving {len(ticker_list)} tickers of type '{ticker_type}'...")
      save_to_csv(ticker_list, filename)
  else:
    print("No tickers were fetched.")

  # 2. Call the fetch_candles method with Polygon-specific arguments.
  # The facade handles passing these through to the correct function.
  # print("Fetching daily candles for AAPL via the facade...")
  # candles = data_provider.fetch_candles(
  #   ticker="AAPL",
  #   from_date="2025-01-01",
  #   to_date="2025-07-23",
  #   timespan="day",
  #   multiplier=1,
  # )

  # # 3. Display the results.
  # if candles:
  #   for candle in candles:
  #     print(f"{candle}")
  # else:
  #   print("Could not fetch candle data.")


if __name__ == "__main__":
  main()
