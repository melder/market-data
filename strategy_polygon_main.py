# main.py
import os

from candle_fetcher import CandleFetcher
from strategies.polygon_candles import PolygonCandleStrategy


def main():
  """
  Client code to fetch candle data using the new strategy pattern.
  """
  api_key = os.getenv("POLYGON_API_KEY", "YOUR_API_KEY")

  if api_key == "YOUR_API_KEY":
    print("ERROR: Please set your Polygon.io API key.")
    return

  # 1. Create the concrete strategy object with its configuration.
  # Here we are setting a custom limit of 100 for this instance.
  polygon_strategy = PolygonCandleStrategy(api_key=api_key, limit=100)

  # 2. Create the context and inject the strategy.
  fetcher = CandleFetcher(strategy=polygon_strategy)

  # 3. Use the context to fetch the candle data.
  print("Fetching daily candles for AAPL...")
  candles = fetcher.get_candle_data(
    ticker="AAPL", interval="day", from_date="2025-01-01", to_date="2025-07-22"
  )

  # 4. Display the results.
  if candles:
    print(f"Successfully fetched {len(candles)} candles.")
    # print("First candle data:")
    # The timestamp is a unix millisecond timestamp, we'll just print it
    for candle in candles:
      print(f"{candle}")
  else:
    print("Could not fetch candle data.")


if __name__ == "__main__":
  main()
