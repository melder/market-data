# main.py
from provider import Provider


def main():
  """
  Client code that now uses the facade to get data from yfinance.
  """
  # 1. Create an instance of the Provider for yfinance (no API key needed).
  data_provider = Provider(provider="yfinance")

  # 2. Call fetch_candles with parameters suited for yfinance.
  print("Fetching daily candles for MSFT via the facade (yfinance)...")
  candles = data_provider.fetch_candles(
    ticker="AAPL", start_date="2025-07-01", end_date="2025-07-23", interval="1d"
  )

  # 3. Display the results.
  if candles:
    print(f"Successfully fetched {len(candles)} candles.")
    for candle in candles:
      print(f"{candle}")
  else:
    print("Could not fetch candle data.")


if __name__ == "__main__":
  main()
