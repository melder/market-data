# main.py
import os

from provider import Provider


def main():
  """
  Client code that now uses the facade to get data from Alpha Vantage.
  """
  api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "YOUR_API_KEY")

  if api_key == "YOUR_API_KEY":
    print("ERROR: Please set your Alpha Vantage API key.")
    return

  # 1. Create an instance of the Provider, telling it to use 'alpha_vantage'.
  data_provider = Provider(provider="alpha_vantage", api_key=api_key)

  # 2. Call fetch_candles with parameters suited for Alpha Vantage.
  # The 'outputsize' argument will be passed through the facade.
  print("Fetching daily candles for IBM via the facade (Alpha Vantage)...")
  candles = data_provider.fetch_candles(
    ticker="IBM",
    outputsize="compact",  # 'compact' gets the last 100 data points
  )

  # 3. Display the results.
  if candles:
    print(f"Successfully fetched {len(candles)} candles.")
    print("Most recent candle data:")
    print(candles[0])  # Alpha Vantage data is sorted most recent first
  else:
    print("Could not fetch candle data.")


if __name__ == "__main__":
  main()
