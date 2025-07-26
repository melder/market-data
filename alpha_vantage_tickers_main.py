# alpha_vantage_tickers_main.py
import os

from provider import Provider
from utils.savers import save_to_csv


def main():
  """
  Client code that uses the Provider facade to fetch tickers from Alpha Vantage.
  """
  api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "YOUR_API_KEY")

  if api_key == "YOUR_API_KEY":
    print("ERROR: Please set your ALPHA_VANTAGE_API_KEY environment variable.")
    return

  # 1. Create an instance of the Provider for Alpha Vantage.
  data_provider = Provider(provider="alpha_vantage", api_key=api_key)

  # 2. Fetch active tickers
  print("Fetching active tickers from Alpha Vantage...")
  active_tickers = data_provider.fetch_tickers()

  if active_tickers:
    filename = "alpha_vantage_tickers_active.csv"
    print(f"\nSaving {len(active_tickers)} active tickers to {filename}...")
    save_to_csv(active_tickers, filename)
  else:
    print("No active tickers were fetched.")


if __name__ == "__main__":
  main()
