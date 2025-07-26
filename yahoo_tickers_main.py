# yahoo_tickers_main.py

from provider import Provider
from utils.savers import save_to_csv


def main():
  """
  Client code that uses the Provider facade to fetch tickers via the
  'yfinance' provider integration, which gets data from NASDAQ's FTP site.
  """
  # 1. Create an instance of the Provider for yfinance.
  data_provider = Provider(provider="yfinance")

  # 2. Fetch tickers for NASDAQ
  print("Fetching NASDAQ tickers...")
  nasdaq_tickers = data_provider.fetch_tickers(exchange="nasdaq")

  if nasdaq_tickers:
    # Save all tickers to a single CSV file.
    filename = "yfinance_nasdaq_tickers_all.csv"
    print(f"\nSaving {len(nasdaq_tickers)} tickers to {filename}...")
    save_to_csv(nasdaq_tickers, filename)
  else:
    print("No NASDAQ tickers were fetched.")

  print("-" * 40)

  # 3. Fetch tickers for other major US exchanges (NYSE, Arca, etc.)
  print("\nFetching tickers for other exchanges (NYSE, Arca...)...")
  other_tickers = data_provider.fetch_tickers(exchange="other")

  if other_tickers:
    # Save all tickers to a separate CSV file.
    filename = "yfinance_other_exchanges_tickers_all.csv"
    print(f"\nSaving {len(other_tickers)} tickers to {filename}...")
    save_to_csv(other_tickers, filename)
  else:
    print("No tickers from other exchanges were fetched.")


if __name__ == "__main__":
  main()
