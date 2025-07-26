# main.py

from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta

from context import StockFetcher
from strategies.yahoo_finance import YahooFinanceStrategy

# In the future, you could add another strategy like this:
# from strategies.alpha_vantage import AlphaVantageStrategy


def main():
  """
  The client code picks a concrete strategy and passes it to the context.
  The client should be aware of the differences between strategies to make
  the right choice.
  """
  ticker_symbol = "AAPL"  # Using Apple Inc. as the example ticker

  # 1. Choose and create a strategy object.
  yahoo_strategy = YahooFinanceStrategy()

  # 2. Create the context and inject the strategy.
  fetcher = StockFetcher(strategy=yahoo_strategy)

  # 3. Use the context to fetch data.
  data = fetcher.fetch_stock_data(ticker_symbol)

  print("-" * 30)
  print(f"Final data received by client:\n{data}")
  print("-" * 30)

  # --- Example of switching strategies at runtime ---
  # If you had another strategy, you could swap it in like this:
  #
  # print("\nClient: Switching strategy...")
  # alpha_vantage_strategy = AlphaVantageStrategy(api_key="YOUR_KEY")
  # fetcher.strategy = alpha_vantage_strategy
  # data = fetcher.fetch_stock_data(ticker_symbol)
  # print(f"Final data received by client:\n{data}")


def main2():
  yahoo = YahooFinanceStrategy()
  fetcher = StockFetcher(strategy=yahoo)

  dt_start = datetime.now(UTC).date() - relativedelta(days=2)
  dt_end = datetime.now(UTC).date()

  data = fetcher.fetch_candles("AAPL", dt_start.isoformat(), dt_end.isoformat())
  # data = fetcher.fetch_candles("AAPL", dt_end.isoformat(), dt_end.isoformat())
  print(data)


if __name__ == "__main__":
  main2()
