# strategies/yahoo_finance.py

from typing import Any

import yfinance as yf

from .base import DataFetchingStrategy


class YahooFinanceStrategy(DataFetchingStrategy):
  """
  A concrete strategy for fetching data from Yahoo Finance.
  """

  def fetch(self, ticker: str) -> dict[str, Any]:
    """
    Implements the fetching logic for Yahoo Finance.
    """
    try:
      stock = yf.Ticker(ticker)
      # .info can be slow and pulls a lot of data;
      # .history is more direct for just the price.
      hist = stock.history(period="1d")

      if hist.empty:
        return {
          "source": "Yahoo Finance",
          "error": f"No data found for ticker '{ticker}'",
        }

      # .iloc[-1] gets the last entry (most recent price)
      price = hist["Close"].iloc[-1]
      currency = stock.info.get("currency", "N/A")

      return {
        "source": "Yahoo Finance",
        "ticker": ticker,
        "price": f"{price:.2f}",
        "currency": currency,
      }
    except Exception as e:
      return {"source": "Yahoo Finance", "error": str(e)}

  def candles(self, ticker, start, end, interval="1m"):
    return yf.download(ticker, start, end, interval="1m")
