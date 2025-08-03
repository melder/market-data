# providers/yfinance.py
import logging
from typing import Any

import pandas as pd
import yfinance as yf

from tickers.models import Candle, Ticker


class YFinanceProvider:
  """Yahoo Finance data provider strategy."""

  # A dictionary to map exchange arguments to their respective data URLs and processing details.
  _EXCHANGE_SOURCES = {
    "nasdaq": {
      "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
      "ticker_col": "Symbol",
      "name_col": "Security Name",
    },
    # This file contains listings for NYSE, NYSE Arca, NYSE MKT, BATS, and IEX.
    "other": {
      "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
      "ticker_col": "ACT Symbol",
      "name_col": "Security Name",
    },
  }

  def __init__(self, api_key: str | None = None):
    """Initializes the yfinance provider. No API key is needed."""
    pass

  def _process_ticker_dataframe(
    self, df: pd.DataFrame, ticker_col: str, name_col: str
  ) -> list[Ticker]:
    """Helper function to process the raw DataFrame from NASDAQ's FTP."""
    if df.empty:
      return []
    # The last row is a footer/summary row which should be removed.
    df = df.copy().iloc[:-1]

    # Standardize column names
    rename_map = {ticker_col: "ticker", name_col: "name"}
    df.rename(columns=rename_map, inplace=True)

    # Determine 'active' status. The 'otherlisted' file lacks this column.
    if "Financial Status" in df.columns:
      df["active"] = df["Financial Status"] == "N"
    else:
      df["active"] = True  # Assume active if status column is not present.

    tickers = [
      Ticker(
        ticker=row.ticker,
        name=row.name,
        active=row.active,
        primary_exchange=getattr(row, "Exchange", None),
        market="stocks",
        locale="us",
      )
      for row in df.itertuples(index=False)
    ]
    return tickers

  def get_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches a list of stock tickers from public directories provided by NASDAQ."""
    exchange = kwargs.get("exchange", "nasdaq")
    if exchange not in self._EXCHANGE_SOURCES:
      raise ValueError(
        f"Unsupported exchange '{exchange}'. Supported values are: {list(self._EXCHANGE_SOURCES.keys())}"
      )

    source_info = self._EXCHANGE_SOURCES[exchange]
    url = source_info["url"]

    try:
      logging.info(f"Downloading ticker list for '{exchange}' from: {url}")
      df = pd.read_csv(url, sep="|")
      tickers = self._process_ticker_dataframe(
        df,
        ticker_col=source_info["ticker_col"],
        name_col=source_info["name_col"],
      )
      logging.info(f"Successfully fetched {len(tickers)} tickers for '{exchange}'.")
      return tickers
    except Exception as e:
      logging.error(f"An error occurred while fetching tickers for '{exchange}': {e}")
      return []

  def _map_to_yfinance_interval(self, timespan: str, multiplier: int) -> str:
    """Maps the generic timespan and multiplier to a yfinance-compatible interval string."""
    if timespan in ("day", "week", "month"):
      # For daily, weekly, monthly, yfinance uses 'd', 'wk', 'mo'
      interval_char = timespan[0]
      if timespan == "week":
        interval_char = "wk"
      return f"{multiplier}{interval_char}"

    if timespan in ("minute", "hour"):
      # For intraday, yfinance uses 'm' and 'h'
      interval_char = timespan[0]
      interval = f"{multiplier}{interval_char}"
      logging.warning(
        f"yfinance has data limitations for intraday intervals like '{interval}'. "
        "Short date ranges are recommended."
      )
      return interval

    raise ValueError(f"Unsupported timespan for yfinance provider: '{timespan}'")

  def get_candles(self, **kwargs: Any) -> list[Candle]:
    """Fetches candle data using the yfinance library."""
    try:
      interval = self._map_to_yfinance_interval(
        timespan=kwargs.get("timespan", "day"),
        multiplier=kwargs.get("multiplier", 1),
      )

      stock = yf.Ticker(kwargs.get("ticker"))
      df = stock.history(
        start=kwargs.get("from_date"), end=kwargs.get("to_date"), interval=interval
      )

      if df.empty:
        return []

      # Convert the DataFrame to our standard list of Candle objects
      candles = [
        Candle(
          open=row.Open,
          high=row.High,
          low=row.Low,
          close=row.Close,
          volume=row.Volume,
          timestamp=int(row.Index.timestamp() * 1000),
        )
        for row in df.itertuples()
      ]
      return candles

    except Exception as e:
      logging.error(f"An error occurred with yfinance get_candles: {e}")
      return []
