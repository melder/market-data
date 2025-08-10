import logging
from typing import Any

import pandas as pd
import yfinance as yf

from market_data.interfaces import CandlesFetcher, TickersFetcher
from market_data.models import Candle, Ticker

# --- Ticker Fetching Logic ---

_EXCHANGE_SOURCES = {
  "nasdaq": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
    "ticker_col": "Symbol",
    "name_col": "Security Name",
  },
  "other": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
    "ticker_col": "ACT Symbol",
    "name_col": "Security Name",
  },
}


def _get_tickers_impl(**kwargs: Any) -> list[Ticker]:
  exchange = kwargs.get("exchange", "nasdaq")
  source_info = _EXCHANGE_SOURCES[exchange]
  try:
    df = pd.read_csv(source_info["url"], sep="|")
    return _process_ticker_dataframe(
      df, source_info["ticker_col"], source_info["name_col"]
    )
  except Exception as e:
    logging.error(f"Error fetching tickers for '{exchange}': {e}", exc_info=True)
    return []


def _process_ticker_dataframe(
  df: pd.DataFrame, ticker_col: str, name_col: str
) -> list[Ticker]:
  if df.empty:
    return []
  df = df.copy().iloc[:-1]
  df = df.rename(columns={ticker_col: "ticker", name_col: "name"})
  df["active"] = (
    (df["Financial Status"] == "N") if "Financial Status" in df.columns else True
  )
  return [
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


# --- Candle Fetching Logic ---


def _map_to_yfinance_interval(timespan: str, multiplier: int) -> str:
  span_map = {"minute": "m", "hour": "h", "day": "d", "week": "wk", "month": "mo"}
  interval_char = span_map.get(timespan)
  if not interval_char:
    raise ValueError(f"Unsupported timespan for yfinance: '{timespan}'")
  return f"{multiplier}{interval_char}"


def _get_candles_impl(**kwargs: Any) -> list[Candle]:
  try:
    interval = _map_to_yfinance_interval(kwargs["timespan"], kwargs["multiplier"])
    stock = yf.Ticker(kwargs["ticker"])
    df = stock.history(
      start=kwargs["from_date"], end=kwargs["to_date"], interval=interval
    )
    if df.empty:
      return []
    return [
      Candle(
        open=r.Open,
        high=r.High,
        low=r.Low,
        close=r.Close,
        volume=r.Volume,
        timestamp=int(r.Index.timestamp() * 1000),
      )
      for r in df.itertuples()
    ]
  except Exception as e:
    logging.error(f"Error with yfinance get_candles: {e}", exc_info=True)
    return []


# --- Public Provider Class ---


class YFinanceProvider:
  def __init__(self):
    self._capabilities = {
      TickersFetcher: _get_tickers_impl,
      CandlesFetcher: _get_candles_impl,
    }

  def supports(self, interface_class: type) -> bool:
    return interface_class in self._capabilities

  def get_fetcher(self, interface_class: type):
    if not self.supports(interface_class):
      raise TypeError(f"This provider does not support {interface_class.__name__}")
    return self._capabilities[interface_class]
