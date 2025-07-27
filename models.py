from dataclasses import dataclass


@dataclass(frozen=True)
class Candle:
  """Represents a single OHLCV candle."""

  open: float
  high: float
  low: float
  close: float
  volume: int
  timestamp: int  # Unix timestamp in milliseconds


@dataclass(frozen=True)
class Ticker:
  """Represents a stock ticker with its metadata."""

  ticker: str
  name: str
  active: bool
  market: str | None = None
  locale: str | None = None
  primary_exchange: str | None = None
  type: str | None = None
  currency_name: str | None = None
  cik: str | None = None
  last_updated_utc: str | None = None
