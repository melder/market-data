from __future__ import annotations

import math

from pydantic import BaseModel, Field, field_validator


class Candle(BaseModel):
  """Represents a single OHLCV candle with Pydantic validation."""

  open: float
  high: float
  low: float
  close: float
  volume: int
  timestamp: int


class Ticker(BaseModel):
  """Represents a stock ticker with Pydantic validation."""

  ticker: str
  name: str | None = None
  active: bool
  market: str | None = None
  locale: str | None = None
  primary_exchange: str | None = Field(default=None, alias="primaryExchange")
  type: str | None = None
  currency_name: str | None = Field(default=None, alias="currencyName")
  cik: str | None = None
  last_updated_utc: str | None = Field(default=None, alias="lastUpdatedUtc")

  @field_validator("name", mode="before")
  def clean_name(cls, v: any) -> str | None:  # noqa: N805
    """Converts float NaN values to None for the name field."""
    if isinstance(v, float) and math.isnan(v):
      return None
    return v
