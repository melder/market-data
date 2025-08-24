from __future__ import annotations

import math

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Candle(BaseModel):
  """Represents a single OHLCV candle with Pydantic validation."""

  model_config = ConfigDict(from_attributes=True)

  open: float
  high: float
  low: float
  close: float
  volume: int
  timestamp: int


class Ticker(BaseModel):
  """Represents a stock ticker with Pydantic validation."""

  model_config = ConfigDict(from_attributes=True)

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
  optionable: bool | None = Field(
    default=None, description="Whether the ticker has options available for trading"
  )

  @field_validator("name", mode="before")
  @classmethod
  def clean_name(cls, v: any) -> str | None:  # noqa: N805
    """Converts float NaN values to None for the name field.

    This is necessary when parsing CSV data where missing company names
    are represented as NaN values by pandas.
    """
    if isinstance(v, float) and math.isnan(v):
      return None
    return v
