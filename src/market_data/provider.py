from typing import Any

# NOTE: This refactor assumes you will create these classes in their respective files.
# For example, a `PolygonProvider` class inside `providers/polygon.py`.
from market_data.models import Candle, Ticker
from market_data.providers.alpha_vantage import AlphaVantageProvider
from market_data.providers.interface import DataProvider
from market_data.providers.polygon import PolygonProvider
from market_data.providers.yfinance import YFinanceProvider

# A mapping from provider names to their corresponding strategy classes.
_PROVIDER_STRATEGIES: dict[str, type[DataProvider]] = {
  "polygon": PolygonProvider,
  "alpha_vantage": AlphaVantageProvider,
  "yfinance": YFinanceProvider,
}

# A set of providers that require an API key.
_PROVIDERS_REQUIRING_KEY = {"polygon", "alpha_vantage"}


class Provider:
  """
  A context class that uses a specific data provider strategy to fetch data.
  This class implements the Strategy design pattern.
  """

  _strategy: DataProvider

  def __init__(self, provider: str, api_key: str | None = None):
    if provider not in _PROVIDER_STRATEGIES:
      raise ValueError(f"Provider '{provider}' is not supported.")

    strategy_class = _PROVIDER_STRATEGIES[provider]

    # Instantiate the chosen strategy. The strategy class itself is now
    # responsible for validating the API key if it needs one.
    if provider in _PROVIDERS_REQUIRING_KEY:
      self._strategy = strategy_class(api_key=api_key)
    else:
      self._strategy = strategy_class()

  def fetch_candles(self, **kwargs: Any) -> list[Candle]:
    """Fetches candle data using the selected provider strategy."""
    return self._strategy.get_candles(**kwargs)

  def fetch_tickers(self, **kwargs: Any) -> list[Ticker]:
    """Fetches tickers using the selected provider strategy."""
    return self._strategy.get_tickers(**kwargs)
