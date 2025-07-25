from typing import Any

from providers import alpha_vantage, polygon, yfinance


class Provider:
  """
  A simple facade for accessing data from different providers.
  """

  def __init__(self, provider: str, api_key: str | None = None):
    if provider not in ["polygon", "alpha_vantage", "yfinance"]:
      raise ValueError(f"Provider '{provider}' is not supported.")

    self.provider = provider
    self.api_key = api_key

  def fetch_candles(self, **kwargs: Any) -> list[dict]:
    """
    Fetches candle data by routing the request to the appropriate
    provider module.
    """
    match self.provider:
      case "polygon":
        if not self.api_key:
          raise ValueError("API key is required for the Polygon provider.")
        return polygon.get_candles(api_key=self.api_key, **kwargs)

      case "alpha_vantage":
        if not self.api_key:
          raise ValueError("API key is required for the Alpha Vantage provider.")
        return alpha_vantage.get_candles(api_key=self.api_key, **kwargs)

      # 3. Add the new case for yfinance
      case "yfinance":
        # yfinance does not require an API key
        return yfinance.get_candles(**kwargs)

      case _:
        return []
