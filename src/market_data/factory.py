from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from enum import Enum, auto


class Tier(Enum):
  FREE = auto()
  PREMIUM = auto()


@dataclass
class ProviderMetadata:
  class_path: str
  tier: Tier
  api_key_env_var: str | None = None
  rate_limit_per_minute: int | None = None


_PROVIDERS = {
  "yfinance": ProviderMetadata(
    class_path="market_data.providers.yfinance.YFinanceProvider", tier=Tier.FREE
  ),
  "polygon": ProviderMetadata(
    class_path="market_data.providers.polygon.PolygonProvider",
    tier=Tier.PREMIUM,
    api_key_env_var="POLYGON_API_KEY",
  ),
  "alpha_vantage": ProviderMetadata(
    class_path="market_data.providers.alpha_vantage.AlphaVantageProvider",
    tier=Tier.FREE,
    api_key_env_var="ALPHA_VANTAGE_API_KEY",
  ),
  "sec": ProviderMetadata(
    class_path="market_data.providers.sec.SecProvider", tier=Tier.FREE
  ),
}


class ProviderFactory:
  @staticmethod
  def _import_from_string(path: str) -> type:
    """Helper to dynamically import a class from a string path."""
    module_name, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

  def create(self, provider_name: str):
    """Creates a provider instance based on its registered name."""
    metadata = _PROVIDERS.get(provider_name)
    if not metadata:
      raise ValueError(f"Provider '{provider_name}' not found.")

    provider_class = self._import_from_string(metadata.class_path)

    constructor_kwargs = {}
    if metadata.api_key_env_var:
      api_key = os.getenv(metadata.api_key_env_var)
      if not api_key:
        raise ValueError(f"Missing required env var '{metadata.api_key_env_var}'")
      constructor_kwargs["api_key"] = api_key

    return provider_class(**constructor_kwargs)
