from __future__ import annotations

import pandas as pd

_SAFE_NA_VALUES = ["", "#N/A", "N/A", "NULL"]


def read_csv_with_conventions(filepath_or_buffer, **kwargs) -> pd.DataFrame:
  """
  A project-specific wrapper for pd.read_csv that handles both files/URLs and buffers.

  It applies our convention of disabling the default "NA" string interpretation
  to correctly handle ticker symbols like "NA".
  """
  return pd.read_csv(
    filepath_or_buffer,
    keep_default_na=False,
    na_values=kwargs.pop("na_values", _SAFE_NA_VALUES),
    **kwargs,
  )
