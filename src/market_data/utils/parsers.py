from __future__ import annotations

import pandas as pd

_SAFE_NA_VALUES = ["", "#N/A", "N/A", "NULL"]


def read_csv_with_conventions(filepath_or_buffer, strip_whitespace=True, **kwargs) -> pd.DataFrame:
  """
  A project-specific wrapper for pd.read_csv that handles both files/URLs and buffers.

  It applies our conventions:
  - Disables default "NA" string interpretation to correctly handle ticker symbols like "NA"
  - Strips whitespace from column names and cell values by default

  Args:
    strip_whitespace: If True, strips leading/trailing whitespace from column names and string values
  """
  df = pd.read_csv(
    filepath_or_buffer,
    keep_default_na=False,
    na_values=kwargs.pop("na_values", _SAFE_NA_VALUES),
    **kwargs,
  )

  if strip_whitespace:
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
      df[col] = df[col].astype(str).str.strip()

  return df
