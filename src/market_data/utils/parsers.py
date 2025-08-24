from __future__ import annotations

import pandas as pd

_SAFE_NA_VALUES = [
  "",  # Empty string
  "#N/A",  # Excel N/A
  "N/A",  # Standard N/A (but not "NA" - that's a ticker symbol!)
  "NULL",  # Database NULL
]


def read_csv_with_conventions(
  filepath_or_buffer, strip_whitespace: bool = True, **kwargs
) -> pd.DataFrame:
  """A project-specific wrapper for pd.read_csv that applies financial data conventions.

  This function handles common issues when parsing financial CSV data:
  - Disables default "NA" string interpretation to correctly handle ticker symbols like "NA"
  - Strips whitespace from column names and cell values by default
  - Uses a conservative set of NA values to avoid false positives

  Args:
    filepath_or_buffer: File path, URL, or buffer object to read
    strip_whitespace: If True, strips leading/trailing whitespace from column names and string values
    **kwargs: Additional arguments passed to pd.read_csv

  Returns:
    pd.DataFrame: Parsed DataFrame with applied conventions

  Example:
    >>> df = read_csv_with_conventions('tickers.csv')
    >>> # Ticker symbol 'NA' will be preserved, not treated as NaN
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
    for col in df.select_dtypes(include=["object"]).columns:
      df[col] = df[col].astype(str).str.strip()

  return df
