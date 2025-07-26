from typing import Any

import pandas as pd


def save_to_csv(data: list[dict[str, Any]], filename: str) -> None:
  """Writes a list of dictionaries to a CSV file."""
  if not data:
    print("No data provided to write.")
    return
  if not filename.startswith("/csv"):
    filename = f"./csv/{filename}"

  try:
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Data successfully written to {filename}")
  except Exception as e:
    print(f"An error occurred while writing to CSV: {e}")
