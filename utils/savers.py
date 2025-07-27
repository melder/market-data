import logging
from pathlib import Path
from typing import Any

import pandas as pd

OUTPUT_DIR = Path("csv")


def save_to_csv(data: list[dict[str, Any]], filename: str) -> None:
  """Writes a list of dictionaries to a CSV file."""
  if not data:
    logging.warning("No data provided to write to CSV.")
    return

  try:
    # Ensure the output directory exists using pathlib.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Create the full path for the output file using pathlib's / operator.
    output_path = OUTPUT_DIR / filename

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info(f"Data successfully written to {output_path}")
  except Exception as e:
    logging.error(f"An error occurred while writing to {output_path}: {e}")
