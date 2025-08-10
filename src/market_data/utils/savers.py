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

  output_path = OUTPUT_DIR / filename
  try:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info(f"Data successfully written to {output_path}")
  except (OSError, PermissionError) as e:
    # Catch specific file system errors.
    logging.error(f"A file system error occurred while writing to {output_path}: {e}")
  except Exception as e:
    # Catch any other unexpected errors.
    logging.error(f"An unexpected error occurred while writing to {output_path}: {e}")
