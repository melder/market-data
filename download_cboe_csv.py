import os

import requests


def download_weeklies():
  download_cboe_options_symbols(
    url="https://www.cboe.com/us/options/symboldir/weeklys_options/?download=csv",
    filename="cboe_options_symbols_weeklies.csv",
  )


def download_quarterlies():
  download_cboe_options_symbols(
    url="https://www.cboe.com/us/options/symboldir/quarterlys_options/?download=csv",
    filename="cboe_options_symbols_quarterlies.csv",
  )


def download_all():
  download_cboe_options_symbols(
    url="https://www.cboe.com/us/options/symboldir/?download=csv"
  )


def download_cboe_options_symbols(url, filename="cboe_options_symbols.csv"):
  """
  Downloads the CBOE options symbols directory CSV and saves it locally.

  Args:
      filename (str): The name of the file to save the CSV data to.
                      Defaults to "cboe_options_symbols.csv".
  """

  print(f"Attempting to download from: {url}")

  try:
    # Some websites may block requests without a valid User-Agent.
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    # It's good practice to set a timeout for requests.
    response = requests.get(url, timeout=30, headers=headers)

    # Raise an exception for bad status codes (e.g., 404, 500)
    response.raise_for_status()

    output_path = os.path.abspath(filename)

    with open(output_path, "wb") as f:
      f.write(response.content)

    print(f"\nSuccessfully downloaded and saved to: {output_path}")

  except requests.exceptions.RequestException as e:
    print(f"An error occurred during the download: {e}")


if __name__ == "__main__":
  download_all()
  download_quarterlies()
  download_weeklies()
