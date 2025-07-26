# providers/alpha_vantage.py
import io

import pandas as pd
import requests
from alpha_vantage.timeseries import TimeSeries
from pandas.errors import ParserError


def get_candles(api_key: str, ticker: str, outputsize: str = "compact") -> list[dict]:
  """
  Fetches daily candle data specifically from the Alpha Vantage API.
  Note: The free tier has a strict rate limit.
  """
  try:
    ts = TimeSeries(key=api_key, output_format="json")
    data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize=outputsize)

    # Alpha Vantage returns a dictionary of dates. We must transform
    # it into a list of dictionaries, our standard format.
    candles = []
    for date, daily_data in data.items():
      candles.append(
        {
          "date": date,
          "open": float(daily_data["1. open"]),
          "high": float(daily_data["2. high"]),
          "low": float(daily_data["3. low"]),
          "close": float(daily_data["4. close"]),
          "volume": int(daily_data["6. volume"]),
        }
      )
    return candles

  except Exception as e:
    # Catch potential rate limit errors or other issues.
    print(f"An error occurred with Alpha Vantage: {e}")
    return []


def get_tickers(api_key: str) -> list[dict]:
  """
  Fetches a list of active stock tickers from Alpha Vantage.

  Args:
      api_key (str): Your Alpha Vantage API key.

  Returns:
      list[dict]: A list of tickers.
  """
  # The API defaults to state=active if the parameter is not provided.
  url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}"
  try:
    print("Downloading active ticker list from Alpha Vantage...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes

    try:
      # The response is a CSV file in text format. Use pandas to read it.
      df = pd.read_csv(io.StringIO(response.text))
    except ParserError:
      print(
        "Failed to parse CSV from Alpha Vantage. The response may be an error message (e.g., API limit reached)."
      )
      print(f"Response content: {response.text}")
      return []

    if df.empty:
      return []

    # Standardize column names and add common fields
    df.rename(
      columns={
        "symbol": "ticker",
        "assetType": "type",
        "ipoDate": "ipo_date",
        "delistingDate": "delisting_date",
      },
      inplace=True,
    )

    # Add common fields for consistency
    # Alpha Vantage is primarily US but has global, this is a simplification
    df["locale"] = "us"
    df["market"] = df["type"].str.lower()
    df["active"] = df["status"] == "Active"

    # Define the columns we want in the final output.
    output_columns = [
      "ticker",
      "name",
      "exchange",
      "type",
      "market",
      "locale",
      "active",
      "ipo_date",
      "delisting_date",
      "status",
    ]

    # Filter for columns that actually exist in the dataframe to avoid errors.
    final_columns = [col for col in output_columns if col in df.columns]

    tickers = df[final_columns].to_dict("records")
    print(f"Successfully fetched {len(tickers)} active tickers from Alpha Vantage.")
    return tickers

  except requests.exceptions.RequestException as e:
    print(f"An HTTP error occurred while fetching tickers from Alpha Vantage: {e}")
    return []
  except Exception as e:
    print(
      f"An unexpected error occurred while fetching tickers from Alpha Vantage: {e}"
    )
    return []
