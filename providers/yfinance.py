# providers/yfinance.py

import pandas as pd
import yfinance as yf

# A dictionary to map exchange arguments to their respective data URLs and processing details.
EXCHANGE_SOURCES = {
  "nasdaq": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
    "ticker_col": "Symbol",
    "name_col": "Security Name",
  },
  # This file contains listings for NYSE, NYSE Arca, NYSE MKT, BATS, and IEX.
  "other": {
    "url": "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
    "ticker_col": "ACT Symbol",
    "name_col": "Security Name",
  },
}


def _process_ticker_dataframe(
  df: pd.DataFrame, ticker_col: str, name_col: str
) -> list[dict]:
  """Helper function to process the raw DataFrame from NASDAQ's FTP."""
  # The last row is a footer/summary row which should be removed.
  if not df.empty:
    df = df.iloc[:-1]

  # Standardize column names
  rename_map = {ticker_col: "ticker", name_col: "name"}
  if "Exchange" in df.columns:
    rename_map["Exchange"] = "exchange_code"
  df.rename(columns=rename_map, inplace=True)

  # Add common fields for consistency
  df["market"] = "stocks"
  df["locale"] = "us"

  # Determine 'active' status. The 'otherlisted' file lacks this column.
  if "Financial Status" in df.columns:
    df["active"] = df["Financial Status"] == "N"
  else:
    # We can assume active if the status column is not present.
    df["active"] = True

  # Define the columns we want in the final output.
  output_columns = ["ticker", "name", "market", "locale", "active", "exchange_code"]

  # Filter for columns that actually exist in the dataframe to avoid errors.
  final_columns = [col for col in output_columns if col in df.columns]

  return df[final_columns].to_dict("records")


def get_tickers(exchange: str = "nasdaq") -> list[dict]:
  """
  Fetches a list of stock tickers from public directories provided by NASDAQ.

  The yfinance library itself does not provide a direct method to fetch a
  comprehensive list of all available tickers. A common and reliable
  approach is to download the list from an exchange like NASDAQ, which
  maintains a public directory of its listed securities.

  Args:
      exchange (str): The exchange list to fetch. Supported values are:
                      'nasdaq': For all NASDAQ-listed securities.
                      'other': For securities listed on other US exchanges like
                               NYSE, Arca, BATS, etc.
  """
  exchange_key = exchange.lower()
  if exchange_key not in EXCHANGE_SOURCES:
    raise ValueError(
      f"Unsupported exchange '{exchange}'. Supported values are: {list(EXCHANGE_SOURCES.keys())}"
    )

  source_info = EXCHANGE_SOURCES[exchange_key]
  url = source_info["url"]

  try:
    print(f"Downloading ticker list for '{exchange_key}' from: {url}")

    # Use pandas to read the pipe-separated file.
    df = pd.read_csv(url, sep="|")

    tickers = _process_ticker_dataframe(
      df,
      ticker_col=source_info["ticker_col"],
      name_col=source_info["name_col"],
    )

    print(f"Successfully fetched {len(tickers)} tickers for '{exchange_key}'.")
    return tickers

  except Exception as e:
    print(f"An error occurred while fetching tickers for '{exchange_key}': {e}")
    return []


def get_candles(
  ticker: str,
  start_date: str,
  end_date: str,
  interval: str = "1d",
) -> list[dict]:
  """
  Fetches candle data using the yfinance library.
  """
  try:
    stock = yf.Ticker(ticker)
    # The history() method returns a pandas DataFrame
    df = stock.history(start=start_date, end=end_date, interval=interval)

    if df.empty:
      return []

    # Convert the DataFrame to our standard list-of-dictionaries format
    candles = []
    for index, row in df.iterrows():
      candles.append(
        {
          # Note the capitalization difference in the DataFrame
          "date": index.strftime("%Y-%m-%d"),
          "open": row["Open"],
          "high": row["High"],
          "low": row["Low"],
          "close": row["Close"],
          "volume": row["Volume"],
        }
      )
    return candles

  except Exception as e:
    print(f"An error occurred with yfinance: {e}")
    return []
