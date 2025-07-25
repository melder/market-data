# providers/yfinance.py

import yfinance as yf


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
