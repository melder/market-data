# Tickers CLI

A command-line tool for fetching financial data from multiple providers.

## Features

- Fetch lists of tickers from supported providers
- Fetch historical candle (OHLCV) data for a given ticker
- Save results as CSV files
- Provider-specific handling and API key management

## Requirements

- Python 3.10+
- [Polygon.io](https://polygon.io/), [Alpha Vantage](https://www.alphavantage.co/), or [Yahoo Finance](https://finance.yahoo.com/) API keys (as needed)
- `.env` file for API keys (see below)

## Installation

```sh
git clone https://github.com/melder/tickers.git
cd tickers
pip install -e .
```

## Setup

Create a `.env` file in the project root with your API keys:

```
POLYGON_API_KEY=your_polygon_api_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
```


## Usage

### Run the CLI

Use the following command to run the CLI:

```sh
python -m tickers.main [COMMAND] [OPTIONS]
```

### Fetch Tickers

```sh
python -m tickers.main fetch-tickers --provider [polygon|alpha_vantage|yfinance] [--exchange nasdaq|other]
```

**Examples:**
```sh
python -m tickers.main fetch-tickers --provider polygon
python -m tickers.main fetch-tickers --provider yfinance --exchange nasdaq
```

### Fetch Candle Data

```sh
python -m tickers.main fetch-candles --provider [polygon|alpha_vantage|yfinance] --ticker TICKER --from-date YYYY-MM-DD [--to-date YYYY-MM-DD] [--timespan day|hour|minute] [--multiplier N]
```

**Example:**
```sh
python -m tickers.main fetch-candles --provider polygon --ticker AAPL --from-date 2024-01-01 --to-date 2024-06-01
```

## Output

- Ticker lists and candle data are saved as CSV files in the `csv/` directory.
- Filenames are provider- and query-specific.

## Development

- All code is in `src/tickers/`.
- Use absolute imports with the `tickers.` prefix for all internal modules.
- Add your own providers by extending the `Provider` class.
- Add new CLI commands using [Click](https://click.palletsprojects.com/).

## License

MIT License