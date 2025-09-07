# Market Data CLI

A command-line tool for fetching financial data from multiple providers.

## Features

- Fetch lists of tickers from supported providers
- Fetch historical candle (OHLCV) data for a given ticker
- **Fetch optionable tickers** - stocks that have options available for trading
- Save results as CSV files
- Provider-specific handling and API key management

## Supported Providers

- **Polygon** - Comprehensive market data with API key required
- **Alpha Vantage** - Stock and forex data with API key required  
- **Yahoo Finance (yfinance)** - Free stock data, no API key required
- **SEC** - Official SEC filing data, no API key required
- **CBOE** - Options market data, no API key required

## Requirements

- Python 3.10+
- API keys for premium providers (see setup below)
- `.env` file for API keys (optional, only for premium providers)

## Installation

```sh
git clone https://github.com/melder/market-data.git
cd market-data
pip install -e .
```

## Setup

For premium providers, create a `.env` file in the project root:

```
POLYGON_API_KEY=your_polygon_api_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
```

Free providers (yfinance, SEC, CBOE) work without API keys.


## Usage

### Run the CLI

```sh
python -m market_data.main [COMMAND] [OPTIONS]
```

### Fetch Tickers

```sh
python -m market_data.main fetch-tickers --provider [polygon|alpha_vantage|yfinance|sec] [--exchange nasdaq]
```

**Examples:**
```sh
python -m market_data.main fetch-tickers --provider polygon
python -m market_data.main fetch-tickers --provider yfinance --exchange nasdaq
python -m market_data.main fetch-tickers --provider sec --exchange nasdaq
```

### Fetch Candle Data

```sh
python -m market_data.main fetch-candles --provider [polygon|alpha_vantage|yfinance] --ticker TICKER --from-date YYYY-MM-DD [--to-date YYYY-MM-DD] [--timespan day|hour|minute] [--multiplier N]
```

**Example:**
```sh
python -m market_data.main fetch-candles --provider polygon --ticker AAPL --from-date 2024-01-01 --to-date 2024-06-01
```

### Fetch Optionable Tickers

```sh
python -m market_data.main fetch-optionable-tickers --provider [cboe|yfinance] [OPTIONS]
```

**Examples:**

CBOE (fast, official source):
```sh
python -m market_data.main fetch-optionable-tickers --provider cboe --type weeklies
python -m market_data.main fetch-optionable-tickers --provider cboe --type all
```

Yahoo Finance (comprehensive but slow):
```sh
# Test with small subset
python -m market_data.main fetch-optionable-tickers --provider yfinance --max-tickers 100 --delay 2.0

# Full scan (takes 3+ hours)
python -m market_data.main fetch-optionable-tickers --provider yfinance --delay 1.5

# Target specific exchange
python -m market_data.main fetch-optionable-tickers --provider yfinance --exchange nasdaq
```

**yfinance Options:**
- `--max-tickers N` - Limit to first N tickers (for testing)
- `--delay X` - Seconds between requests (default: 1.5, minimum recommended)
- `--exchange` - Filter by exchange (nasdaq/other)

## Output

All data is saved as CSV files in the `csv/` directory with descriptive filenames:

- **Tickers**: `{provider}_{exchange}_tickers.csv`
- **Candles**: `{provider}_{ticker}_candles_{from_date}_to_{to_date}.csv`
- **Optionable Tickers**: `{provider}_{type}_optionable_tickers.csv`

## Development

### Architecture
- **Factory Pattern**: `ProviderFactory` creates provider instances dynamically
- **Protocol/Strategy Pattern**: Providers implement `DataProvider` protocol
- **Abstract Base Classes**: `TickersFetcher`, `CandlesFetcher`, `OptionableFetcher`

### Project Structure
```
src/market_data/
├── main.py           # CLI entry point
├── factory.py        # ProviderFactory
├── interfaces.py     # Abstract base classes
├── models.py         # Pydantic data models
├── providers/        # Provider implementations
│   ├── polygon.py    # Polygon.io integration
│   ├── yfinance.py   # Yahoo Finance integration
│   ├── cboe.py       # CBOE options data
│   └── sec.py        # SEC filing data
└── utils/            # Utilities for parsing/saving
```

### Adding New Providers
1. Implement the `DataProvider` protocol in `providers/`
2. Add fetcher classes inheriting from abstract interfaces
3. Register in `ProviderFactory`
4. Follow existing patterns for error handling and logging

### Code Quality
```sh
ruff check .        # Linting
ruff format .       # Code formatting
```

## License

MIT License
