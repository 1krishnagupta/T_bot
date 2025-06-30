# Jigsaw Flow Trading Bot Directory Structure

The Jigsaw Flow Trading Bot is a comprehensive options trading system that automates intraday trading strategies based on price action, multi-timeframe technical indicators, ETF sector momentum, and compression breakout patterns.

## Top-Level Structure

```
T_bot/
├── Code/               # Main code directory
│   ├── bot_core/       # Core trading components
│   ├── ui/             # User interface components
├── run_bot.py          # Main entry point
├── logs/               # Log files directory
├── config/             # Configuration files
│   ├── credentials.txt # Authentication and trading settings
│   ├── settings.yaml   # Additional settings
├── mongodb/            # MongoDB installation directory
├── backtest_data/      # Historical data for backtesting
```

## Core Files Overview

### Main Entry Point

| File | Description |
|------|-------------|
| `run_bot.py` | The main entry point for the application. Parses command-line arguments, initializes components, and starts the trading bot. Supports both headless (CLI) and GUI modes, with test mode capabilities. |

### Bot Core Components

| File | Description |
|------|-------------|
| `bot_core/tastytrade_api.py` | Client for the TastyTrade API. Handles authentication, session management, and API requests for trading operations. |
| `bot_core/config_loader.py` | Loads and manages configuration from YAML, JSON, or TXT files. Provides default configurations and handles credential management. |
| `bot_core/instrument_fetcher.py` | Fetches instrument data from the broker API, including equities, options chains, and market quotes. Includes methods for finding appropriate option contracts for trades. |
| `bot_core/market_data_client.py` | Connects to real-time market data streams. Processes quotes, trades, and sector updates, and can build candles from tick data. |
| `bot_core/candle_builder.py` | Builds OHLC candles of different time intervals from tick data. Manages candle completion and provides historical candle access. |
| `bot_core/candle_data_client.py` | Fetches historical candle data for analysis and backtesting. Can retrieve data from the database or external sources. |
| `bot_core/mongodb_handler.py` | Manages MongoDB database operations. Handles auto-installation, connections, and database operations for storing market data. |
| `bot_core/jigsaw_strategy.py` | Implements the core Jigsaw Flow trading strategy. Detects sector alignment, compression patterns, and trading signals using technical indicators. |
| `bot_core/order_manager.py` | Manages order creation, submission, and tracking. Provides methods for different order types and trade management. |
| `bot_core/backtest_engine.py` | Engine for running backtests on historical market data. Simulates trades and calculates performance metrics. |

### User Interface Components

| File | Description |
|------|-------------|
| `ui/jigsaw_flow_ui.py` | Main UI implementation using PyQt5. Contains widget classes for all UI components, including the login screen, trading dashboard, configuration panel, and more. |
| `ui/ui_controller.py` | Controller that connects the UI with business logic. Handles UI events, updates the display, and coordinates between the UI and trading components. |

## Utility Files and Directories

| Directory | Description |
|-----------|-------------|
| `logs/` | Contains application log files organized by date. |
| `config/` | Stores configuration files including credentials and application settings. |
| `mongodb/` | Directory for MongoDB installation and data files. |
| `test_data/` | Contains generated test data for testing the trading system. |
| `backtest_data/` | Stores historical market data for backtesting purposes. |

## Component Relationships

- `run_bot.py` initializes the system and creates instances of core components.
- `ui_controller.py` connects UI events to core trading logic.
- `jigsaw_strategy.py` implements the trading strategy using data from `market_data_client.py` and executing trades via `order_manager.py`.
- `mongodb_handler.py` provides database functionality used by `market_data_client.py` and `candle_builder.py` to store and retrieve market data.
- `candle_data_client.py` uses historical data for analysis and backtesting.
- `backtest_engine.py` simulates the trading strategy on historical data.

## Configuration

The system uses a flexible configuration system with:

- `credentials.txt`: Contains broker authentication details and core trading parameters.

The trading bot can run in several modes:
- Normal mode with real trading
- Test mode with simulated trading
- Headless (CLI) mode without UI
- Backtesting mode for strategy evaluation
