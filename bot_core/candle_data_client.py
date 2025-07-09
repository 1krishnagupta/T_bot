# File: Code/bot_core/candle_data_client.py

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import json
import logging
from typing import Dict, List, Optional, Callable, Tuple, Any, Union
from Code.bot_core.tastytrade_data_fetcher import TastyTradeDataFetcher
from Code.bot_core.tradestation_data_fetcher import TradeStationDataFetcher
from Code.bot_core.backtest_directory_manager import BacktestDirectoryManager

from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class CandleDataClient:
    """Client for fetching historical candle data"""
    
    def __init__(self, market_data_client):
        """
        Initialize the candle data client
        
        Args:
            market_data_client (MarketDataClient): Initialized MarketDataClient instance
        """
        self.market_data_client = market_data_client
        self.candle_data = {}  # Store received candle data
        self.active_subscriptions = {}
        self.db = get_mongodb_handler() if market_data_client.save_to_db else None
        
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"candle_data_{today}.log")
        
        self.logger = logging.getLogger("CandleDataClient")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
    def get_historical_data(self, symbol, period="1d", days_back=30, callback=None):
        """
        Fetch historical candle data for a symbol
        
        Args:
            symbol (str): Equity symbol (e.g. "SPY", "AAPL")
            period (str): Candle period (e.g. "5m", "1h", "1d")
            days_back (int): Number of days to look back
            callback (callable): Callback function for when data is received
                
        Returns:
            str: Subscription ID
        """
        # Calculate from_time
        from_time = int(time.time()) - (days_back * 24 * 60 * 60)
        
        # Generate a unique subscription ID
        subscription_id = f"{symbol}_{period}_{from_time}"
        
        # Setup callback for candle data
        def on_candle_data(data):
            if "symbol" in data and data["symbol"].startswith(symbol):
                # Store the data
                if subscription_id not in self.candle_data:
                    self.candle_data[subscription_id] = []
                    
                self.candle_data[subscription_id].append(data)
                
                # Call user callback if provided
                if callback:
                    callback(data)
        
        # Subscribe to candle data
        channel_id = self.market_data_client.subscribe_to_candles(
            symbol, period, from_time
        )
        
        # Store subscription information
        self.active_subscriptions[subscription_id] = {
            "symbol": symbol,
            "period": period,
            "from_time": from_time,
            "channel_id": channel_id
        }
        
        return subscription_id
        
    def cancel_subscription(self, subscription_id):
        """
        Cancel a historical data subscription
        
        Args:
            subscription_id (str): Subscription ID to cancel
        """
        if subscription_id in self.active_subscriptions:
            subscription = self.active_subscriptions[subscription_id]
            
            # Unsubscribe from the channel
            self.market_data_client.unsubscribe(subscription["channel_id"])
            
            # Remove subscription from active list
            del self.active_subscriptions[subscription_id]
            
    def get_candle_data(self, subscription_id):
        """
        Get the collected candle data for a subscription
        
        Args:
            subscription_id (str): Subscription ID
                
        Returns:
            list: Collected candle data
        """
        return self.candle_data.get(subscription_id, [])
        
    def clear_candle_data(self, subscription_id=None):
        """
        Clear stored candle data
        
        Args:
            subscription_id (str, optional): Specific subscription to clear,
                or all if None
        """
        if subscription_id:
            if subscription_id in self.candle_data:
                del self.candle_data[subscription_id]
        else:
            self.candle_data = {}
    
    def get_candles_from_db(self, symbol, period, start_time=None, end_time=None, limit=100):
        """
        Get historical candles from the database
        
        Args:
            symbol (str): Instrument symbol
            period (str): Candle period (e.g., "1m", "5m")
            start_time (Union[str, datetime]): Start time in ISO format or datetime
            end_time (Union[str, datetime]): End time in ISO format or datetime
            limit (int): Maximum number of candles to return
            
        Returns:
            List[Dict]: List of candles from the database
        """
        if not self.db:
            return []
            
        try:
            period_str = period if isinstance(period, str) else f"{period}m"
            
            # Build query
            query = {
                "symbol": symbol,
                "period": period_str
            }
            
            if start_time:
                if 'start_time' not in query:
                    query['start_time'] = {}
                query['start_time']['$gte'] = start_time if isinstance(start_time, str) else start_time.isoformat()
                
            if end_time:
                if 'start_time' not in query:
                    query['start_time'] = {}
                query['start_time']['$lte'] = end_time if isinstance(end_time, str) else end_time.isoformat()
                
            # Query database
            candles = self.db.find_many(COLLECTIONS['CANDLES'], query, limit=limit)
            
            # Sort by start_time
            return sorted(candles, key=lambda x: x['start_time'])
        except Exception as e:
            self.logger.error(f"Error getting candles from database: {e}")
            return []
    
    def get_latest_candle(self, symbol, period):
        """
        Get the latest candle for a symbol and period from the database
        
        Args:
            symbol (str): Instrument symbol
            period (str): Candle period (e.g., "1m", "5m")
            
        Returns:
            Dict: Latest candle from the database, or None if not found
        """
        if not self.db:
            return None
            
        try:
            period_str = period if isinstance(period, str) else f"{period}m"
            
            # Build query
            query = {
                "symbol": symbol,
                "period": period_str
            }
            
            # Query database and sort by start_time in descending order
            candles = self.db.find_many(COLLECTIONS['CANDLES'], query, limit=1)
            
            if candles:
                return candles[0]
            return None
        except Exception as e:
            self.logger.error(f"Error getting latest candle from database: {e}")
            return None
            
    @staticmethod
    def get_recommended_period(days_back):
        """
        Get recommended candle period based on days back
        
        Args:
            days_back (int): Number of days to look back
                
        Returns:
            str: Recommended period (e.g. "5m", "1h", "1d")
        """
        if days_back <= 1:
            return "1m"  # 1-minute candles for 1 day or less
        elif days_back <= 7:
            return "5m"  # 5-minute candles for up to a week
        elif days_back <= 30:
            return "30m"  # 30-minute candles for up to a month
        elif days_back <= 90:
            return "1h"   # 1-hour candles for up to 3 months
        elif days_back <= 180:
            return "2h"   # 2-hour candles for up to 6 months
        else:
            return "1d"   # Daily candles for longer periods
    
    def get_candles_by_date_range(self, symbol, period, start_date, end_date=None):
        """
        Get candles for a specific date range
        
        Args:
            symbol (str): Instrument symbol
            period (str): Candle period (e.g., "1m", "5m")
            start_date (Union[str, datetime.date]): Start date
            end_date (Union[str, datetime.date], optional): End date, defaults to today
            
        Returns:
            List[Dict]: List of candles for the date range
        """
        # Convert dates to datetime objects if they are strings
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).date()
            
        if end_date is None:
            end_date = datetime.now().date()
        elif isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).date()
        
        # Build start and end times
        start_time = datetime.combine(start_date, datetime.min.time()).isoformat()
        end_time = datetime.combine(end_date, datetime.max.time()).isoformat()
        
        # Get candles from database
        return self.get_candles_from_db(symbol, period, start_time, end_time)
    
    

    def fetch_historical_data_for_backtesting(self, symbols, period, start_date, end_date=None, data_source="TradeStation", **kwargs):
        """
        Fetch historical data for backtesting from external sources and save to CSV files
        Now stops on failure instead of falling back to other sources
        """
        # Import directory manager
        from Code.bot_core.backtest_directory_manager import BacktestDirectoryManager
        dir_manager = BacktestDirectoryManager()
        
        result = {}
        
        # Convert dates to datetime objects if they are strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                
        if end_date is None:
            end_date = datetime.now().date()
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Determine period string
        period_str = f"{period}m" if isinstance(period, int) else period
        
        # Calculate date range info
        days_diff = (end_date - start_date).days
        self.logger.info(f"Fetching {days_diff} days of {period_str} data from {start_date} to {end_date}")
        
        # FIXED: Check strategy type BEFORE fetching additional symbols
        use_mag7 = False
        config = kwargs.get('config')
        if not config and hasattr(self, 'config'):
            config = self.config
        
        if config:
            use_mag7 = config.get("trading_config", {}).get("use_mag7_confirmation", False)
        
        # Only fetch additional symbols based on strategy type
        all_symbols = list(symbols)  # Start with requested symbols
        
        if use_mag7:
            # Fetch Mag7 stocks for Mag7 strategy
            mag7_stocks = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"]
            if config:
                mag7_stocks = config.get("trading_config", {}).get("mag7_stocks", mag7_stocks)
            
            # Add Mag7 stocks to symbols list
            all_symbols.extend(mag7_stocks)
            print(f"[*] Using Mag7 strategy - fetching Mag7 stocks: {', '.join(mag7_stocks)}")
        else:
            # Fetch sector ETFs for sector alignment strategy
            sector_etfs = ["XLK", "XLF", "XLV", "XLY"]
            if config:
                selected_sectors = config.get("trading_config", {}).get("selected_sectors", sector_etfs)
                sector_etfs = selected_sectors
            
            # Add sector ETFs to symbols list
            all_symbols.extend(sector_etfs)
            print(f"[*] Using Sector Alignment strategy - fetching ETFs: {', '.join(sector_etfs)}")
        
        all_symbols = list(set(all_symbols))  # Remove duplicates
        
        print(f"[*] Fetching data for symbols: {', '.join(all_symbols)}")
        
        # Show data source limitations
        if data_source == "TradeStation":
            print(f"\n[*] Using TradeStation API")
            print(f"[*] TradeStation Data Capabilities:")
            print(f"    - 1m data: Up to 40 days")
            print(f"    - 5m data: Up to 6 months")
            print(f"    - 15m data: Up to 1 year")
            print(f"    - 30m data: Up to 2 years")
            print(f"    - 1h data: Up to 3 years")
            print(f"    - 1d data: Up to 10 years\n")
        elif data_source == "TastyTrade":
            print(f"\n[*] Using TastyTrade API")
            print(f"[*] TastyTrade Data Capabilities:")
            print(f"    - Requires active API connection")
            print(f"    - Real-time and historical options data")
            print(f"    - All timeframes available with account\n")
        elif data_source == "YFinance":
            print(f"\n[*] Using Yahoo Finance API")
            print(f"[*] YFinance Data Capabilities:")
            print(f"    - 1m data: Only last 7 days")
            print(f"    - 5m data: Only last 60 days")
            print(f"    - 15m data: Only last 60 days")
            print(f"    - 30m+ data: Up to years of data")
            print(f"    - Free, no authentication required\n")
            
            # Check for YFinance limitations
            if period_str == "1m" and days_diff > 7:
                error_msg = f"[!] ERROR: YFinance only provides 7 days of 1-minute data, but you requested {days_diff} days"
                print(error_msg)
                raise ValueError(error_msg)
            elif period_str in ["5m", "15m"] and days_diff > 60:
                error_msg = f"[!] ERROR: YFinance only provides 60 days of {period_str} data, but you requested {days_diff} days"
                print(error_msg)
                raise ValueError(error_msg)
        
        # Initialize data fetcher based on source
        data_fetcher = None
        auth_failed = False
        
        if data_source == "TradeStation":
            try:
                from Code.bot_core.tradestation_data_fetcher import TradeStationDataFetcher
                tradestation_fetcher = TradeStationDataFetcher()
                if not tradestation_fetcher.test_connection():
                    auth_failed = True
                    error_msg = "[!] TradeStation authentication failed"
                    print(error_msg)
                    print("[!] Possible reasons:")
                    print("    1. Invalid API credentials")
                    print("    2. API key doesn't have market data permissions")
                    print("    3. TradeStation account not active")
                    print("\n[!] Suggestion: You can try other data sources:")
                    print("    - YFinance: Free, no auth required (limited history)")
                    print("    - TastyTrade: Requires account login (full history)")
                    raise ConnectionError(error_msg)
                else:
                    data_fetcher = tradestation_fetcher
            except Exception as e:
                error_msg = f"[!] Error initializing TradeStation: {str(e)}"
                print(error_msg)
                raise
                
        elif data_source == "TastyTrade":
            try:
                # Check if API is available
                api = kwargs.get('api')
                if not api:
                    error_msg = "[!] TastyTrade API not available - requires login"
                    print(error_msg)
                    print("\n[!] Suggestion: You can try other data sources:")
                    print("    - YFinance: Free, no auth required (limited history)")
                    print("    - TradeStation: API key required (extensive history)")
                    raise ConnectionError(error_msg)
                else:
                    from Code.bot_core.tastytrade_data_fetcher import TastyTradeDataFetcher
                    data_fetcher = TastyTradeDataFetcher(api=api)
            except Exception as e:
                error_msg = f"[!] Error initializing TastyTrade: {str(e)}"
                print(error_msg)
                raise
        
        # If authentication failed or no fetcher, stop here
        if auth_failed or (data_source != "YFinance" and not data_fetcher):
            error_msg = f"[!] Failed to initialize {data_source} data fetcher"
            print(error_msg)
            raise RuntimeError(error_msg)
        
        # Now fetch data for each symbol
        fetch_errors = []
        
        for symbol in all_symbols:
            # Check cache first
            file_path = dir_manager.get_historical_data_path(
                symbol, period_str, start_date, end_date, data_source
            )
            
            # Check if we already have this data cached
            if os.path.exists(file_path):
                print(f"[*] Loading cached data for {symbol} from {file_path}")
                try:
                    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    # ... process cached data ...
                    result[symbol] = self._dataframe_to_candles(df, symbol, period_str)
                    print(f"[✓] Loaded {len(result[symbol])} candles from cache for {symbol}")
                    continue
                except Exception as e:
                    print(f"[!] Error loading cached data: {e}")
            
            # Fetch new data
            try:
                df = pd.DataFrame()
                
                if data_source == "TradeStation":
                    print(f"[*] Fetching data for {symbol} using TradeStation API...")
                    df = data_fetcher.fetch_bars(symbol, start_date, end_date, period_str)
                    
                elif data_source == "TastyTrade":
                    print(f"[*] Fetching data for {symbol} using TastyTrade API...")
                    timeframe_map = {
                        "1m": "1Min",
                        "5m": "5Min",
                        "15m": "15Min",
                        "30m": "30Min",
                        "1h": "1Hour",
                        "1d": "1Day"
                    }
                    tt_timeframe = timeframe_map.get(period_str, "5Min")
                    df = data_fetcher.fetch_bars(symbol, start_date, end_date, tt_timeframe)
                    
                elif data_source == "YFinance":
                    print(f"[*] Fetching data for {symbol} using YFinance...")
                    import yfinance as yf
                    ticker = yf.Ticker(symbol)
                    
                    interval_map = {
                        "1m": "1m",
                        "5m": "5m",
                        "15m": "15m",
                        "30m": "30m",
                        "1h": "60m",
                        "1d": "1d"
                    }
                    interval = interval_map.get(period_str, "5m")
                    
                    df = ticker.history(
                        start=start_date,
                        end=end_date + timedelta(days=1),
                        interval=interval
                    )
                
                if df.empty:
                    error_msg = f"[!] No data returned from {data_source} for {symbol}"
                    print(error_msg)
                    fetch_errors.append(f"{symbol}: No data returned")
                    continue
                
                # Process and save data
                print(f"[✓] Received {len(df)} candles for {symbol}")
                
                # Save to CSV
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                df.to_csv(file_path)
                print(f"[✓] Saved data to {file_path}")
                
                # Convert to candles format
                result[symbol] = self._dataframe_to_candles(df, symbol, period_str)
                
            except Exception as e:
                error_msg = f"[✗] Error fetching data for {symbol}: {str(e)}"
                print(error_msg)
                fetch_errors.append(f"{symbol}: {str(e)}")
                self.logger.error(error_msg, exc_info=True)
        
        # If we had any errors, report them
        if fetch_errors:
            print(f"\n[!] Failed to fetch data for {len(fetch_errors)} symbols:")
            for error in fetch_errors:
                print(f"    - {error}")
            
            # If we couldn't fetch critical data, raise an error
            if len(fetch_errors) == len(all_symbols):
                raise RuntimeError(f"Failed to fetch data for all symbols from {data_source}")
        
        return result

    

    def _dataframe_to_candles(self, df, symbol, period_str):
        """Convert DataFrame to list of candle dictionaries"""
        candles = []
        for timestamp, row in df.iterrows():
            candle = {
                "symbol": symbol,
                "period": period_str,
                "start_time": timestamp.isoformat(),
                "timestamp": timestamp.isoformat(),
                "open": float(row.get("open", row.get("Open", 0))),
                "high": float(row.get("high", row.get("High", 0))),
                "low": float(row.get("low", row.get("Low", 0))),
                "close": float(row.get("close", row.get("Close", 0))),
                "volume": float(row.get("volume", row.get("Volume", 0)))
            }
            candles.append(candle)
        return candles

            

    def get_candles_for_backtesting(self, symbols, period, start_date, end_date, data_source="YFinance"):
        """
        Get candles for backtesting for multiple symbols
        """
        result = {}
        
        # First try to get data from MongoDB
        for symbol in symbols:
            candles = self.get_candles_by_date_range(symbol, period, start_date, end_date)
            
            if candles and len(candles) > 0:
                result[symbol] = candles
                print(f"[✓] Got {symbol} from MongoDB: {len(candles)} candles")
        
        # If we don't have data for all symbols, fetch from external sources
        missing_symbols = [symbol for symbol in symbols if symbol not in result or not result[symbol]]
        if missing_symbols:
            print(f"[*] Fetching missing symbols from {data_source}: {missing_symbols}")
            try:
                # Pass config to fetch_historical_data_for_backtesting
                external_data = self.fetch_historical_data_for_backtesting(
                    missing_symbols, period, start_date, end_date, 
                    data_source=data_source,
                    config=getattr(self, 'config', None)  # Pass the config if available
                )
                
                # Merge the results
                for symbol, candles in external_data.items():
                    result[symbol] = candles
                    
            except (ConnectionError, RuntimeError) as e:
                # Re-raise authentication/connection errors to stop backtest
                print(f"[!] Critical error fetching data: {str(e)}")
                raise
            except Exception as e:
                self.logger.error(f"Failed to fetch external data: {e}")
                print(f"[!] Error fetching data: {str(e)}")
                raise
        
        return result


    def _normalize_timezone(self, df, target_tz='UTC'):
        """
        Normalize DataFrame timezone to avoid comparison issues
        
        Args:
            df: DataFrame with datetime index
            target_tz: Target timezone (default UTC)
            
        Returns:
            DataFrame with normalized timezone
        """
        if df.index.tz is not None:
            # Convert to target timezone
            df.index = df.index.tz_convert(target_tz)
            # Make timezone naive for easier comparison
            df.index = df.index.tz_localize(None)
        return df


    def _safe_date_filter(self, df, start_date, end_date):
        """
        Safely filter DataFrame by date range handling timezone issues
        
        Args:
            df: DataFrame to filter
            start_date: Start date (string or datetime)
            end_date: End date (string or datetime)
            
        Returns:
            Filtered DataFrame
        """
        # Convert dates to pandas timestamps
        if isinstance(start_date, str):
            start_ts = pd.Timestamp(start_date)
        else:
            start_ts = pd.Timestamp(start_date)
            
        if isinstance(end_date, str):
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
        else:
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
        
        # Remove timezone info from comparison timestamps
        start_ts = start_ts.tz_localize(None)
        end_ts = end_ts.tz_localize(None)
        
        # Normalize DataFrame timezone
        df = self._normalize_timezone(df)
        
        # Filter safely
        mask = (df.index >= start_ts) & (df.index < end_ts)
        return df[mask]