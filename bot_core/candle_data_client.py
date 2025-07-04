# File: Code/bot_core/candle_data_client.py

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import json
import logging
from typing import Dict, List, Optional, Callable, Tuple, Any, Union
from Code.bot_core.directory_manager import BacktestDirectoryManager
from Code.bot_core.tastytrade_data_fetcher import TastyTradeDataFetcher

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
    
    
    def fetch_historical_data_for_backtesting(self, symbols, period, start_date, end_date=None, data_source="TastyTrade", **kwargs):
        """
        Fetch historical data for backtesting from external sources and save to CSV files
        FIXED: Only use the selected data source, don't fall back to other sources
        """
        # Import directory manager
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
        
        # ALWAYS fetch sector ETFs regardless of what symbols are requested
        sector_etfs = ["XLK", "XLF", "XLV", "XLY"]
        
        # Also fetch Mag7 stocks if Mag7 strategy is enabled
        mag7_stocks = []
        if "trading_config" in self.config and self.config["trading_config"].get("use_mag7_confirmation", False):
            mag7_stocks = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"]
        
        all_symbols = list(symbols) + sector_etfs + mag7_stocks
        all_symbols = list(set(all_symbols))  # Remove duplicates
        
        print(f"[*] Fetching data for symbols: {', '.join(all_symbols)}")
        
        # Check for YFinance limitations and adjust date range if needed
        if data_source == "YFinance":
            if period_str == "1m" and days_diff > 7:
                print(f"[!] WARNING: YFinance only provides 7 days of 1-minute data")
                print(f"[!] Adjusting start date from {start_date} to last 7 days")
                start_date = end_date - timedelta(days=7)
                days_diff = 7
            elif period_str in ["5m", "15m"] and days_diff > 60:
                print(f"[!] WARNING: YFinance only provides 60 days of {period_str} data")
                print(f"[!] Adjusting start date from {start_date} to last 60 days")
                start_date = end_date - timedelta(days=60)
                days_diff = 60
        
        for symbol in all_symbols:
            # Get proper file path from directory manager
            file_path = dir_manager.get_historical_data_path(
                symbol, period_str, start_date, end_date, data_source
            )
            
            # Check if we already have this data cached
            if os.path.exists(file_path):
                print(f"[*] Loading cached data for {symbol} from {file_path}")
                try:
                    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    
                    # Handle different timestamp column names
                    if df.index.name in ['Date', 'Datetime', 'date', 'datetime']:
                        df.index.name = 'timestamp'
                    
                    # If timestamp is a column, not index
                    timestamp_cols = ['timestamp', 'Timestamp', 'date', 'Date', 'datetime', 'Datetime']
                    for col in timestamp_cols:
                        if col in df.columns:
                            df = df.set_index(col)
                            df.index.name = 'timestamp'
                            break
                            
                    # Ensure index is datetime
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index)

                    # Normalize timezone
                    df = self._normalize_timezone(df)
                    # Convert DataFrame to list of dicts
                    candles = []
                    for timestamp, row in df.iterrows():
                        candle = {
                            "symbol": symbol,
                            "period": period_str,
                            "start_time": timestamp.isoformat(),
                            "timestamp": timestamp.isoformat(),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row["volume"]) if "volume" in row else 0
                        }
                        candles.append(candle)
                    result[symbol] = candles
                    print(f"[✓] Loaded {len(candles)} candles from cache for {symbol}")
                    continue
                except Exception as e:
                    print(f"[!] Error loading cached data: {e}")
            
            # Fetch new data ONLY from the selected source
            try:
                df = pd.DataFrame()  # Initialize empty DataFrame
                
                if data_source == "TastyTrade":
                    print(f"[*] Fetching data for {symbol} using TastyTrade API")
                    
                    # Use the market data client's API instance if available
                    if hasattr(self, 'market_data_client') and self.market_data_client and hasattr(self.market_data_client, 'api'):
                        api = self.market_data_client.api
                    else:
                        # Try to get API from kwargs
                        api = kwargs.get('api')
                        
                    if not api:
                        print(f"[!] No TastyTrade API instance available for {symbol}")
                        continue  # Skip this symbol if no API
                    else:
                        # Initialize fetcher
                        fetcher = TastyTradeDataFetcher(api=api)
                        
                        # Map period to TastyTrade timeframe
                        timeframe_map = {
                            "1m": "1Min",
                            "5m": "5Min", 
                            "15m": "15Min",
                            "30m": "30Min",
                            "1h": "1Hour",
                            "1d": "1Day"
                        }
                        tt_timeframe = timeframe_map.get(period_str, "5Min")
                        
                        # For large date ranges, fetch in chunks
                        if days_diff > 30 and period_str in ["1m", "5m"]:
                            # Fetch in monthly chunks
                            all_dfs = []
                            current_start = start_date
                            
                            while current_start < end_date:
                                current_end = min(current_start + timedelta(days=30), end_date)
                                print(f"  Fetching chunk: {current_start} to {current_end}")
                                
                                chunk_df = fetcher.fetch_bars(symbol, current_start, current_end, tt_timeframe)
                                if not chunk_df.empty:
                                    all_dfs.append(chunk_df)
                                
                                current_start = current_end + timedelta(days=1)
                                time.sleep(1)  # Rate limiting
                            
                            if all_dfs:
                                df = pd.concat(all_dfs)
                                df = df[~df.index.duplicated(keep='first')]
                        else:
                            # Single fetch
                            df = fetcher.fetch_bars(symbol, start_date, end_date, tt_timeframe)
                        
                        if df.empty:
                            print(f"[!] No data returned from TastyTrade for {symbol}")
                            continue  # Skip this symbol
                            
                elif data_source == "YFinance":
                    print(f"[*] Fetching data for {symbol} using YFinance")
                    
                    # YFinance code
                    ticker = yf.Ticker(symbol)
                    
                    # Map period to YFinance interval
                    interval_map = {
                        "1m": "1m",
                        "5m": "5m",
                        "15m": "15m",
                        "30m": "30m",
                        "1h": "60m",
                        "1d": "1d"
                    }
                    interval = interval_map.get(period_str, "5m")
                    
                    # Fetch data with already adjusted date range
                    try:
                        df = ticker.history(
                            start=start_date,
                            end=end_date + timedelta(days=1),
                            interval=interval
                        )
                    except Exception as yf_error:
                        print(f"[!] YFinance error: {yf_error}")
                        # Try with period parameter as fallback
                        if interval == "1m":
                            df = ticker.history(period="7d", interval=interval)
                        elif interval == "5m":
                            df = ticker.history(period="60d", interval=interval)
                        else:
                            df = ticker.history(period="max", interval=interval)
                        
                        # Filter by date range after fetching
                        if not df.empty:
                            df = self._safe_date_filter(df, start_date, end_date)
                    
                    if df.empty:
                        print(f"[!] No data found for {symbol} from YFinance")
                        continue
                else:
                    print(f"[!] Unknown data source: {data_source}")
                    continue
                
                # Process dataframe
                if not df.empty:
                    # Normalize timezone before processing
                    df = self._normalize_timezone(df)
                    
                    # Standardize column names
                    df.columns = [col.lower() for col in df.columns]
                    
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    # Save to CSV
                    df.to_csv(file_path)
                    print(f"[✓] Saved {len(df)} candles to {file_path}")
                    
                    # Convert to candles format
                    candles = []
                    for timestamp, row in df.iterrows():
                        candle = {
                            "symbol": symbol,
                            "period": period_str,
                            "start_time": timestamp.isoformat(),
                            "timestamp": timestamp.isoformat(),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row["volume"]) if "volume" in row else 0
                        }
                        candles.append(candle)
                        
                    result[symbol] = candles
                    print(f"[✓] Successfully fetched {len(candles)} candles for {symbol}")
                else:
                    print(f"[!] No data retrieved for {symbol}")
                    
            except Exception as e:
                print(f"[✗] Error fetching data for {symbol}: {e}")
                self.logger.error(f"Error fetching data for {symbol}: {e}", exc_info=True)
                import traceback
                traceback.print_exc()
        
        return result

            

    def get_candles_for_backtesting(self, symbols, period, start_date, end_date, data_source="YFinance"):
        
        """
        Get candles for backtesting for multiple symbols
        
        Args:
            symbols (List[str]): List of instrument symbols
            period (str): Candle period (e.g., "1m", "5m")
            start_date (Union[str, datetime.date]): Start date
            end_date (Union[str, datetime.date], optional): End date, defaults to today
            
        Returns:
            Dict[str, List[Dict]]: Dictionary mapping symbols to lists of candles
        """
        result = {}
        
        # First try to get data from MongoDB
        for symbol in symbols:
            candles = self.get_candles_by_date_range(symbol, period, start_date, end_date)
            
            # If we got candles from the database, use them
            if candles and len(candles) > 0:
                result[symbol] = candles
                print(f"[✓] Got {symbol} from MongoDB: {len(candles)} candles")
        
        # If we don't have data for all symbols, fetch from external sources
        missing_symbols = [symbol for symbol in symbols if symbol not in result or not result[symbol]]
        if missing_symbols:
            print(f"[*] Fetching missing symbols from {data_source}: {missing_symbols}")
            external_data = self.fetch_historical_data_for_backtesting(
                missing_symbols, period, start_date, end_date, 
                data_source=data_source  # ADD THIS PARAMETER
            )
            
            # Merge the results
            for symbol, candles in external_data.items():
                result[symbol] = candles
                
                # Optionally save to MongoDB for future use
                if self.db and candles:
                    try:
                        # Save candles to MongoDB
                        for candle in candles[:100]:  # Limit to avoid overwhelming DB
                            candle_data = {
                                'symbol': symbol,
                                'period': period if isinstance(period, str) else f"{period}m",
                                'start_time': candle['start_time'],
                                'timestamp': candle['timestamp'],
                                'open': candle['open'],
                                'high': candle['high'],
                                'low': candle['low'],
                                'close': candle['close'],
                                'volume': candle.get('volume', 0)
                            }
                            self.db.insert_one(COLLECTIONS['CANDLES'], candle_data)
                        print(f"[✓] Saved {min(100, len(candles))} candles to MongoDB for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Failed to save to MongoDB: {e}")
                
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