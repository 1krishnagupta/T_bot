# File: Code/bot_core/candle_data_client.py

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import json
import logging
from typing import Dict, List, Optional, Callable, Tuple, Any, Union
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
        self.market_data = market_data_client
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
    
    def _calculate_max_bars_for_timeframe(self, timeframe_str):
        """
        Calculate maximum bars allowed for a timeframe
        
        TradeStation limit: 57,600 bars per request
        But we need to be much more conservative!
        """
        # Based on TradeStation's actual limits and including extended hours
        # Extended hours: 4:00 AM - 8:00 PM = 16 hours = 960 minutes per day
        
        if timeframe_str == "1m":
            # 1-minute bars: 960 per day with extended hours
            # 57,600 / 960 = 60 days theoretical
            # But let's be very conservative to avoid errors
            return 30  # 30 days to be safe
        elif timeframe_str == "5m":
            # 5-minute bars: 192 per day with extended hours
            # 57,600 / 192 = 300 days theoretical
            return 120  # 4 months to be safe
        elif timeframe_str == "15m":
            # 15-minute bars: 64 per day with extended hours
            # 57,600 / 64 = 900 days theoretical
            return 300  # 10 months to be safe
        elif timeframe_str == "30m":
            # 30-minute bars: 32 per day with extended hours
            # 57,600 / 32 = 1,800 days theoretical
            return 600  # 20 months to be safe
        elif timeframe_str == "1h" or timeframe_str == "60m":
            # 60-minute bars: 16 per day with extended hours
            # 57,600 / 16 = 3,600 days theoretical
            return 900  # 30 months to be safe
        else:
            # Default to 5m calculation
            return 120
    
    def _chunk_date_range(self, start_date, end_date, timeframe_str):
        """
        Chunk a date range into smaller pieces that fit within TradeStation limits
        
        Returns:
            List of (chunk_start, chunk_end) tuples
        """
        max_days = self._calculate_max_bars_for_timeframe(timeframe_str)
        
        # Convert to datetime if needed
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_dt = datetime.combine(start_date, datetime.min.time())
        
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_dt = datetime.combine(end_date, datetime.min.time())
        
        chunks = []
        current_start = start_dt
        
        while current_start < end_dt:
            # Calculate chunk end, but don't exceed max_days
            chunk_end = current_start + timedelta(days=max_days - 1)  # -1 to be inclusive
            if chunk_end > end_dt:
                chunk_end = end_dt
            
            chunks.append((current_start, chunk_end))
            
            # Start next chunk the day after this one ends
            current_start = chunk_end + timedelta(days=1)
        
        return chunks

    def fetch_historical_data_for_backtesting(self, symbols, period, start_date, end_date=None, data_source="TradeStation", **kwargs):
        """
        Fetch historical data for backtesting from external sources and save to CSV files
        Now handles TradeStation's 57,600 bar limit by chunking requests
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
        
        # Get system date (which might be wrong)
        system_today = datetime.now().date()
        
        # Determine period string
        period_str = f"{period}m" if isinstance(period, int) else period
        
        # Calculate date range info
        days_diff = (end_date - start_date).days
        self.logger.info(f"Fetching {days_diff} days of {period_str} data from {start_date} to {end_date}")
        
        # Check if we've already shown data source limitations
        if not hasattr(self, '_shown_data_limitations'):
            self._shown_data_limitations = True
            
            # Show data source limitations only once
            if data_source == "TradeStation":
                print(f"\n[*] Using TradeStation API")
                print(f"[*] System date: {system_today}")
                print(f"[*] Requested range: {start_date} to {end_date} ({days_diff} days)")
                print(f"[*] TradeStation Data Limitations:")
                print(f"    - 1m data: Maximum 30 days per request")
                print(f"    - 5m data: Maximum 120 days per request")
                print(f"    - 15m data: Maximum 300 days per request")
                print(f"    - Maximum 57,600 bars per request")
                print(f"    - Data includes extended trading hours\n")
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
        
        # FIXED: Check strategy type BEFORE fetching additional symbols
        use_mag7 = False
        config = kwargs.get('config')
        if not config and hasattr(self, 'config'):
            config = self.config
        
        if config:
            use_mag7 = config.get("trading_config", {}).get("use_mag7_confirmation", False)
        
        # Create a set of all symbols we need to fetch
        all_symbols_set = set(symbols)  # Start with requested symbols
        
        if use_mag7:
            # Fetch Mag7 stocks for Mag7 strategy
            mag7_stocks = config.get("trading_config", {}).get("mag7_stocks", 
                ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
            
            # Add Mag7 stocks to symbols set
            all_symbols_set.update(mag7_stocks)
            
            # Only print this once
            if not hasattr(self, '_shown_mag7_info'):
                self._shown_mag7_info = True
                print(f"[*] Using Mag7 strategy - fetching Mag7 stocks: {', '.join(mag7_stocks)}")
        else:
            # Fetch sector ETFs for sector alignment strategy
            sector_etfs = ["XLK", "XLF", "XLV", "XLY"]
            if config:
                selected_sectors = config.get("trading_config", {}).get("selected_sectors", sector_etfs)
                sector_etfs = selected_sectors
            
            # Add sector ETFs to symbols set
            all_symbols_set.update(sector_etfs)
            
            # Only print this once
            if not hasattr(self, '_shown_sector_info'):
                self._shown_sector_info = True
                print(f"[*] Using Sector Alignment strategy - fetching ETFs: {', '.join(sector_etfs)}")
        
        all_symbols = list(all_symbols_set)  # Convert back to list
        
        print(f"[*] Fetching data for {len(all_symbols)} unique symbols...")
        
        # Initialize data fetcher based on source
        data_fetcher = None
        auth_failed = False
        
        if data_source == "TradeStation":
            try:
                from Code.bot_core.tradestation_data_fetcher import TradeStationDataFetcher
                
                # Pass the API instance if available
                api_instance = kwargs.get('api')
                if hasattr(self, 'market_data') and hasattr(self.market_data, 'api'):
                    api_instance = self.market_data.api
                
                tradestation_fetcher = TradeStationDataFetcher(api=api_instance)
                
                # Test connection before proceeding
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
                    raise ConnectionError(error_msg)
                else:
                    data_fetcher = tradestation_fetcher
                    self.logger.info("TradeStation connection successful")
            except Exception as e:
                error_msg = f"[!] Error initializing TradeStation: {str(e)}"
                print(error_msg)
                self.logger.error(error_msg, exc_info=True)
                raise
        else:
            print("[!] Using YFinance data source")
                
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
                # Only show cache message once
                if not hasattr(self, '_shown_cache_message'):
                    self._shown_cache_message = True
                    print(f"[*] Using cached data where available...")
                
                # Log to logger instead of console
                self.logger.info(f"Loading cached data for {symbol}")
                
                try:
                    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    # ... process cached data ...
                    result[symbol] = self._dataframe_to_candles(df, symbol, period_str)
                    continue
                except Exception as e:
                    print(f"[!] Error loading cached data: {e}")
            
            # Fetch new data
            try:
                df = pd.DataFrame()
                
                if data_source == "TradeStation":
                    print(f"[*] Fetching data for {symbol}...")
                    
                    # Check if we need to chunk the request
                    max_days = self._calculate_max_bars_for_timeframe(period_str)
                    
                    # Allow override from config
                    if config and 'max_days_per_chunk' in config.get('trading_config', {}):
                        override_days = config['trading_config']['max_days_per_chunk']
                        print(f"[*] Using config override: {override_days} days per chunk")
                        max_days = override_days
                    
                    if days_diff > max_days:
                        print(f"[*] Date range ({days_diff} days) exceeds limit ({max_days} days), chunking requests...")
                        
                        # Get chunks
                        chunks = self._chunk_date_range(start_date, end_date, period_str)
                        print(f"[*] Split into {len(chunks)} chunks")
                        
                        # Fetch each chunk
                        all_dfs = []
                        for i, (chunk_start, chunk_end) in enumerate(chunks):
                            chunk_days = (chunk_end - chunk_start).days + 1
                            print(f"\n    Chunk {i+1}/{len(chunks)}: {chunk_start.date()} to {chunk_end.date()} ({chunk_days} days)")
                            
                            # Calculate approximate bars for this chunk
                            if period_str == "1m":
                                approx_bars = chunk_days * 960  # Extended hours
                            elif period_str == "5m":
                                approx_bars = chunk_days * 192
                            elif period_str == "15m":
                                approx_bars = chunk_days * 64
                            else:
                                approx_bars = chunk_days * 390 / int(period_str.replace('m', ''))
                            
                            print(f"      Estimated bars: {int(approx_bars):,}")
                            
                            if approx_bars > 57600:
                                print(f"      ⚠️  WARNING: This chunk may still exceed 57,600 bar limit!")
                            
                            try:
                                chunk_df = data_fetcher.fetch_bars(symbol, chunk_start, chunk_end, period_str)
                                
                                if not chunk_df.empty:
                                    all_dfs.append(chunk_df)
                                    print(f"      ✓ Received {len(chunk_df)} bars")
                                else:
                                    print(f"      ! No data received for this chunk")
                            except Exception as e:
                                error_str = str(e)
                                print(f"      ✗ Error fetching chunk: {error_str}")
                                
                                if "Request exceeds history limit" in error_str:
                                    print(f"      ! Chunk still too large. Try smaller chunks.")
                                    print(f"      ! Suggestion: Set max_days_per_chunk in config to {max_days // 2}")
                                elif "400" in error_str:
                                    print(f"      ! Bad request. Check if dates are valid for this symbol.")
                            
                            # Small delay between requests
                            if i < len(chunks) - 1:
                                time.sleep(0.5)
                        
                        # Combine all chunks
                        if all_dfs:
                            df = pd.concat(all_dfs)
                            df = df[~df.index.duplicated(keep='first')]  # Remove duplicates
                            df.sort_index(inplace=True)
                            print(f"\n    ✓ Combined {len(all_dfs)} chunks into {len(df)} total bars")
                        else:
                            print(f"\n    ✗ Failed to fetch any data for {symbol}")
                    else:
                        # Single request is fine
                        print(f"    Date range fits in single request ({days_diff} days <= {max_days} days)")
                        try:
                            df = data_fetcher.fetch_bars(symbol, start_date, end_date, period_str)
                            if not df.empty:
                                print(f"    ✓ Received {len(df)} bars")
                            else:
                                print(f"    ! No data received")
                        except Exception as e:
                            error_str = str(e)
                            print(f"    ✗ Error: {error_str}")
                            if "Request exceeds history limit" in error_str:
                                print(f"    ! Still exceeded limit. Try a shorter date range.")
                                print(f"    ! Maximum for {period_str}: {max_days} days")
            
                elif data_source == "YFinance":
                    print(f"[*] Fetching data for {symbol}...")
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