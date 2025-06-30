# Code/bot_core/tastytrade_data_fetcher.py

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import logging
import json
import threading
import queue
import websocket

class TastyTradeDataFetcher:
    """
    Fetches historical market data from TastyTrade DxLink API
    """
    
    def __init__(self, api=None, api_key=None, api_secret=None):
        """
        Initialize TastyTrade data fetcher
        
        Args:
            api: TastyTrade API instance (preferred)
            api_key: Not used (kept for compatibility)
            api_secret: Not used (kept for compatibility)
        """
        self.api = api
        self.ws = None
        self.data_queue = queue.Queue()
        self.candle_data = {}
        self.collection_complete = threading.Event()
        self.target_symbol = None
        self.expected_candles = 0
        self.received_candles = 0
        
        # Setup logging
        self.logger = logging.getLogger("TastyTradeDataFetcher")
        
    def _get_period_and_type(self, timeframe):
        """
        Convert timeframe string to period and type for DxLink
        
        Args:
            timeframe: Timeframe string like '1Min', '5Min', etc.
            
        Returns:
            tuple: (period, type) e.g., (5, 'm')
        """
        timeframe_map = {
            '1m': (1, 'm'),
            '5m': (5, 'm'),
            '15m': (15, 'm'),
            '30m': (30, 'm'),
            '1h': (1, 'h'),
            '1d': (1, 'd'),
            # Keep old format for backward compatibility
            '1Min': (1, 'm'),
            '5Min': (5, 'm'),
            '15Min': (15, 'm'),
            '30Min': (30, 'm'),
            '1Hour': (1, 'h'),
            '2Hour': (2, 'h'),
            '1Day': (1, 'd')
        }
        
        return timeframe_map.get(timeframe, (5, 'm'))
    
    def _calculate_from_time(self, start_date, end_date, timeframe):
        """
        Calculate the fromTime parameter for DxLink
        
        Args:
            start_date: Start date
            end_date: End date
            timeframe: Timeframe for candles
            
        Returns:
            int: Unix timestamp for fromTime
        """
        # Convert to datetime if string
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        elif isinstance(start_date, date) and not isinstance(start_date, datetime):
            # Convert date to datetime
            start_date = datetime.combine(start_date, datetime.min.time())
            
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, date) and not isinstance(end_date, datetime):
            # Convert date to datetime
            end_date = datetime.combine(end_date, datetime.min.time())
            
        # Calculate number of days
        days_back = (end_date - start_date).days
        
        # Add some buffer to ensure we get all data
        from_time = start_date - timedelta(days=1)
        
        # Convert to Unix timestamp (seconds)
        return int(from_time.timestamp())

    
    def _estimate_candle_count(self, start_date, end_date, timeframe):
        """
        Estimate the number of candles we should receive
        
        Args:
            start_date: Start date
            end_date: End date
            timeframe: Timeframe string
            
        Returns:
            int: Estimated number of candles
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        elif isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
            
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())
            
        days = (end_date - start_date).days + 1  # Include end date
        
        # Estimate based on timeframe (accounting for market hours)
        # Fixed: Use consistent format keys
        estimates = {
            '1m': days * 390,      # 6.5 hours * 60 minutes
            '5m': days * 78,       # 6.5 hours * 12 per hour
            '15m': days * 26,      # 6.5 hours * 4 per hour
            '30m': days * 13,      # 6.5 hours * 2 per hour
            '1h': days * 7,        # ~7 hours per day
            '2h': days * 4,        # ~4 candles per day
            '1d': days,            # 1 per day
            # Keep old format for compatibility
            '1Min': days * 390,
            '5Min': days * 78,
            '15Min': days * 26,
            '30Min': days * 13,
            '1Hour': days * 7,
            '2Hour': days * 4,
            '1Day': days
        }
        
        # Use a default if timeframe not found
        estimated = estimates.get(timeframe, days * 78)
        self.logger.info(f"Estimated {estimated} candles for {days} days with {timeframe} timeframe")
        return estimated
    
    def fetch_bars(self, symbol, start_date, end_date, timeframe='1Min'):
        """
        Fetch historical bars from TastyTrade
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            start_date: Start date (datetime or string)
            end_date: End date (datetime or string)
            timeframe: Bar timeframe ('1Min', '5Min', '15Min', '1Hour', '1Day')
            
        Returns:
            pd.DataFrame: DataFrame with OHLCV data
        """
        try:
            if not self.api:
                self.logger.error("No TastyTrade API instance provided")
                return pd.DataFrame()
                
            # Get streaming token
            token_data = self.api.get_quote_token()
            if not token_data:
                self.logger.error("Failed to get streaming token")
                return pd.DataFrame()
                
            token = token_data.get("token")
            dxlink_url = token_data.get("dxlink-url")
            
            # Calculate fromTime
            from_time = self._calculate_from_time(start_date, end_date, timeframe)
            
            # Get period and type
            period, candle_type = self._get_period_and_type(timeframe)
            
            # Create candle symbol
            candle_symbol = f"{symbol}{{={period}{candle_type}}}"
            
            self.logger.info(f"Fetching {symbol} candles: {candle_symbol} from {datetime.fromtimestamp(from_time)}")
            
            # Reset state
            self.candle_data = {}
            self.collection_complete.clear()
            self.target_symbol = symbol
            self.expected_candles = self._estimate_candle_count(start_date, end_date, timeframe)
            self.received_candles = 0
            self.last_update_time = time.time()
            
            # Connect to WebSocket
            def on_message(ws, message):
                self._handle_candle_message(message)
                
            def on_error(ws, error):
                self.logger.error(f"WebSocket error: {error}")
                self.collection_complete.set()  # Set to prevent infinite wait
                
            def on_close(ws, close_status_code, close_msg):
                self.logger.info("WebSocket closed")
                self.collection_complete.set()
                
            def on_open(ws):
                self.logger.info("WebSocket opened, setting up candle subscription")
                
                # Send setup message
                setup_msg = {
                    "type": "SETUP",
                    "channel": 0,
                    "version": "0.1-DXF-JS/0.3.0",
                    "keepaliveTimeout": 60,
                    "acceptKeepaliveTimeout": 60
                }
                ws.send(json.dumps(setup_msg))
                
                # Wait for auth state
                time.sleep(0.5)
                
                # Send auth
                auth_msg = {
                    "type": "AUTH",
                    "channel": 0,
                    "token": token
                }
                ws.send(json.dumps(auth_msg))
                
                # Wait for auth
                time.sleep(0.5)
                
                # Create channel
                channel_msg = {
                    "type": "CHANNEL_REQUEST",
                    "channel": 1,
                    "service": "FEED",
                    "parameters": {"contract": "AUTO"}
                }
                ws.send(json.dumps(channel_msg))
                
                # Wait for channel
                time.sleep(0.5)
                
                # Setup feed
                feed_setup_msg = {
                    "type": "FEED_SETUP",
                    "channel": 1,
                    "acceptAggregationPeriod": 0.1,
                    "acceptDataFormat": "COMPACT",
                    "acceptEventFields": {
                        "Candle": ["eventType", "eventSymbol", "time", "sequence", 
                                  "count", "open", "high", "low", "close", "volume", "vwap"]
                    }
                }
                ws.send(json.dumps(feed_setup_msg))
                
                # Wait for feed setup
                time.sleep(0.5)
                
                # Subscribe to candles
                subscription_msg = {
                    "type": "FEED_SUBSCRIPTION",
                    "channel": 1,
                    "reset": True,
                    "add": [{
                        "type": "Candle",
                        "symbol": candle_symbol,
                        "fromTime": from_time
                    }]
                }
                ws.send(json.dumps(subscription_msg))
                self.logger.info(f"Sent subscription for {candle_symbol} with fromTime {from_time}")
                
            # Create WebSocket
            self.ws = websocket.WebSocketApp(
                dxlink_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            # Start WebSocket in thread
            ws_thread = threading.Thread(target=self.ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()
            
            # Dynamic timeout based on expected data size
            timeout = max(60, min(300, self.expected_candles / 100))  # 60s minimum, 300s maximum
            self.logger.info(f"Waiting up to {timeout}s for {self.expected_candles} candles")
            
            # Wait for data collection with dynamic timeout and progress check
            start_wait = time.time()
            while not self.collection_complete.is_set() and (time.time() - start_wait) < timeout:
                time.sleep(1)
                
                # Check if we're still receiving data
                if hasattr(self, 'last_update_time'):
                    if time.time() - self.last_update_time > 10:  # No updates for 10 seconds
                        self.logger.warning("No data received for 10 seconds, assuming complete")
                        break
                        
                # Check if we've received a reasonable amount of data
                if self.received_candles > 0 and self.received_candles >= self.expected_candles * 0.5:
                    self.logger.info(f"Received {self.received_candles} candles (50% of expected), proceeding")
                    break
                    
            self.logger.info(f"Data collection finished. Received {self.received_candles} candles")
                
            # Close WebSocket
            if self.ws:
                self.ws.close()
                
            # Convert to DataFrame
            if self.candle_data:
                # Sort by timestamp
                sorted_times = sorted(self.candle_data.keys())
                
                data = []
                for timestamp in sorted_times:
                    candle = self.candle_data[timestamp]
                    # Convert epoch milliseconds to datetime
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    
                    # Filter by date range
                    if isinstance(start_date, str):
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    elif isinstance(start_date, date) and not isinstance(start_date, datetime):
                        start_dt = datetime.combine(start_date, datetime.min.time())
                    else:
                        start_dt = start_date
                        
                    if isinstance(end_date, str):
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                    elif isinstance(end_date, date) and not isinstance(end_date, datetime):
                        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
                    else:
                        end_dt = end_date + timedelta(days=1)
                        
                    if start_dt <= dt <= end_dt:
                        data.append({
                            'timestamp': dt,
                            'open': candle['open'],
                            'high': candle['high'],
                            'low': candle['low'],
                            'close': candle['close'],
                            'volume': candle['volume']
                        })
                
                if data:
                    df = pd.DataFrame(data)
                    df.set_index('timestamp', inplace=True)
                    
                    # Remove any duplicates
                    df = df[~df.index.duplicated(keep='first')]
                    
                    self.logger.info(f"Successfully fetched {len(df)} bars for {symbol}")
                    return df
                else:
                    self.logger.warning(f"No data in requested date range for {symbol}")
                    return pd.DataFrame()
            else:
                self.logger.warning(f"No candle data received for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching bars: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _handle_candle_message(self, message):
        """Handle incoming candle messages from WebSocket"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "FEED_DATA":
                feed_data = data.get("data", [])
                
                for event in feed_data:
                    if isinstance(event, list) and len(event) >= 2:
                        event_type = event[0]
                        
                        if event_type == "Candle" and len(event) >= 11:
                            # Parse candle data
                            try:
                                timestamp = event[2]  # Unix timestamp in milliseconds
                                
                                candle = {
                                    'symbol': event[1],
                                    'time': timestamp,
                                    'open': float(event[5]) if event[5] and str(event[5]) != "NaN" else 0.0,
                                    'high': float(event[6]) if event[6] and str(event[6]) != "NaN" else 0.0,
                                    'low': float(event[7]) if event[7] and str(event[7]) != "NaN" else 0.0,
                                    'close': float(event[8]) if event[8] and str(event[8]) != "NaN" else 0.0,
                                    'volume': float(event[9]) if event[9] and str(event[9]) != "NaN" else 0.0
                                }
                                
                                # Skip invalid candles
                                if candle['open'] == 0 and candle['high'] == 0 and candle['low'] == 0 and candle['close'] == 0:
                                    continue
                                
                                # Store candle
                                self.candle_data[timestamp] = candle
                                self.received_candles += 1
                                self.last_update_time = time.time()
                                
                                # Log progress periodically
                                if self.received_candles % 100 == 0:
                                    self.logger.info(f"Received {self.received_candles} candles...")
                                    
                                # Check if we've received enough candles
                                if self.received_candles >= self.expected_candles * 0.8:  # 80% threshold
                                    self.collection_complete.set()
                                    
                            except (ValueError, IndexError) as e:
                                self.logger.error(f"Error parsing candle data: {e}")
                                
        except Exception as e:
            self.logger.error(f"Error handling candle message: {e}")
    
    def test_connection(self):
        """Test if the API credentials are valid"""
        try:
            if self.api:
                # Try to get a quote token
                token_data = self.api.get_quote_token()
                return token_data is not None and "token" in token_data
            return False
        except:
            return False
    
    def fetch_multiple_timeframes(self, symbol, start_date, end_date):
        """
        Fetch data for multiple timeframes
        
        Returns:
            dict: Dictionary with timeframe as key and DataFrame as value
        """
        timeframes = {
            '1m': '1Min',
            '5m': '5Min',
            '15m': '15Min'
        }
        
        results = {}
        
        for key, tf in timeframes.items():
            self.logger.info(f"Fetching {key} data for {symbol}")
            df = self.fetch_bars(symbol, start_date, end_date, tf)
            if not df.empty:
                results[key] = df
            time.sleep(1)  # Small delay between requests
            
        return results