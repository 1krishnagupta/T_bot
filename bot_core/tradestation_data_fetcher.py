# Code/bot_core/tradestation_data_fetcher.py
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
import requests  # ADD THIS IMPORT
from .tradestation_api import TradeStationAPI

class TradeStationDataFetcher:
    """
    Fetches historical market data from TradeStation API
    Styled similar to TastyTradeDataFetcher for consistency
    """
    
    def __init__(self, api=None, api_key=None, api_secret=None):
        """
        Initialize TradeStation data fetcher
        
        Args:
            api: TradeStation API instance (preferred)
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
        self.logger = logging.getLogger("TradeStationDataFetcher")
        
        # If no API instance provided, create one
        if not self.api:
            self.api = TradeStationAPI()
            # Ensure the API is logged in
            if not self.api.check_and_refresh_session():
                self.logger.info("TradeStation API not authenticated, attempting login...")
                if not self.api.login():
                    self.logger.error("Failed to authenticate with TradeStation API")
    
    def _get_period_and_type(self, timeframe):
        """
        Convert timeframe string to TradeStation format
        
        Args:
            timeframe: Timeframe string like '1Min', '5Min', etc.
            
        Returns:
            dict: Period parameters for TradeStation
        """
        timeframe_map = {
            '1m': {'interval': 1, 'unit': 'Minute'},
            '5m': {'interval': 5, 'unit': 'Minute'},
            '15m': {'interval': 15, 'unit': 'Minute'},
            '30m': {'interval': 30, 'unit': 'Minute'},
            '1h': {'interval': 1, 'unit': 'Hour'},
            '2h': {'interval': 2, 'unit': 'Hour'},
            '1d': {'interval': 1, 'unit': 'Daily'},
            # Keep old format for backward compatibility
            '1Min': {'interval': 1, 'unit': 'Minute'},
            '5Min': {'interval': 5, 'unit': 'Minute'},
            '15Min': {'interval': 15, 'unit': 'Minute'},
            '30Min': {'interval': 30, 'unit': 'Minute'},
            '1Hour': {'interval': 1, 'unit': 'Hour'},
            '2Hour': {'interval': 2, 'unit': 'Hour'},
            '1Day': {'interval': 1, 'unit': 'Daily'}
        }
        
        return timeframe_map.get(timeframe, {'interval': 5, 'unit': 'Minute'})
    
    def _calculate_bars_needed(self, start_date, end_date, timeframe):
        """
        Calculate number of bars needed for the date range
        
        Args:
            start_date: Start date
            end_date: End date
            timeframe: Timeframe for candles
            
        Returns:
            int: Number of bars needed
        """
        # Convert to datetime if string
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        elif isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
            
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())
        
        # Calculate days
        days = (end_date - start_date).days + 1
        
        # Get timeframe params
        tf_params = self._get_period_and_type(timeframe)
        
        # Estimate bars based on timeframe
        if tf_params['unit'] == 'Minute':
            # 6.5 hours per trading day, 390 minutes
            bars_per_day = 390 / tf_params['interval']
            return int(days * bars_per_day * 1.2)  # 20% buffer
        elif tf_params['unit'] == 'Hour':
            # ~7 hours per trading day
            bars_per_day = 7 / tf_params['interval']
            return int(days * bars_per_day * 1.2)
        else:  # Daily
            return days + 10  # Buffer for weekends
    
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
        return self._calculate_bars_needed(start_date, end_date, timeframe)
    
    def fetch_bars(self, symbol, start_date, end_date, timeframe='5m'):
        """Fixed version using streaming barchart endpoint"""
        try:
            if not self.api:
                self.logger.error("No API instance available")
                return pd.DataFrame()
            
            # Ensure logged in
            if not self.api.check_and_refresh_session():
                self.logger.info("Attempting to login to TradeStation...")
                if not self.api.login():
                    self.logger.error("Failed to login to TradeStation")
                    return pd.DataFrame()
            
            # Convert dates
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            elif isinstance(start_date, date) and not isinstance(start_date, datetime):
                start_date = datetime.combine(start_date, datetime.min.time())
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            elif isinstance(end_date, date) and not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.min.time())
            
            # Format dates for TradeStation
            start_str = start_date.strftime("%m-%d-%Y")
            end_str = end_date.strftime("%m-%d-%Y")
            
            # Get interval and unit
            tf_params = self._get_period_and_type(timeframe)
            interval = tf_params['interval']
            unit = tf_params['unit']
            
            self.logger.info(f"Fetching {symbol} data from {start_str} to {end_str}, {interval} {unit}")
            
            # Use streaming endpoint from swagger
            endpoint = f"/v2/stream/barchart/{symbol}/{interval}/{unit}/{start_str}/{end_str}"
            
            # This is a streaming endpoint, need to handle differently
            headers = self.api.get_auth_headers()
            headers['Accept'] = 'application/vnd.tradestation.streams+json'
            
            url = f"{self.api.base_url}{endpoint}"
            
            # Make streaming request with proper error handling
            try:
                response = requests.get(url, headers=headers, stream=True, timeout=30)
                
                if response.status_code == 401:
                    self.logger.error("Authentication failed - token may be expired")
                    # Try to refresh token
                    if self.api._refresh_access_token():
                        # Retry with new token
                        headers = self.api.get_auth_headers()
                        headers['Accept'] = 'application/vnd.tradestation.streams+json'
                        response = requests.get(url, headers=headers, stream=True, timeout=30)
                    else:
                        # Full re-auth needed
                        if self.api.login():
                            headers = self.api.get_auth_headers()
                            headers['Accept'] = 'application/vnd.tradestation.streams+json'
                            response = requests.get(url, headers=headers, stream=True, timeout=30)
                        else:
                            self.logger.error("Failed to re-authenticate with TradeStation")
                            return pd.DataFrame()
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                    return pd.DataFrame()
                
                # Parse streaming response
                df_data = []
                error_count = 0
                max_errors = 5
                
                for line in response.iter_lines():
                    if line:
                        try:
                            line_str = line.decode('utf-8').strip()
                            
                            # Skip END marker
                            if line_str == 'END':
                                self.logger.debug(f"Received END marker for {symbol}")
                                break
                            
                            # Check for ERROR
                            if line_str.startswith('ERROR'):
                                self.logger.error(f"Stream error for {symbol}: {line_str}")
                                error_count += 1
                                if error_count >= max_errors:
                                    break
                                continue
                            
                            # Parse JSON data
                            data = json.loads(line_str)
                            
                            # Parse timestamp
                            ts_str = data.get('TimeStamp', '')
                            if '/Date(' in ts_str:
                                # Extract milliseconds from /Date(1234567890000)/
                                ms = int(ts_str.replace('/Date(', '').replace(')/', ''))
                                timestamp = datetime.fromtimestamp(ms / 1000)
                            else:
                                timestamp = pd.to_datetime(data.get('TimeStamp'))
                            
                            df_data.append({
                                'timestamp': timestamp,
                                'open': float(data.get('Open', 0)),
                                'high': float(data.get('High', 0)),
                                'low': float(data.get('Low', 0)),
                                'close': float(data.get('Close', 0)),
                                'volume': float(data.get('TotalVolume', 0))
                            })
                        except json.JSONDecodeError as e:
                            self.logger.debug(f"Error parsing JSON line: {e}")
                            continue
                        except Exception as e:
                            self.logger.debug(f"Error parsing line: {e}")
                            continue
                
                if not df_data:
                    self.logger.warning(f"No data received for {symbol}")
                    return pd.DataFrame()
                
                self.logger.info(f"Received {len(df_data)} bars for {symbol}")
                
                df = pd.DataFrame(df_data)
                df.set_index('timestamp', inplace=True)
                df.sort_index(inplace=True)
                
                # Validate data
                if df.empty:
                    self.logger.warning(f"Empty dataframe for {symbol}")
                elif df['close'].sum() == 0:
                    self.logger.warning(f"All close prices are zero for {symbol}")
                
                return df
                
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout fetching data for {symbol}")
                return pd.DataFrame()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error fetching data for {symbol}: {e}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching bars for {symbol}: {e}", exc_info=True)
            return pd.DataFrame()
        
    
    def test_connection(self):
        """Test if the API credentials are valid"""
        try:
            if self.api:
                # Check if we can login or are already logged in
                is_connected = self.api.check_and_refresh_session() or self.api.login()
                if is_connected:
                    self.logger.info("TradeStation connection test successful")
                else:
                    self.logger.error("TradeStation connection test failed")
                return is_connected
            return False
        except Exception as e:
            self.logger.error(f"Error testing TradeStation connection: {e}")
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
            time.sleep(0.5)  # Rate limiting
            
        return results
    
    def get_data_limitations(self):
        """
        Get TradeStation data limitations
        
        Returns:
            dict: Data limitations by timeframe
        """
        return {
            "1m": "Up to 40 days of 1-minute data",
            "5m": "Up to 6 months of 5-minute data",
            "15m": "Up to 1 year of 15-minute data",
            "30m": "Up to 2 years of 30-minute data",
            "1h": "Up to 3 years of hourly data",
            "1d": "Up to 10 years of daily data",
            "max_bars": "57,600 bars per request",
            "rate_limit": "120 requests per minute"
        }