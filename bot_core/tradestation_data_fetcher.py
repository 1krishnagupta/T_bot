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
                return pd.DataFrame()
            
            # Ensure logged in
            if not self.api.check_and_refresh_session():
                if not self.api.login():
                    return pd.DataFrame()
            
            # Convert dates
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Format dates for TradeStation
            start_str = start_date.strftime("%m-%d-%Y")
            end_str = end_date.strftime("%m-%d-%Y")
            
            # Get interval and unit
            tf_params = self._get_period_and_type(timeframe)
            interval = tf_params['interval']
            unit = tf_params['unit']
            
            # Use streaming endpoint from swagger
            endpoint = f"/v2/stream/barchart/{symbol}/{interval}/{unit}/{start_str}/{end_str}"
            
            # This is a streaming endpoint, need to handle differently
            headers = self.api.get_auth_headers()
            headers['Accept'] = 'application/vnd.tradestation.streams+json'
            
            url = f"{self.api.base_url}{endpoint}"
            
            # Make streaming request
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch data: {response.status_code}")
                return pd.DataFrame()
            
            # Parse streaming response
            df_data = []
            for line in response.iter_lines():
                if line:
                    try:
                        # Skip END marker
                        if line.decode('utf-8').strip() == 'END':
                            break
                        
                        data = json.loads(line)
                        
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
                    except Exception as e:
                        self.logger.debug(f"Error parsing line: {e}")
                        continue
            
            if not df_data:
                return pd.DataFrame()
            
            df = pd.DataFrame(df_data)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching bars: {e}")
            return pd.DataFrame()
        
    
    def test_connection(self):
        """Test if the API credentials are valid"""
        try:
            if self.api:
                # Check if we can login or are already logged in
                return self.api.check_and_refresh_session() or self.api.login()
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