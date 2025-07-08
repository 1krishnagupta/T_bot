# Code/bot_core/tradestation_data_fetcher.py

import os
import pandas as pd
import numpy as np
import requests
import time
import logging
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Union
import base64
import hmac
import hashlib

class TradeStationDataFetcher:
    """
    Fetches historical market data from TradeStation API
    """
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        Initialize TradeStation data fetcher
        
        Args:
            api_key: TradeStation API key
            api_secret: TradeStation API secret
        """
        # Use provided credentials or defaults
        self.api_key = api_key or "6ZhZile2KIwtU2xwdNGBYdNpPmRynB5J"
        self.api_secret = api_secret or "tY4dNJuhFst_XeqMmB95pF2_EriSqxc-ruQdnNILc4L5_vm9M0Iixwf9FUGw-WbQ"
        
        # TradeStation API endpoints
        self.base_url = "https://api.tradestation.com/v3"
        self.auth_url = "https://signin.tradestation.com/oauth/token"
        
        # Authentication
        self.access_token = None
        self.token_expiry = None
        
        # Setup logging
        self.logger = logging.getLogger("TradeStationDataFetcher")
        
    def _authenticate(self):
        """
        Authenticate with TradeStation API
        
        Returns:
            bool: True if authentication successful
        """
        try:
            # Check if we have a valid token
            if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
                return True
            
            # Request new token
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            # Create basic auth header
            auth_string = f"{self.api_key}:{self.api_secret}"
            auth_bytes = auth_string.encode('ascii')
            auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
            headers["Authorization"] = f"Basic {auth_b64}"
            
            data = {
                "grant_type": "client_credentials",
                "scope": "marketdata"
            }
            
            response = requests.post(self.auth_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                expires_in = token_data.get("expires_in", 3600)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)  # Buffer
                
                self.logger.info("Successfully authenticated with TradeStation API")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error during authentication: {e}")
            return False
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make authenticated request to TradeStation API
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Response data or None if error
        """
        # Ensure we're authenticated
        if not self._authenticate():
            return None
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"API request failed: {response.status_code} {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error making API request: {e}")
            return None
    
    def _convert_timeframe(self, timeframe: str) -> Dict[str, Union[str, int]]:
        """
        Convert timeframe to TradeStation format
        
        Args:
            timeframe: Timeframe string like '1m', '5m', etc.
            
        Returns:
            Dict with interval and unit for TradeStation API
        """
        timeframe_map = {
            '1m': {'interval': 1, 'unit': 'Minute'},
            '5m': {'interval': 5, 'unit': 'Minute'},
            '15m': {'interval': 15, 'unit': 'Minute'},
            '30m': {'interval': 30, 'unit': 'Minute'},
            '1h': {'interval': 1, 'unit': 'Hour'},
            '2h': {'interval': 2, 'unit': 'Hour'},
            '1d': {'interval': 1, 'unit': 'Daily'},
            # Legacy format support
            '1Min': {'interval': 1, 'unit': 'Minute'},
            '5Min': {'interval': 5, 'unit': 'Minute'},
            '15Min': {'interval': 15, 'unit': 'Minute'},
            '30Min': {'interval': 30, 'unit': 'Minute'},
            '1Hour': {'interval': 1, 'unit': 'Hour'},
            '2Hour': {'interval': 2, 'unit': 'Hour'},
            '1Day': {'interval': 1, 'unit': 'Daily'}
        }
        
        return timeframe_map.get(timeframe, {'interval': 5, 'unit': 'Minute'})
    
    def fetch_bars(self, symbol: str, start_date: Union[str, date, datetime], 
                   end_date: Union[str, date, datetime], timeframe: str = '5m') -> pd.DataFrame:
        """
        Fetch historical bars from TradeStation
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            start_date: Start date (datetime, date, or string)
            end_date: End date (datetime, date, or string)
            timeframe: Bar timeframe ('1m', '5m', '15m', '1h', '1d')
            
        Returns:
            pd.DataFrame: DataFrame with OHLCV data
        """
        try:
            # Convert dates to datetime if needed
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            elif isinstance(start_date, date) and not isinstance(start_date, datetime):
                start_date = datetime.combine(start_date, datetime.min.time())
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            elif isinstance(end_date, date) and not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.min.time())
            
            # Get timeframe parameters
            tf_params = self._convert_timeframe(timeframe)
            
            # Calculate number of bars needed
            days_diff = (end_date - start_date).days
            
            # Estimate bars based on timeframe
            if tf_params['unit'] == 'Minute':
                # 6.5 hours per trading day, 390 minutes
                bars_per_day = 390 / tf_params['interval']
                max_bars = int(days_diff * bars_per_day * 1.2)  # 20% buffer
            elif tf_params['unit'] == 'Hour':
                # ~7 hours per trading day
                bars_per_day = 7 / tf_params['interval']
                max_bars = int(days_diff * bars_per_day * 1.2)
            else:  # Daily
                max_bars = days_diff + 10  # Buffer for weekends
            
            # TradeStation has a limit of 57,600 bars per request
            max_bars = min(max_bars, 57600)
            
            self.logger.info(f"Fetching {symbol} {timeframe} data from {start_date} to {end_date}")
            
            # Build request parameters
            params = {
                'symbol': symbol,
                'interval': tf_params['interval'],
                'unit': tf_params['unit'],
                'barsback': max_bars,
                'lastdate': end_date.strftime('%Y-%m-%d'),
                'sessiontemplate': 'Default'  # Regular trading hours
            }
            
            # Make API request
            data = self._make_request('/marketdata/barcharts', params)
            
            if not data or 'Bars' not in data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            bars = data['Bars']
            if not bars:
                return pd.DataFrame()
            
            # Create DataFrame
            df_data = []
            for bar in bars:
                timestamp = pd.to_datetime(bar['TimeStamp'])
                
                # Filter by date range
                if start_date <= timestamp <= end_date + timedelta(days=1):
                    df_data.append({
                        'timestamp': timestamp,
                        'open': float(bar['Open']),
                        'high': float(bar['High']),
                        'low': float(bar['Low']),
                        'close': float(bar['Close']),
                        'volume': float(bar.get('TotalVolume', 0))
                    })
            
            if not df_data:
                self.logger.warning(f"No data in requested date range for {symbol}")
                return pd.DataFrame()
            
            df = pd.DataFrame(df_data)
            df.set_index('timestamp', inplace=True)
            
            # Remove any duplicates
            df = df[~df.index.duplicated(keep='first')]
            
            # Sort by timestamp
            df.sort_index(inplace=True)
            
            self.logger.info(f"Successfully fetched {len(df)} bars for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching bars: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def fetch_multiple_timeframes(self, symbol: str, start_date: Union[str, date, datetime],
                                 end_date: Union[str, date, datetime]) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple timeframes
        
        Returns:
            dict: Dictionary with timeframe as key and DataFrame as value
        """
        timeframes = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m'
        }
        
        results = {}
        
        for key, tf in timeframes.items():
            self.logger.info(f"Fetching {key} data for {symbol}")
            df = self.fetch_bars(symbol, start_date, end_date, tf)
            if not df.empty:
                results[key] = df
            time.sleep(0.5)  # Rate limiting
            
        return results
    
    def test_connection(self) -> bool:
        """Test if the API credentials are valid"""
        try:
            return self._authenticate()
        except:
            return False
    
    def get_data_limitations(self) -> Dict[str, str]:
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