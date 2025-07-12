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
import webbrowser
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class TradeStationDataFetcher:
    """
    Fetches historical market data from TradeStation API with proper OAuth authentication
    """
    
    def __init__(self, api_key: str = None, api_secret: str = None, refresh_token: str = None):
        """
        Initialize TradeStation data fetcher
        
        Args:
            api_key: TradeStation API key (Client ID)
            api_secret: TradeStation API secret (Client Secret)
            refresh_token: Refresh token (optional, will prompt for auth if not provided)
        """
        # Hardcoded credentials as requested
        self.CLIENT_ID = api_key or "6ZhZile2KIwtU2xwdNGBYdNpPmRynB5J"
        self.CLIENT_SECRET = api_secret or "tY4dNJuhFst_XeqMmB95pF2_EriSqxc-ruQdnNILc4L5_vm9M0Iixwf9FUGw-WbQ"
        
        # TradeStation API endpoints
        self.base_url = "https://api.tradestation.com"  # Production
        # self.base_url = "https://sim-api.tradestation.com"  # Simulation
        self.auth_url = "https://signin.tradestation.com"
        
        # Authentication tokens
        self.refresh_token = refresh_token
        self.access_token = None
        self.token_expiry = None
        
        # For OAuth callback
        self.auth_code = None
        self.callback_received = threading.Event()
        
        # Setup logging
        self.logger = logging.getLogger("TradeStationDataFetcher")
        
        # File to store refresh token
        self.token_file = os.path.join(os.path.dirname(__file__), '.tradestation_token.json')
        
        # Load saved refresh token if exists
        if not self.refresh_token:
            self._load_refresh_token()
    
    def _load_refresh_token(self):
        """Load refresh token from file if it exists"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    self.refresh_token = data.get('refresh_token')
                    self.logger.info("Loaded refresh token from file")
        except Exception as e:
            self.logger.error(f"Error loading refresh token: {e}")
    
    def _save_refresh_token(self):
        """Save refresh token to file"""
        try:
            with open(self.token_file, 'w') as f:
                json.dump({'refresh_token': self.refresh_token}, f)
            self.logger.info("Saved refresh token to file")
        except Exception as e:
            self.logger.error(f"Error saving refresh token: {e}")
    
    def _get_authorization_code(self):
        """
        Get authorization code through OAuth flow
        """
        # Build authorization URL
        auth_params = {
            'response_type': 'code',
            'client_id': self.CLIENT_ID,
            'audience': 'https://api.tradestation.com',
            'redirect_uri': 'http://localhost:3000',
            'scope': 'openid MarketData profile ReadAccount Trade offline_access Matrix OptionSpreads'
        }
        
        auth_url = f"{self.auth_url}/authorize?{urllib.parse.urlencode(auth_params)}"
        
        print("\n" + "="*80)
        print("TRADESTATION AUTHENTICATION REQUIRED")
        print("="*80)
        print("\nPlease login to TradeStation to authorize this application.")
        print("\nOpening browser to:")
        print(auth_url)
        print("\nIf browser doesn't open automatically, please copy and paste the URL above.")
        print("\nAfter logging in, you'll be redirected to localhost:3000 with a code.")
        print("="*80 + "\n")
        
        # Try to open browser automatically
        try:
            webbrowser.open(auth_url)
        except:
            pass
        
        # Start local server to catch callback
        server_thread = threading.Thread(target=self._run_callback_server)
        server_thread.daemon = True
        server_thread.start()
        
        # Wait for callback
        print("Waiting for authorization...")
        self.callback_received.wait(timeout=300)  # 5 minute timeout
        
        if not self.auth_code:
            # If server didn't catch it, ask user to paste
            print("\nIf you see a 'This site can't be reached' error, that's normal!")
            print("Please copy the ENTIRE URL from your browser and paste it here:")
            callback_url = input().strip()
            
            # Extract code from URL
            if 'code=' in callback_url:
                self.auth_code = callback_url.split('code=')[1].split('&')[0]
            else:
                raise Exception("No authorization code found in URL")
        
        return self.auth_code
    
    def _run_callback_server(self):
        """Run a simple HTTP server to catch OAuth callback"""
        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(handler_self):
                # Extract code from query string
                if 'code=' in handler_self.path:
                    code = handler_self.path.split('code=')[1].split('&')[0]
                    self.auth_code = code
                    self.callback_received.set()
                    
                    # Send response
                    handler_self.send_response(200)
                    handler_self.send_header('Content-type', 'text/html')
                    handler_self.end_headers()
                    handler_self.wfile.write(b'<html><body><h1>Authorization successful!</h1><p>You can close this window.</p></body></html>')
                else:
                    handler_self.send_response(400)
                    handler_self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress log messages
        
        try:
            server = HTTPServer(('localhost', 3000), CallbackHandler)
            server.timeout = 300  # 5 minute timeout
            server.handle_request()  # Handle one request then stop
        except:
            pass  # Server might fail if port is in use, that's OK
    
    def _get_refresh_token(self, auth_code):
        """
        Exchange authorization code for refresh token
        """
        url = f"{self.auth_url}/oauth/token"
        
        payload = {
            'grant_type': 'authorization_code',
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET,
            'code': auth_code,
            'redirect_uri': 'http://localhost:3000'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(url, data=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            self.refresh_token = data['refresh_token']
            self.access_token = data['access_token']
            expires_in = data.get('expires_in', 1200)  # 20 minutes default
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
            
            # Save refresh token
            self._save_refresh_token()
            
            self.logger.info("Successfully obtained refresh token")
            return True
        else:
            self.logger.error(f"Failed to get refresh token: {response.status_code} {response.text}")
            return False
    
    def _get_access_token(self):
        """
        Get access token using refresh token
        """
        # Check if we have a valid access token
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token
        
        # If no refresh token, need to do full auth flow
        if not self.refresh_token:
            self.logger.info("No refresh token available, initiating OAuth flow")
            auth_code = self._get_authorization_code()
            if not self._get_refresh_token(auth_code):
                raise Exception("Failed to obtain refresh token")
        
        # Use refresh token to get access token
        url = f"{self.auth_url}/oauth/token"
        
        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET,
            'refresh_token': self.refresh_token
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(url, data=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data['access_token']
            expires_in = data.get('expires_in', 1200)  # 20 minutes default
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
            
            self.logger.info("Successfully refreshed access token")
            return self.access_token
        else:
            self.logger.error(f"Failed to refresh access token: {response.status_code} {response.text}")
            # If refresh failed, clear tokens and try full auth again
            self.refresh_token = None
            self.access_token = None
            os.remove(self.token_file) if os.path.exists(self.token_file) else None
            raise Exception("Failed to refresh access token. Please re-authenticate.")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make authenticated request to TradeStation API
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Response data or None if error
        """
        # Get access token (will refresh if needed)
        access_token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # Try refreshing token once
                self.logger.warning("Got 401, refreshing token")
                self.access_token = None  # Force refresh
                access_token = self._get_access_token()
                headers["Authorization"] = f"Bearer {access_token}"
                
                # Retry request
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json()
                else:
                    self.logger.error(f"API request failed after retry: {response.status_code} {response.text}")
                    return None
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
            data = self._make_request('/v3/marketdata/barcharts/' + symbol, params)
            
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
            # Try to get access token
            access_token = self._get_access_token()
            
            # Try a simple API call
            response = self._make_request('/v3/marketdata/symbols/SPY')
            
            return response is not None
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
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