# Code/bot_core/tradestation_api.py

import requests
import time
import logging
import os
import json
import webbrowser
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Setup logging
today = datetime.now().strftime("%Y-%m-%d")
log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"broker_api_{today}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class TradeStationAPI:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize the TradeStation API client.
        
        Args:
            username: Not used for TradeStation (kept for compatibility)
            password: Not used for TradeStation (kept for compatibility)
        """
        # TradeStation OAuth credentials
        self.CLIENT_ID = "6ZhZile2KIwtU2xwdNGBYdNpPmRynB5J"
        self.CLIENT_SECRET = "tY4dNJuhFst_XeqMmB95pF2_EriSqxc-ruQdnNILc4L5_vm9M0Iixwf9FUGw-WbQ"
        
        # API endpoints
        self.base_url = "https://api.tradestation.com"
        self.auth_url = "https://signin.tradestation.com"
        
        # Authentication tokens
        self.refresh_token = None
        self.access_token = None
        self.session_token = None  # For compatibility
        self.remember_token = None  # For compatibility
        self.token_expiry = None
        self.last_login_time = None
        self.session_lifetime_seconds = 60 * 60 * 8  # 8 hours
        
        # For OAuth callback
        self.auth_code = None
        self.callback_received = threading.Event()
        
        # File to store tokens
        self.token_file = os.path.join(os.path.dirname(__file__), '.tradestation_tokens.json')
        
        # Statistics for compatibility
        self.login_attempts = 0
        self.login_success = 0
        self.login_failures = 0
        self.session_refresh_count = 0
        self._last_request_time = 0
        self._min_request_interval = 0.2
        
        # Load saved tokens
        self._load_tokens()
        
        logger.info(f"TradeStationAPI initialized")
    
    def _load_tokens(self):
        """Load tokens from file if they exist"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    self.refresh_token = data.get('refresh_token')
                    self.access_token = data.get('access_token')
                    token_expiry_str = data.get('token_expiry')
                    if token_expiry_str:
                        self.token_expiry = datetime.fromisoformat(token_expiry_str)
                    logger.info("Loaded tokens from file")
        except Exception as e:
            logger.error(f"Error loading tokens: {e}")
    
    def _save_tokens(self):
        """Save tokens to file"""
        try:
            data = {
                'refresh_token': self.refresh_token,
                'access_token': self.access_token,
                'token_expiry': self.token_expiry.isoformat() if self.token_expiry else None
            }
            with open(self.token_file, 'w') as f:
                json.dump(data, f)
            logger.info("Saved tokens to file")
        except Exception as e:
            logger.error(f"Error saving tokens: {e}")
    
    def login(self) -> bool:
        """
        Authenticate with TradeStation API using OAuth flow.
        
        Returns:
            bool: True if login was successful, False otherwise
        """
        self.login_attempts += 1
        
        try:
            # Check if we have a valid access token
            if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
                # Test if token is still valid
                if self._test_token():
                    self.session_token = self.access_token  # For compatibility
                    self.last_login_time = time.time()
                    self.login_success += 1
                    print("[✓] Already logged in with valid token")
                    return True
            
            # If we have refresh token, try to refresh
            if self.refresh_token:
                if self._refresh_access_token():
                    self.session_token = self.access_token  # For compatibility
                    self.last_login_time = time.time()
                    self.login_success += 1
                    print("[✓] Logged in using refresh token")
                    return True
            
            # Need to do full OAuth flow
            print("\n" + "="*80)
            print("TRADESTATION AUTHENTICATION REQUIRED")
            print("="*80)
            
            # Get authorization code
            auth_code = self._get_authorization_code()
            if not auth_code:
                self.login_failures += 1
                return False
            
            # Exchange for tokens
            if self._exchange_code_for_tokens(auth_code):
                self.session_token = self.access_token  # For compatibility
                self.last_login_time = time.time()
                self.login_success += 1
                print("[✓] Successfully logged in to TradeStation")
                return True
            else:
                self.login_failures += 1
                return False
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            self.login_failures += 1
            return False
    
    def _test_token(self) -> bool:
        """Test if current access token is valid"""
        try:
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(f"{self.base_url}/v3/marketdata/symbols/SPY", headers=headers)
            return response.status_code == 200
        except:
            return False
    
    def _get_authorization_code(self):
        """Get authorization code through OAuth flow"""
        # Build authorization URL
        auth_params = {
            'response_type': 'code',
            'client_id': self.CLIENT_ID,
            'audience': 'https://api.tradestation.com',
            'redirect_uri': 'http://localhost:3000',
            'scope': 'openid MarketData profile ReadAccount Trade offline_access Matrix OptionSpreads'
        }
        
        auth_url = f"{self.auth_url}/authorize?{urllib.parse.urlencode(auth_params)}"
        
        print("\nPlease login to TradeStation to authorize this application.")
        print("\nOpening browser to:")
        print(auth_url)
        print("\nIf browser doesn't open automatically, please copy and paste the URL above.")
        print("="*80 + "\n")
        
        # Try to open browser
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
        parent = self
        
        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                # Extract code from query string
                if 'code=' in self.path:
                    code = self.path.split('code=')[1].split('&')[0]
                    parent.auth_code = code
                    parent.callback_received.set()
                    
                    # Send response
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(b'<html><body><h1>Authorization successful!</h1><p>You can close this window.</p></body></html>')
                else:
                    self.send_response(400)
                    self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress log messages
        
        try:
            server = HTTPServer(('localhost', 3000), CallbackHandler)
            server.timeout = 300  # 5 minute timeout
            server.handle_request()  # Handle one request then stop
        except:
            pass
    
    def _exchange_code_for_tokens(self, auth_code):
        """Exchange authorization code for access and refresh tokens"""
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
            
            # Save tokens
            self._save_tokens()
            
            logger.info("Successfully obtained tokens")
            return True
        else:
            logger.error(f"Failed to exchange code for tokens: {response.status_code} {response.text}")
            return False
    
    def _refresh_access_token(self):
        """Refresh access token using refresh token"""
        if not self.refresh_token:
            return False
        
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
            expires_in = data.get('expires_in', 1200)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
            
            # Update refresh token if provided
            if 'refresh_token' in data:
                self.refresh_token = data['refresh_token']
            
            # Save tokens
            self._save_tokens()
            
            logger.info("Successfully refreshed access token")
            return True
        else:
            logger.error(f"Failed to refresh access token: {response.status_code}")
            # Clear tokens if refresh failed
            self.refresh_token = None
            self.access_token = None
            if os.path.exists(self.token_file):
                os.remove(self.token_file)
            return False
    
    def logout(self) -> bool:
        """
        Logout from TradeStation (clear tokens).
        
        Returns:
            bool: True if logout was successful
        """
        try:
            # Clear tokens
            self.access_token = None
            self.refresh_token = None
            self.session_token = None
            self.token_expiry = None
            
            # Remove token file
            if os.path.exists(self.token_file):
                os.remove(self.token_file)
            
            logger.info("Logged out successfully")
            print("[✓] Successfully logged out")
            return True
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False
    
    def check_and_refresh_session(self) -> bool:
        """
        Check if session is valid and refresh if needed.
        
        Returns:
            bool: True if session is valid
        """
        if not self.access_token:
            return self.login()
        
        if self.token_expiry and datetime.now() >= self.token_expiry:
            logger.info("Access token expired, refreshing...")
            self.session_refresh_count += 1
            return self._refresh_access_token()
        
        return True
    
    def get_auth_headers(self) -> Dict[str, str]:
        """
        Get authenticated headers with valid access token.
        
        Returns:
            dict: Headers with authorization token
        """
        self.check_and_refresh_session()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def safe_request(self, method: str, endpoint: str, retries: int = 3, backoff_factor: float = 2.0, **kwargs) -> requests.Response:
        """
        Make an authenticated API request with error handling and retries.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            retries: Number of retry attempts
            backoff_factor: Backoff factor for retries
            **kwargs: Additional request parameters
            
        Returns:
            requests.Response: API response
        """
        # Ensure endpoint starts with /
        if not endpoint.startswith('/'):
            endpoint = f"/{endpoint}"
        
        url = f"{self.base_url}{endpoint}"
        
        # Rate limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        
        if "headers" not in kwargs:
            kwargs["headers"] = self.get_auth_headers()
        
        attempt = 0
        last_error = None
        
        while attempt < retries:
            try:
                response = requests.request(method, url, **kwargs)
                self._last_request_time = time.time()
                
                if response.status_code == 401:
                    # Try refreshing token
                    logger.warning("Got 401, refreshing token")
                    if self._refresh_access_token():
                        kwargs["headers"] = self.get_auth_headers()
                        attempt += 1
                        continue
                    else:
                        # Full re-auth needed
                        if self.login():
                            kwargs["headers"] = self.get_auth_headers()
                            attempt += 1
                            continue
                elif response.status_code == 429:
                    # Rate limit
                    retry_after = int(response.headers.get('Retry-After', backoff_factor ** attempt))
                    time.sleep(retry_after)
                    attempt += 1
                    continue
                elif response.status_code >= 500:
                    # Server error
                    attempt += 1
                    if attempt < retries:
                        time.sleep(backoff_factor ** attempt)
                        continue
                
                return response
                
            except Exception as e:
                last_error = e
                logger.error(f"Request failed (attempt {attempt+1}/{retries}): {str(e)}")
                attempt += 1
                if attempt < retries:
                    time.sleep(backoff_factor ** attempt)
        
        # All retries failed
        raise last_error if last_error else Exception("All request attempts failed")
    
    def fetch_account_balance(self, account_number: str = None) -> Dict[str, float]:
        """
        Fetch account balance information.
        
        Args:
            account_number: TradeStation account number (will auto-detect if not provided)
            
        Returns:
            dict: Dictionary with balance information
        """
        try:
            # If no account number provided, get accounts list first
            if not account_number:
                accounts_response = self.safe_request("GET", "/v3/brokerage/accounts")
                if accounts_response.status_code == 200:
                    accounts_data = accounts_response.json()
                    accounts = accounts_data.get("Accounts", [])
                    if accounts:
                        # Find the first active account
                        for account in accounts:
                            if account.get("Status") == "Active":
                                account_number = account["AccountID"]
                                break
                        if not account_number and accounts:
                            account_number = accounts[0]["AccountID"]
                    else:
                        logger.error("No accounts found")
                        return self._empty_balance()
                elif accounts_response.status_code == 403:
                    logger.error("Access forbidden - check if account has proper permissions")
                    print("[!] Access forbidden - your account may not have trading permissions enabled")
                    return self._empty_balance()
                else:
                    logger.error(f"Failed to fetch accounts: {accounts_response.status_code}")
                    return self._empty_balance()
            
            # Fetch account balances
            endpoint = f"/v3/brokerage/accounts/{account_number}/balances"
            response = self.safe_request("GET", endpoint)
            
            if response.status_code == 200:
                balances_data = response.json()
                
                # Handle both single balance and array of balances
                if isinstance(balances_data, dict):
                    balance = balances_data
                elif isinstance(balances_data, list) and len(balances_data) > 0:
                    balance = balances_data[0]
                else:
                    balance = {}
                
                # TradeStation uses different field names
                cash_balance = float(balance.get("CashBalance", balance.get("Cash", 0.0)))
                buying_power = float(balance.get("BuyingPower", balance.get("DayTradingBuyingPower", 0.0)))
                equity = float(balance.get("Equity", balance.get("MarketValue", 0.0)))
                
                logger.info(f"Balance fetched for account {account_number}")
                print(f"[✓] Balance fetched for account {account_number}")
                print(f"Cash Balance: ${cash_balance:,.2f}")
                print(f"Buying Power: ${buying_power:,.2f}")
                
                return {
                    "cash_balance": cash_balance,
                    "available_trading_funds": buying_power,
                    "net_liquidating_value": equity,
                    "updated_at": datetime.now().isoformat()
                }
            elif response.status_code == 403:
                logger.error(f"Access forbidden for account {account_number} - check account permissions")
                print(f"[!] Cannot access balance for account {account_number}")
                print("[!] This may be due to:")
                print("    - Account not having trading permissions")
                print("    - Account being a paper/demo account without balance access")
                print("    - Incorrect account type for API access")
                return self._empty_balance()
            else:
                logger.error(f"Failed to fetch account balance: {response.status_code}")
                if response.text:
                    logger.error(f"Response: {response.text}")
                return self._empty_balance()
                
        except Exception as e:
            logger.error(f"Error fetching account balance: {e}")
            return self._empty_balance()
    
    def _empty_balance(self):
        """Return empty balance structure"""
        return {
            "cash_balance": 0.0,
            "available_trading_funds": 0.0,
            "net_liquidating_value": 0.0,
            "updated_at": ""
        }
    
    def get_quote_token(self) -> Dict[str, str]:
        """
        Get a token for streaming market data.
        TradeStation uses the same access token for streaming.
        
        Returns:
            dict: Dictionary with streaming token info
        """
        self.check_and_refresh_session()
        
        # TradeStation uses the access token for streaming
        return {
            "token": self.access_token,
            "dxlink-url": "wss://stream.tradestation.com/v3/marketdata/stream",
            "level": "live"  # or "delayed" based on account
        }
    
    def get_market_quotes(self, symbols: List[str], instrument_type: str = "equity") -> List[Dict]:
        """
        Fetch current market quotes for multiple instruments.
        
        Args:
            symbols: List of symbols to fetch quotes for
            instrument_type: Type of instrument
            
        Returns:
            list: List of quote data for each symbol
        """
        if not symbols:
            logger.error("No symbols provided for market quotes")
            return []
        
        try:
            # TradeStation API expects comma-separated symbols
            symbols_str = ",".join(symbols)
            
            endpoint = f"/v3/marketdata/quotes/{symbols_str}"
            response = self.safe_request("GET", endpoint)
            
            if response.status_code == 200:
                quotes_data = response.json()
                quotes = quotes_data.get("Quotes", [])
                
                # Convert to TastyTrade-like format for compatibility
                formatted_quotes = []
                for quote in quotes:
                    formatted_quote = {
                        "symbol": quote.get("Symbol"),
                        "bid": quote.get("Bid", 0),
                        "ask": quote.get("Ask", 0),
                        "last": quote.get("Last", 0),
                        "bidSize": quote.get("BidSize", 0),
                        "askSize": quote.get("AskSize", 0),
                        "volume": quote.get("Volume", 0),
                        "high": quote.get("High", 0),
                        "low": quote.get("Low", 0),
                        "open": quote.get("Open", 0),
                        "close": quote.get("PreviousClose", 0)
                    }
                    formatted_quotes.append(formatted_quote)
                
                logger.info(f"Fetched {len(formatted_quotes)} quotes")
                return formatted_quotes
            else:
                logger.error(f"Failed to fetch market quotes: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching market quotes: {e}")
            return []
    
    def get_equity_details(self, symbol: str) -> Dict:
        """
        Get detailed information about an equity.
        
        Args:
            symbol: Equity symbol
            
        Returns:
            dict: Equity details
        """
        try:
            endpoint = f"/v3/marketdata/symbols/{symbol}"
            response = self.safe_request("GET", endpoint)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Fetched equity details for {symbol}")
                return data
            else:
                logger.error(f"Failed to fetch equity details for {symbol}: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching equity details: {e}")
            return {}
    
    def get_option_chain(self, symbol: str) -> Dict:
        """
        Get option chain for an underlying symbol.
        
        Args:
            symbol: Underlying symbol
            
        Returns:
            dict: Option chain data
        """
        try:
            endpoint = f"/v3/marketdata/options/chains/{symbol}"
            response = self.safe_request("GET", endpoint)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Fetched option chain for {symbol}")
                return data
            else:
                logger.error(f"Failed to fetch option chain for {symbol}: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching option chain: {e}")
            return {}