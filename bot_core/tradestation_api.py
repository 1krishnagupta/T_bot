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
        """
        # API endpoints
        self.base_url = "https://api.tradestation.com"
        self.auth_url = "https://signin.tradestation.com"  # Correct auth URL
        
        # Initialize credentials
        self.username = username
        self.password = password
        self.CLIENT_ID = None
        self.CLIENT_SECRET = None
        
        # Load credentials from file
        self._load_credentials()
        
        # Authentication tokens
        self.refresh_token = None
        self.access_token = None
        self.session_token = None
        self.remember_token = None
        self.token_expiry = None
        self.last_login_time = None
        self.session_lifetime_seconds = 60 * 60 * 8
        
        # For OAuth callback
        self.auth_code = None
        self.callback_received = threading.Event()
        
        # File to store tokens
        self.token_file = os.path.join(os.path.dirname(__file__), '.tradestation_tokens.json')
        
        # Statistics
        self.login_attempts = 0
        self.login_success = 0
        self.login_failures = 0
        self.session_refresh_count = 0
        self._last_request_time = 0
        self._min_request_interval = 0.2
        
        # Load saved tokens
        self._load_tokens()
        
        logger.info(f"TradeStationAPI initialized with client_id: {self.CLIENT_ID[:10]}...")


    def _load_credentials(self):
        """Load credentials from credentials.txt file"""
        try:
            # Try multiple paths to find credentials.txt
            possible_paths = [
                os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'credentials.txt'),
                os.path.join(os.path.dirname(__file__), '..', 'config', 'credentials.txt'),
                os.path.join(os.path.dirname(__file__), 'credentials.txt'),
                'credentials.txt'
            ]
            
            cred_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    cred_path = path
                    break
            
            if not cred_path:
                logger.error("credentials.txt not found in any expected location")
                print("[!] ERROR: credentials.txt not found!")
                print("[!] Please create credentials.txt with your TradeStation API details")
                raise FileNotFoundError("credentials.txt not found")
            
            with open(cred_path, 'r') as f:
                import yaml
                creds = yaml.safe_load(f)
                
            broker = creds.get('broker', {})
            
            # Load username/password if not provided
            if not self.username:
                self.username = broker.get('username', '')
            if not self.password:
                self.password = broker.get('password', '')
            
            # Load API credentials (REQUIRED)
            self.CLIENT_ID = broker.get('api_key', '')
            self.CLIENT_SECRET = broker.get('api_secret', '')
            
            if not self.CLIENT_ID or not self.CLIENT_SECRET:
                logger.error("API credentials missing in credentials.txt")
                print("[!] ERROR: API key/secret missing in credentials.txt!")
                print("[!] You need to add:")
                print("    api_key: 'your_tradestation_api_key'")
                print("    api_secret: 'your_tradestation_api_secret'")
                raise ValueError("API credentials missing")
            
            logger.info(f"Loaded credentials from {cred_path}")
            print(f"[✓] Loaded credentials from {cred_path}")
            
        except Exception as e:
            logger.error(f"Error loading credentials: {e}")
            raise


    
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

    def get_account_ids(self):
        """Get all account IDs for the user"""
        if not self.userid:
            return []
        
        endpoint = f"/v2/users/{self.userid}/accounts"
        response = self.safe_request("GET", endpoint)
        
        if response.status_code == 200:
            accounts = response.json()
            account_ids = [acc.get("Key") for acc in accounts]
            print(f"Available accounts: {account_ids}")
            return account_ids
        return []
    
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
        # Build authorization URL with actual client ID
        auth_params = {
            'response_type': 'code',
            'client_id': self.CLIENT_ID,  # Use actual client ID
            'audience': 'https://api.tradestation.com',
            'redirect_uri': 'http://localhost:3000',
            'scope': 'openid profile MarketData ReadAccount Trade offline_access Matrix OptionSpreads'
        }
        
        # Correct URL format - only one /authorize
        auth_url = f"{self.auth_url}/authorize?{urllib.parse.urlencode(auth_params)}"
        
        print("\n" + "="*80)
        print("TRADESTATION AUTHENTICATION REQUIRED")
        print("="*80)
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
        url = f"{self.auth_url}/oauth/token"  # Correct token endpoint
        
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
            self.refresh_token = data.get('refresh_token')
            self.access_token = data.get('access_token')
            expires_in = data.get('expires_in', 1200)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
            
            # Save tokens
            self._save_tokens()
            
            logger.info("Successfully obtained tokens")
            print("[✓] Successfully authenticated with TradeStation")
            return True
        else:
            logger.error(f"Failed to exchange code for tokens: {response.status_code} {response.text}")
            print(f"[✗] Authentication failed: {response.status_code}")
            try:
                error_data = response.json()
                print(f"[✗] Error: {error_data}")
            except:
                print(f"[✗] Response: {response.text}")
            return False
        
    
    def _refresh_access_token(self):
        """Refresh access token - FIXED VERSION"""
        if not self.refresh_token:
            return False
        
        url = f"{self.base_url}/v2/security/authorize"
        
        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET,
            'refresh_token': self.refresh_token,
            'response_type': 'token'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(url, data=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get('access_token')
            expires_in = data.get('expires_in', 1200)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
            
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
        """Fixed version using correct endpoint"""
        try:
            # Get user ID first if not available
            if not hasattr(self, 'userid') or not self.userid:
                # Need to get user info
                return self._empty_balance()
            
            # Get accounts for user
            endpoint = f"/v2/users/{self.userid}/accounts"
            response = self.safe_request("GET", endpoint)
            
            if response.status_code != 200:
                return self._empty_balance()
            
            accounts = response.json()
            if not accounts:
                return self._empty_balance()
            
            # Use first account or specified one
            if not account_number:
                account_number = accounts[0].get("Key")
            
            # Get balance for account
            balance_endpoint = f"/v2/accounts/{account_number}/balances"
            balance_response = self.safe_request("GET", balance_endpoint)
            
            if balance_response.status_code == 200:
                balances = balance_response.json()
                if balances:
                    balance = balances[0] if isinstance(balances, list) else balances
                    
                    return {
                        "cash_balance": float(balance.get("RealTimeAccountBalance", 0)),
                        "available_trading_funds": float(balance.get("RealTimeBuyingPower", 0)),
                        "net_liquidating_value": float(balance.get("RealTimeEquity", 0)),
                        "updated_at": datetime.now().isoformat()
                    }
            
            return self._empty_balance()
            
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
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
        """Fixed version using correct endpoint"""
        if not symbols:
            return []
        
        try:
            # TradeStation quote endpoint from swagger
            endpoint = f"/v2/data/quote/{','.join(symbols)}"
            
            # Add APIVersion parameter as required
            params = {'APIVersion': '20160101'}
            
            response = self.safe_request("GET", endpoint, params=params)
            
            if response.status_code == 200:
                quotes = response.json()
                
                # Convert to standardized format
                formatted_quotes = []
                for quote in quotes:
                    formatted_quote = {
                        "symbol": quote.get("Symbol"),
                        "bid": float(quote.get("Bid", 0)),
                        "ask": float(quote.get("Ask", 0)),
                        "last": float(quote.get("Last", 0)),
                        "bidSize": float(quote.get("BidSize", 0)),
                        "askSize": float(quote.get("AskSize", 0)),
                        "volume": float(quote.get("Volume", 0)),
                        "high": float(quote.get("High", 0)),
                        "low": float(quote.get("Low", 0)),
                        "open": float(quote.get("Open", 0)),
                        "close": float(quote.get("Close", 0))
                    }
                    formatted_quotes.append(formatted_quote)
                
                return formatted_quotes
            else:
                self.logger.error(f"Failed to fetch quotes: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching quotes: {e}")
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