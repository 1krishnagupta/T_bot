import requests
import time
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union

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

class TastyTradeAPI:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize the TastyTrade API client.
        
        Args:
            username: TastyTrade username (optional if using credential manager)
            password: TastyTrade password (optional if using credential manager)
        """
        self.base_url = "https://api.cert.tastyworks.com"  # Certification environment
        # self.base_url = "https://api.tastyworks.com"  # Production environment
        
        self.session_token: Optional[str] = None
        self.remember_token: Optional[str] = None
        self.username = username
        self.password = password
        self.last_login_time: Optional[float] = None
        self.session_lifetime_seconds = 60 * 60 * 8  # 8 hours
        self.max_retries = 3
        self.login_attempts = 0
        self.login_success = 0
        self.login_failures = 0
        self.session_refresh_count = 0
        self._last_request_time = 0
        self._min_request_interval = 0.2  # 200ms between requests
        self._streaming_token = None
        self._streaming_url = None

        logger.info(f"TastyTradeAPI initialized for user: {self.username}")

    def login(self) -> bool:
        """
        Authenticate with TastyTrade API.
        
        Returns:
            bool: True if login was successful, False otherwise
        """
        if not self.username or not self.password:
            logger.error("Login failed: Missing username or password")
            return False

        url = f"{self.base_url}/sessions"
        payload = {
            "login": self.username,
            "password": self.password,
            "remember-me": True
        }
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "jigsaw-flow-bot/1.0"
        }

        for attempt in range(1, self.max_retries + 1):
            self.login_attempts += 1
            try:
                # Rate limiting
                elapsed = time.time() - self._last_request_time
                if elapsed < self._min_request_interval:
                    time.sleep(self._min_request_interval - elapsed)

                response = requests.post(url, json=payload, headers=headers)
                self._last_request_time = time.time()

                if response.status_code == 201:
                    data = response.json()["data"]
                    self.session_token = data["session-token"]
                    self.remember_token = data["remember-token"]
                    self.last_login_time = time.time()
                    self.login_success += 1
                    
                    logger.info(f"Login successful on attempt {attempt}")
                    logger.info(f"Session token: {self.session_token[:10]}...")  # Log partial token for security
                    print(f"[✓] Logged in as {data['user']['username']}")
                    return True
                else:
                    self.login_failures += 1
                    error_msg = f"Login failed attempt {attempt}: {response.status_code}"
                    if response.status_code == 401:
                        error_msg += " - Invalid credentials"
                    logger.warning(error_msg)
                    print(f"[!] Login attempt {attempt} failed: {response.status_code}")
                    time.sleep(2 ** attempt)  # Exponential backoff

            except requests.exceptions.RequestException as e:
                self.login_failures += 1
                logger.error(f"Login exception attempt {attempt}: {str(e)}")
                print(f"[!] Network error during login attempt {attempt}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff

        logger.error("[✗] Failed to login after maximum retries")
        print("[✗] Failed to login after maximum retries")
        return False

    def logout(self) -> bool:
        """
        Terminate the current session.
        
        Returns:
            bool: True if logout was successful, False otherwise
        """
        if not self.session_token:
            print("[!] No active session to logout")
            return False

        url = f"{self.base_url}/sessions"
        headers = {
            "Authorization": self.session_token,
            "Content-Type": "application/json"
        }

        try:
            response = requests.delete(url, headers=headers)
            if response.status_code == 204:
                logger.info("Logged out successfully")
                print("[✓] Successfully logged out and session destroyed")
                self.session_token = None
                return True
            else:
                logger.warning(f"Logout failed: {response.status_code} {response.text}")
                print(f"[✗] Logout failed: {response.status_code} - {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Logout exception: {str(e)}")
            print(f"[✗] Logout failed due to error: {str(e)}")
            return False

    def check_and_refresh_session(self) -> bool:
        """
        Check if session is valid and refresh if needed.
        
        Returns:
            bool: True if session is valid, False otherwise
        """
        if not self.session_token or (time.time() - self.last_login_time) > self.session_lifetime_seconds:
            logger.info("Session expired or inactive. Attempting re-login")
            self.session_refresh_count += 1
            print("[!] Session expired or not active. Reconnecting...")
            return self.login()
        return True

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Get authenticated headers with valid session token.
        
        Returns:
            dict: Headers with authorization token
        """
        self.check_and_refresh_session()
        return {
            "Authorization": self.session_token,
            "Content-Type": "application/json",
            "User-Agent": "jigsaw-bot/1.0"
        }

    def safe_request(self, method: str, endpoint: str, retries: int = 3, backoff_factor: float = 2.0, **kwargs) -> requests.Response:
        """
        Make an authenticated API request with enhanced error handling and retries.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint URL
            retries: Number of retry attempts
            backoff_factor: Backoff factor for retries (exponential backoff)
            **kwargs: Additional request parameters
            
        Returns:
            requests.Response: API response
        """
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

                # Handle different error codes
                if response.status_code == 401:
                    # Authentication error
                    logger.warning("401 Unauthorized. Re-logging in...")
                    self.login()
                    kwargs["headers"] = self.get_auth_headers()
                    attempt += 1
                    if attempt < retries:
                        # Wait with exponential backoff
                        wait_time = backoff_factor ** attempt
                        time.sleep(wait_time)
                        continue
                elif response.status_code == 429:
                    # Rate limit exceeded
                    logger.warning("429 Rate limit exceeded")
                    # Get retry-after header if available
                    retry_after = int(response.headers.get('Retry-After', backoff_factor ** attempt))
                    time.sleep(retry_after)
                    attempt += 1
                    continue
                elif response.status_code >= 500:
                    # Server error, retry
                    logger.warning(f"Server error: {response.status_code}")
                    attempt += 1
                    if attempt < retries:
                        wait_time = backoff_factor ** attempt
                        time.sleep(wait_time)
                        continue
                
                # Success or non-retryable error
                return response

            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout, 
                    requests.exceptions.RequestException) as e:
                last_error = e
                logger.error(f"Request failed (attempt {attempt+1}/{retries}): {str(e)}")
                attempt += 1
                if attempt < retries:
                    wait_time = backoff_factor ** attempt
                    time.sleep(wait_time)
                
        # If we got here, all retries failed
        logger.error(f"All {retries} request attempts failed. Last error: {last_error}")
        raise last_error

    def fetch_account_balance(self, account_number: str) -> Dict[str, float]:
        """
        Fetch account balance information.
        
        Args:
            account_number: TastyTrade account number
            
        Returns:
            dict: Dictionary with balance information
        """
        endpoint = f"/accounts/{account_number}/balances"
        response = self.safe_request("GET", endpoint)

        if response.status_code == 200:
            balance = response.json()["data"]
            cash_balance = float(balance.get("cash-balance", 0.0))
            available_funds = float(balance.get("available-trading-funds", 0.0))

            logger.info(f"Balance fetched for account {account_number}")
            print(f"[✓] Balance fetched for account {account_number}")
            print(f"Cash Balance: {cash_balance}")
            print(f"Available Trading Funds: {available_funds}")

            return {
                "cash_balance": cash_balance,
                "available_trading_funds": available_funds,
                "net_liquidating_value": float(balance.get("net-liquidating-value", 0.0)),
                "updated_at": balance.get("updated-at", "")
            }
        else:
            logger.error(f"Failed to fetch account balance: {response.status_code}")
            print(f"[✗] Failed to fetch account balance: {response.status_code}")
            return {
                "cash_balance": 0.0,
                "available_trading_funds": 0.0,
                "net_liquidating_value": 0.0,
                "updated_at": ""
            }
    
    def get_quote_token(self) -> Dict[str, str]:
        """
        Get a token for streaming market data.
        
        Returns:
            dict: Dictionary with streaming token info
        """
        endpoint = "/api-quote-tokens"
        response = self.safe_request("GET", endpoint)
        
        if response.status_code == 200:
            data = response.json().get("data", {})
            self._streaming_token = data.get("token")
            self._streaming_url = data.get("dxlink-url")
            
            logger.info(f"Successfully fetched streaming token: {self._streaming_token[:10]}...")
            return data
        else:
            logger.error(f"Failed to fetch streaming token: {response.status_code}")
            print(f"[✗] Failed to fetch streaming token: {response.status_code}")
            return {}
    
    def get_market_quotes(self, symbols: List[str], instrument_type: str = "equity") -> List[Dict]:
        """
        Fetch current market quotes for multiple instruments.
        
        Args:
            symbols: List of symbols to fetch quotes for
            instrument_type: Type of instrument (equity, equity-option, cryptocurrency, index, future, future-option)
            
        Returns:
            list: List of quote data for each symbol
        """
        if not symbols:
            logger.error("No symbols provided for market quotes")
            return []
        
        # Convert symbols list to comma-separated string
        symbols_str = ",".join(symbols)
        
        # Build endpoint with query parameters
        endpoint = f"/market-data/by-type?{instrument_type}={symbols_str}"
        
        response = self.safe_request("GET", endpoint)
        
        if response.status_code == 200:
            quotes = response.json().get("data", {}).get("items", [])
            logger.info(f"Fetched {len(quotes)} {instrument_type} quotes")
            return quotes
        else:
            logger.error(f"Failed to fetch market quotes: {response.status_code}")
            print(f"[✗] Failed to fetch market quotes: {response.status_code}")
            return []
    
    def get_equity_details(self, symbol: str) -> Dict:
        """
        Get detailed information about an equity.
        
        Args:
            symbol: Equity symbol
            
        Returns:
            dict: Equity details
        """
        endpoint = f"/instruments/equities/{symbol}"
        response = self.safe_request("GET", endpoint)
        
        if response.status_code == 200:
            data = response.json().get("data", {})
            logger.info(f"Fetched equity details for {symbol}")
            return data
        else:
            logger.error(f"Failed to fetch equity details for {symbol}: {response.status_code}")
            print(f"[✗] Failed to fetch equity details for {symbol}: {response.status_code}")
            return {}
    
    def get_option_chain(self, symbol: str) -> Dict:
        """
        Get option chain for an underlying symbol.
        
        Args:
            symbol: Underlying symbol
            
        Returns:
            dict: Option chain data
        """
        endpoint = f"/option-chains/{symbol}/nested"
        response = self.safe_request("GET", endpoint)
        
        if response.status_code == 200:
            data = response.json().get("data", {})
            logger.info(f"Fetched option chain for {symbol}")
            return data
        else:
            logger.error(f"Failed to fetch option chain for {symbol}: {response.status_code}")
            print(f"[✗] Failed to fetch option chain for {symbol}: {response.status_code}")
            return {}