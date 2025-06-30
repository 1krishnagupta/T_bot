# Code/bot_core/instrument_fetcher.py

import requests
import logging
import os
import json
import random
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

class InstrumentFetcher:
    """
    Class for fetching instruments and market data from TastyTrade API.
    """
    
    def __init__(self, api, test_mode=False):
        """
        Initialize the instrument fetcher
        
        Args:
            api: TastyTrade API client
            test_mode (bool): Whether to use test data (deprecated, always False now)
        """
        self.api = api
        self.test_mode = False  # Force to false, no more test mode
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"instrument_fetcher_{today}.log")
        
        self.logger = logging.getLogger("InstrumentFetcher")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
    def fetch_equities(self, is_etf=False, is_index=False):
        """
        Fetch list of equities with optional filters
        
        Args:
            is_etf (bool): Filter for ETFs
            is_index (bool): Filter for indices
            
        Returns:
            list: List of equity objects
        """
        params = {
            "is-etf": str(is_etf).lower(),
            "is-index": str(is_index).lower()
        }
        response = self.api.safe_request("GET", "/instruments/equities", params=params)
        if response.status_code == 200:
            items = response.json()["data"]["items"]
            print(f"[✓] Fetched {len(items)} equities (ETF={is_etf}, Index={is_index})")
            return items
        else:
            print(f"[✗] Failed to fetch equities: {response.status_code}")
            return []

    def fetch_active_equities(self):
        """
        Fetch list of active equities
        
        Returns:
            list: List of active equity objects
        """
        response = self.api.safe_request("GET", "/instruments/equities/active")
        if response.status_code == 200:
            items = response.json()["data"]["items"]
            print(f"[✓] Fetched {len(items)} active equities")
            return items
        else:
            print(f"[✗] Failed to fetch active equities: {response.status_code}")
            return []

    def fetch_equity(self, symbol):
        """
        Fetch a single equity object for a given symbol
        
        Args:
            symbol (str): Equity symbol (e.g., "SPY", "NIFTY")
            
        Returns:
            dict: Equity data or None if not found
        """
        response = self.api.safe_request("GET", f"/instruments/equities/{symbol}")
        if response.status_code == 200:
            data = response.json()["data"]
            print(f"[✓] Fetched equity data for {symbol}")
            return data
        else:
            print(f"[✗] Failed to fetch equity {symbol}: {response.status_code}")
            return None

    def fetch_nested_option_chains(self, underlying_symbol):
        """
        Fetch nested option chains (expirations and strikes) for a symbol
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            dict: Nested option chain data with expirations and strikes
        """
        response = self.api.safe_request("GET", f"/option-chains/{underlying_symbol}/nested")
        if response.status_code == 200:
            data = response.json()["data"]
            expirations_count = len(data.get("expirations", []))
            print(f"[✓] Fetched nested option chain for {underlying_symbol} with {expirations_count} expirations")
            return data
        else:
            print(f"[✗] Failed to fetch nested option chain for {underlying_symbol}: {response.status_code}")
            return None

    def fetch_detailed_option_chains(self, underlying_symbol):
        """
        Fetch detailed option chains for a symbol (full option data)
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            list: List of detailed option objects
        """
        response = self.api.safe_request("GET", f"/option-chains/{underlying_symbol}")
        if response.status_code == 200:
            data = response.json()["data"]["items"]
            print(f"[✓] Fetched {len(data)} detailed options for {underlying_symbol}")
            return data
        else:
            print(f"[✗] Failed to fetch detailed options for {underlying_symbol}: {response.status_code}")
            return []

    def fetch_compact_option_chains(self, underlying_symbol):
        """
        Fetch compact option chains for a symbol
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            list: List of compact option symbols
        """
        response = self.api.safe_request("GET", f"/option-chains/{underlying_symbol}/compact")
        if response.status_code == 200:
            data = response.json()["data"]["items"]
            print(f"[✓] Fetched {len(data)} compact option symbols for {underlying_symbol}")
            return data
        else:
            print(f"[✗] Failed to fetch compact options for {underlying_symbol}: {response.status_code}")
            return []

    def fetch_equity_options(self, symbols=None, active=True, with_expired=False):
        """
        Fetch equity options by symbol(s)
        
        Args:
            symbols (list): List of option symbols in OCC format
            active (bool): Whether to include only active options
            with_expired (bool): Whether to include expired options
            
        Returns:
            list: List of option data
        """
        params = {}
        
        if symbols:
            # Convert symbols list to the format expected by the API
            for i, symbol in enumerate(symbols):
                params[f"symbol[{i}]"] = symbol
        
        if active is not None:
            params["active"] = str(active).lower()
            
        if with_expired is not None:
            params["with-expired"] = str(with_expired).lower()
            
        response = self.api.safe_request("GET", "/instruments/equity-options", params=params)
        if response.status_code == 200:
            data = response.json()["data"]["items"]
            print(f"[✓] Fetched {len(data)} equity options")
            return data
        else:
            print(f"[✗] Failed to fetch equity options: {response.status_code}")
            return []

    def fetch_equity_option(self, symbol):
        """
        Fetch a single equity option by symbol
        
        Args:
            symbol (str): Option symbol in OCC format (e.g., "SPY 230731C00393000")
            
        Returns:
            dict: Option data or None if not found
        """
        response = self.api.safe_request("GET", f"/instruments/equity-options/{symbol}")
        if response.status_code == 200:
            data = response.json()["data"]
            print(f"[✓] Fetched option data for {symbol}")
            return data
        else:
            print(f"[✗] Failed to fetch option {symbol}: {response.status_code}")
            return None

    def fetch_market_quote(self, symbols, instrument_type="equity"):
        """
        Fetch current market quotes for multiple instruments
        
        Args:
            symbols (list): List of symbols to fetch quotes for
            instrument_type (str): Type of instrument, options:
                "equity", "equity-option", "cryptocurrency", "index", "future", "future-option"
                
        Returns:
            list: List of quote data for each symbol
        """
        if not symbols:
            print("[✗] No symbols provided")
            return []
            
        # Use the API's market quote functionality
        return self.api.get_market_quotes(symbols, instrument_type)
            
    def get_api_quote_token(self):
        """
        Get an API quote token for streaming market data
        
        Returns:
            dict: Dictionary with token, dxlink-url, and level
        """
        return self.api.get_quote_token()
            
    def get_streamer_symbol(self, symbol, instrument_type="equity"):
        """
        Get the streamer symbol for a given instrument
        
        Args:
            symbol (str): Regular symbol (e.g. "SPY")
            instrument_type (str): Type of instrument
                
        Returns:
            str: Streamer symbol for use with DXLink
        """
        # For equity options, we need to fetch the instrument to get the streamer symbol
        if instrument_type == "equity-option":
            option = self.fetch_equity_option(symbol)
            if option and "streamer-symbol" in option:
                return option["streamer-symbol"]
            return f".{symbol}"  # Default format for options
            
        # For equities, we can fetch the equity details
        if instrument_type == "equity":
            equity = self.fetch_equity(symbol)
            if equity and "streamer-symbol" in equity:
                return equity["streamer-symbol"]
            return symbol
            
        # Default to original symbol if we can't determine the streamer symbol
        return symbol
            
    def get_current_price(self, symbol, instrument_type="equity"):
        """
        Get current price for a symbol
        
        Args:
            symbol (str): Symbol to get price for
            instrument_type (str): Type of instrument
            
        Returns:
            float: Current price or None if not available
        """
        # Fetch market quote
        quotes = self.fetch_market_quote([symbol], instrument_type)
        
        if quotes and len(quotes) > 0:
            quote = quotes[0]
            
            # Use last price if available, otherwise use mid price
            if "last" in quote and quote["last"]:
                return float(quote["last"])
            elif "bid" in quote and "ask" in quote:
                bid = float(quote.get("bid", 0))
                ask = float(quote.get("ask", 0))
                
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
                elif bid > 0:
                    return bid
                elif ask > 0:
                    return ask
                
        return None
        
    def get_option_chain(self, symbol, expiry_date=None, strike_price=None, option_type=None):
        """
        Get filtered option chain for a symbol
        
        Args:
            symbol (str): Underlying symbol
            expiry_date (str): Filter by expiration date (ISO format)
            strike_price (float): Filter by strike price
            option_type (str): Filter by option type ("Call" or "Put")
            
        Returns:
            list: Filtered option chain
        """
        # Fetch detailed option chain
        detailed_options = self.fetch_detailed_option_chains(symbol)
        
        if not detailed_options:
            return []
            
        # Apply filters
        filtered_options = detailed_options
        
        if expiry_date:
            filtered_options = [opt for opt in filtered_options if opt.get("expiration-date") == expiry_date]
            
        if strike_price:
            filtered_options = [opt for opt in filtered_options if abs(float(opt.get("strike-price", 0)) - strike_price) < 0.01]
            
        if option_type:
            filtered_options = [opt for opt in filtered_options if opt.get("option-type") == option_type]
            
        return filtered_options
        
    def get_option_expirations(self, symbol):
        """
        Get available expiration dates for a symbol
        
        Args:
            symbol (str): Underlying symbol
            
        Returns:
            list: List of expiration dates
        """
        # Fetch nested option chain
        nested_chain = self.fetch_nested_option_chains(symbol)
        
        if not nested_chain or "expirations" not in nested_chain:
            return []
            
        # Extract expiration dates
        expirations = []
        for exp in nested_chain["expirations"]:
            if "expiration-date" in exp:
                expirations.append(exp["expiration-date"])
                
        return sorted(expirations)


    # Updates to instrument_fetcher.py - add this method

    def fetch_multiple_equities(self, symbols):
        """
        Fetch multiple equity details simultaneously
        
        Args:
            symbols (list): List of equity symbols to fetch
            
        Returns:
            dict: Dictionary mapping symbols to equity details
        """
        if not symbols:
            return {}
            
        # Convert symbols list to the format expected by the API
        params = {}
        for i, symbol in enumerate(symbols):
            params[f"symbol[{i}]"] = symbol
        
        response = self.api.safe_request("GET", "/instruments/equities", params=params)
        
        if response.status_code == 200:
            results = {}
            items = response.json().get("data", {}).get("items", [])
            
            for item in items:
                symbol = item.get("symbol")
                if symbol:
                    results[symbol] = item
                    
            print(f"[✓] Fetched {len(results)} equities simultaneously")
            return results
        else:
            print(f"[✗] Failed to fetch equities: {response.status_code}")
            return {}

    
    def check_liquidity_criteria(self, symbol, option_symbol=None):
        """
        Check if an instrument meets the liquidity criteria
        
        Args:
            symbol (str): Underlying symbol
            option_symbol (str, optional): Option symbol to check
            
        Returns:
            bool: True if instrument meets liquidity criteria, False otherwise
        """
        try:
            # Get config values
            min_volume = self.config.get("trading_config", {}).get("liquidity_min_volume", 1000000)
            min_oi = self.config.get("trading_config", {}).get("liquidity_min_oi", 500)
            max_spread = self.config.get("trading_config", {}).get("liquidity_max_spread", 0.10)
            
            # Check underlying volume
            equity = self.fetch_equity(symbol)
            if equity:
                volume = float(equity.get("volume", 0))
                if volume < min_volume:
                    self.logger.info(f"{symbol} failed volume check: {volume} < {min_volume}")
                    return False
            
            # If checking option, verify open interest and spread
            if option_symbol:
                option = self.fetch_equity_option(option_symbol)
                if option:
                    oi = float(option.get("open-interest", 0))
                    bid = float(option.get("bid", 0))
                    ask = float(option.get("ask", 0))
                    
                    # Calculate spread
                    spread = ask - bid if bid > 0 and ask > 0 else float('inf')
                    
                    if oi < min_oi:
                        self.logger.info(f"{option_symbol} failed OI check: {oi} < {min_oi}")
                        return False
                        
                    if spread > max_spread:
                        self.logger.info(f"{option_symbol} failed spread check: {spread} > {max_spread}")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking liquidity criteria: {e}")
            return False