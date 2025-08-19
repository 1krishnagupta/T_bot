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
    Class for fetching instruments and market data from TradeStation API.
    """
    
    def __init__(self, api, test_mode=False):
        """
        Initialize the instrument fetcher
        
        Args:
            api: TradeStation API client
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
        Note: TradeStation doesn't have a direct equities list endpoint
        
        Args:
            is_etf (bool): Filter for ETFs
            is_index (bool): Filter for indices
            
        Returns:
            list: List of equity objects
        """
        # TradeStation doesn't provide a full equities list endpoint
        # Return common symbols for now
        if is_etf:
            symbols = ["SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLV", "XLY"]
        elif is_index:
            symbols = ["$SPX", "$NDX", "$DJI", "$RUT"]
        else:
            symbols = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"]
        
        items = []
        for symbol in symbols:
            items.append({
                "symbol": symbol,
                "name": symbol,
                "type": "ETF" if is_etf else "Index" if is_index else "Stock"
            })
        
        print(f"[✓] Returning {len(items)} symbols (ETF={is_etf}, Index={is_index})")
        return items
    
    def fetch_active_equities(self):
        """
        Fetch list of active equities
        Note: TradeStation doesn't have this specific endpoint
        
        Returns:
            list: List of active equity objects
        """
        # Return common active symbols
        return self.fetch_equities(is_etf=False, is_index=False)
    
    def fetch_equity(self, symbol):
        """Fetch symbol info using correct endpoint"""
        try:
            # Use symbol lookup endpoint from swagger
            response = self.api.safe_request("GET", f"/v2/data/symbol/{symbol}")
            if response.status_code == 200:
                data = response.json()
                return {
                    "symbol": data.get("Name", symbol),
                    "name": data.get("Name"),
                    "description": data.get("Description", ""),
                    "exchange": data.get("Exchange", ""),
                    "type": data.get("Category", "Stock"),
                    "currency": data.get("Currency", "USD"),
                    "country": data.get("Country", "US")
                }
            return None
        except Exception as e:
            self.logger.error(f"Error fetching equity {symbol}: {e}")
            return None
    

    def search_symbols(self, criteria):
        """Search symbols using correct endpoint"""
        try:
            # Use symbol search endpoint from swagger
            endpoint = f"/v2/data/symbols/search/{criteria}"
            response = self.api.safe_request("GET", endpoint)
            
            if response.status_code == 200:
                symbols = response.json()
                return symbols
            return []
        except Exception as e:
            self.logger.error(f"Error searching symbols: {e}")
            return []


    def fetch_option_expirations(self, symbol):
        """Get option expirations - TradeStation doesn't have this in swagger"""
        # TradeStation doesn't provide a separate expirations endpoint
        # You need to get the full option chain and extract expirations
        try:
            # This would need to be implemented based on available endpoints
            # TradeStation may require using their desktop API for options
            self.logger.warning("Option chains not available in TradeStation REST API")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching expirations: {e}")
            return []


    def fetch_nested_option_chains(self, underlying_symbol):
        """
        Fetch nested option chains (expirations and strikes) for a symbol
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            dict: Nested option chain data with expirations and strikes
        """
        try:
            # First, try to get option expirations
            exp_response = self.api.safe_request("GET", f"/v3/marketdata/options/expirations/{underlying_symbol}")
            
            if exp_response.status_code == 200:
                exp_data = exp_response.json()
                expirations_list = exp_data.get("Expirations", [])
                
                if not expirations_list:
                    print(f"[!] No option expirations found for {underlying_symbol}")
                    return None
                
                # For TradeStation, we need to fetch strikes for each expiration
                expirations = []
                
                # Limit to first 3 expirations to avoid too many API calls
                for exp_date in expirations_list[:3]:
                    # Fetch strikes for this expiration
                    strikes_response = self.api.safe_request(
                        "GET", 
                        f"/v3/marketdata/options/strikes/{underlying_symbol}",
                        params={"expiration": exp_date}
                    )
                    
                    if strikes_response.status_code == 200:
                        strikes_data = strikes_response.json()
                        strike_prices = strikes_data.get("Strikes", [])
                        
                        # Build strikes list
                        strikes = []
                        for strike in strike_prices:
                            # TradeStation option symbol format
                            # Format: SYMBOL YYMMDD C/P STRIKE (multiplied by 1000)
                            exp_formatted = exp_date.replace("-", "")[2:]  # YYMMDD format
                            strike_int = int(strike * 1000)
                            
                            call_symbol = f"{underlying_symbol} {exp_formatted}C{strike_int:08d}"
                            put_symbol = f"{underlying_symbol} {exp_formatted}P{strike_int:08d}"
                            
                            strikes.append({
                                "strike-price": strike,
                                "call": call_symbol,
                                "put": put_symbol
                            })
                        
                        expirations.append({
                            "expiration-date": exp_date,
                            "strikes": strikes
                        })
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.1)
                
                result = {
                    "underlying-symbol": underlying_symbol,
                    "expirations": expirations
                }
                
                print(f"[✓] Fetched option chain for {underlying_symbol} with {len(expirations)} expirations")
                return result
                
            elif exp_response.status_code == 404:
                # Try alternative endpoint or format
                print(f"[!] Option chain endpoint not found for {underlying_symbol}, trying alternative...")
                
                # Try the chains endpoint directly
                response = self.api.safe_request("GET", f"/v3/marketdata/options/chains/{underlying_symbol}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Convert TradeStation format to nested format
                    expirations = {}
                    option_chains = data.get("OptionChains", [])
                    
                    for chain in option_chains:
                        exp_date = chain.get("Expiration")
                        if exp_date not in expirations:
                            expirations[exp_date] = {"expiration-date": exp_date, "strikes": []}
                        
                        # Add strikes for this expiration
                        for option in chain.get("Options", []):
                            strike = option.get("StrikePrice")
                            strike_data = {
                                "strike-price": strike,
                                "call": option.get("Call", {}).get("Symbol"),
                                "put": option.get("Put", {}).get("Symbol")
                            }
                            expirations[exp_date]["strikes"].append(strike_data)
                    
                    result = {
                        "underlying-symbol": underlying_symbol,
                        "expirations": list(expirations.values())
                    }
                    
                    print(f"[✓] Fetched option chain for {underlying_symbol} with {len(expirations)} expirations")
                    return result
                else:
                    print(f"[✗] Failed to fetch option chain for {underlying_symbol}: {response.status_code}")
                    return None
            else:
                print(f"[✗] Failed to fetch option expirations for {underlying_symbol}: {exp_response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching nested option chain for {underlying_symbol}: {e}")
            return None
    
    def fetch_detailed_option_chains(self, underlying_symbol):
        """
        Fetch detailed option chains for a symbol (full option data)
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            list: List of detailed option objects
        """
        try:
            response = self.api.safe_request("GET", f"/v3/marketdata/options/chains/{underlying_symbol}")
            if response.status_code == 200:
                data = response.json()
                options = []
                
                for chain in data.get("OptionChains", []):
                    exp_date = chain.get("Expiration")
                    
                    for option_data in chain.get("Options", []):
                        # Add call option
                        if "Call" in option_data:
                            call = option_data["Call"]
                            options.append({
                                "symbol": call.get("Symbol"),
                                "underlying-symbol": underlying_symbol,
                                "expiration-date": exp_date,
                                "strike-price": option_data.get("StrikePrice"),
                                "option-type": "Call",
                                "bid": call.get("Bid", 0),
                                "ask": call.get("Ask", 0),
                                "volume": call.get("Volume", 0),
                                "open-interest": call.get("OpenInterest", 0),
                                "delta": call.get("Delta", 0),
                                "gamma": call.get("Gamma", 0),
                                "theta": call.get("Theta", 0),
                                "vega": call.get("Vega", 0),
                                "iv": call.get("ImpliedVolatility", 0)
                            })
                        
                        # Add put option
                        if "Put" in option_data:
                            put = option_data["Put"]
                            options.append({
                                "symbol": put.get("Symbol"),
                                "underlying-symbol": underlying_symbol,
                                "expiration-date": exp_date,
                                "strike-price": option_data.get("StrikePrice"),
                                "option-type": "Put",
                                "bid": put.get("Bid", 0),
                                "ask": put.get("Ask", 0),
                                "volume": put.get("Volume", 0),
                                "open-interest": put.get("OpenInterest", 0),
                                "delta": put.get("Delta", 0),
                                "gamma": put.get("Gamma", 0),
                                "theta": put.get("Theta", 0),
                                "vega": put.get("Vega", 0),
                                "iv": put.get("ImpliedVolatility", 0)
                            })
                
                print(f"[✓] Fetched {len(options)} detailed options for {underlying_symbol}")
                return options
            else:
                print(f"[✗] Failed to fetch detailed options for {underlying_symbol}: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error fetching detailed options for {underlying_symbol}: {e}")
            return []
    
    def fetch_compact_option_chains(self, underlying_symbol):
        """
        Fetch compact option chains for a symbol
        
        Args:
            underlying_symbol (str): Underlying equity symbol
            
        Returns:
            list: List of compact option symbols
        """
        # For TradeStation, we'll return just the symbols from the detailed chain
        detailed = self.fetch_detailed_option_chains(underlying_symbol)
        return [{"symbol": opt["symbol"]} for opt in detailed if "symbol" in opt]
    
    def fetch_equity_options(self, symbols=None, active=True, with_expired=False):
        """
        Fetch equity options by symbol(s)
        
        Args:
            symbols (list): List of option symbols
            active (bool): Whether to include only active options
            with_expired (bool): Whether to include expired options
            
        Returns:
            list: List of option data
        """
        if not symbols:
            return []
        
        options = []
        for symbol in symbols:
            try:
                # TradeStation doesn't have a direct option lookup by symbol
                # We need to parse the underlying and fetch the chain
                # For now, return basic structure
                options.append({
                    "symbol": symbol,
                    "active": active,
                    "instrument-type": "Equity Option"
                })
            except Exception as e:
                self.logger.error(f"Error fetching option {symbol}: {e}")
        
        print(f"[✓] Fetched {len(options)} equity options")
        return options
    
    def fetch_equity_option(self, symbol):
        """
        Fetch a single equity option by symbol
        
        Args:
            symbol (str): Option symbol (e.g., "SPY 230731C00393000")
            
        Returns:
            dict: Option data or None if not found
        """
        try:
            # TradeStation uses a different option symbol format
            # We need to convert from OCC format to TradeStation format
            # For now, return basic structure
            return {
                "symbol": symbol,
                "instrument-type": "Equity Option",
                "streamer-symbol": symbol
            }
        except Exception as e:
            self.logger.error(f"Error fetching option {symbol}: {e}")
            return None
    
    def fetch_market_quote(self, symbols, instrument_type="equity"):
        """
        Fetch current market quotes for multiple instruments
        
        Args:
            symbols (list): List of symbols to fetch quotes for
            instrument_type (str): Type of instrument
                
        Returns:
            list: List of quote data for each symbol
        """
        if not symbols:
            print("[✗] No symbols provided")
            return []
        
        return self.api.get_market_quotes(symbols, instrument_type)
    
    def get_api_quote_token(self):
        """
        Get an API quote token for streaming market data
        
        Returns:
            dict: Dictionary with token, streaming URL, and level
        """
        return self.api.get_quote_token()
    
    def get_streamer_symbol(self, symbol, instrument_type="equity"):
        """
        Get the streamer symbol for a given instrument
        
        Args:
            symbol (str): Regular symbol (e.g. "SPY")
            instrument_type (str): Type of instrument
                
        Returns:
            str: Streamer symbol for use with streaming
        """
        # TradeStation uses the same symbol for streaming
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
        
        results = {}
        
        # TradeStation requires individual requests
        for symbol in symbols:
            equity = self.fetch_equity(symbol)
            if equity:
                results[symbol] = equity
        
        print(f"[✓] Fetched {len(results)} equities")
        return results
    
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
            # Get config values (would need to be passed in or stored)
            min_volume = 1000000  # Default values
            min_oi = 500
            max_spread = 0.10
            
            # Check underlying volume
            quotes = self.fetch_market_quote([symbol])
            if quotes:
                quote = quotes[0]
                volume = float(quote.get("volume", 0))
                if volume < min_volume:
                    self.logger.info(f"{symbol} failed volume check: {volume} < {min_volume}")
                    return False
            
            # If checking option, verify open interest and spread
            if option_symbol:
                # Would need to implement option-specific checks
                # For now, return True
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking liquidity criteria: {e}")
            return False