# Code/bot_core/market_data_client.py

import json
import time
import threading
import websocket
import logging
from datetime import datetime
import os
from typing import Dict, List, Optional, Callable, Any, Tuple
import asyncio
import concurrent.futures
import requests

from Code.bot_core.candle_builder import CandleBuilder
from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class MarketDataClient:
    """Client for streaming real-time market data from TastyTrade/DXLink"""
    
    def __init__(self, api_quote_token, on_quote=None, on_trade=None, on_greek=None, 
            on_candle=None, on_sector_update=None, on_mag7_update=None, save_to_db=True, 
            build_candles=True, candle_periods=(1, 2, 3, 5, 15), api=None): 
        """
        Initialize the market data client for TradeStation
        
        Args:
            api_quote_token (dict): Not used for TradeStation - kept for compatibility
            on_quote (callable): Callback for quote events
            on_trade (callable): Callback for trade events
            on_greek (callable): Callback for greek events
            on_candle (callable): Callback for candle events
            on_sector_update (callable): Callback for sector ETF updates
            on_mag7_update (callable): Callback for Mag7 stock updates
            save_to_db (bool): Whether to save data to database
            build_candles (bool): Whether to build candles from tick data
            candle_periods (tuple): Candle periods in minutes to build
            api: TradeStation API instance
        """
        self.api = api  # TradeStation API instance
        self.running = False
        self.stream_threads = {}
        self.active_streams = {}
        
        # Database storage
        self.save_to_db = save_to_db
        self.db = get_mongodb_handler() if save_to_db else None
        
        # Initialize collections if database is enabled
        if self.save_to_db and self.db:
            self.db.create_collection(COLLECTIONS['QUOTES'])
            self.db.create_collection(COLLECTIONS['TRADES'])
            self.db.create_collection(COLLECTIONS['GREEKS'])
            
            # Create indexes for faster queries
            self.db.create_index(COLLECTIONS['QUOTES'], [("symbol", 1), ("timestamp", 1)])
            self.db.create_index(COLLECTIONS['TRADES'], [("symbol", 1), ("timestamp", 1)])
            self.db.create_index(COLLECTIONS['GREEKS'], [("symbol", 1), ("timestamp", 1)])
        
        # Callbacks
        self.on_quote = on_quote
        self.on_trade = on_trade
        self.on_greek = on_greek
        self.on_candle = on_candle
        self.on_sector_update = on_sector_update
        self.on_mag7_update = on_mag7_update
        
        # For tracking sector ETF data
        self.sector_prices = {}
        self.sector_updates_pending = set()
        
        # Candle building
        self.build_candles = build_candles
        self.candle_periods = candle_periods
        self.candle_builder = CandleBuilder(periods=candle_periods, save_to_db=save_to_db) if build_candles else None
        
        # Thread pool for parallel processing
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"market_data_{today}.log")
        
        self.logger = logging.getLogger("MarketDataClient")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
   
    def _load_config(self):
        """Load configuration from credentials.txt or settings file"""
        try:
            # Try multiple paths to find config file
            config_paths = [
                os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'credentials.txt'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'settings.yaml'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'settings.txt')
            ]
            
            for path in config_paths:
                if os.path.exists(path):
                    import yaml
                    with open(path, 'r') as f:
                        data = yaml.safe_load(f)
                        
                    # Look for trading config in multiple places
                    if 'trading_config' in data:
                        return data['trading_config']
                    elif 'mag7_stocks' in data:  # Direct config
                        return data
                    elif 'broker' in data:  # In broker section
                        return data.get('broker', {})
                    
            return {}
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            return {}

    def _get_mag7_stocks(self):
        """Get Mag7 stocks from config or default"""
        config = self._load_config()
        return config.get('mag7_stocks', ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
    
    def _get_sector_etfs(self):
        """Get sector ETFs from config or default"""
        config = self._load_config()
        return config.get('sector_etfs', ["XLK", "XLF", "XLV", "XLY"])

    def _save_quote_to_db(self, quote):
        """Save a quote to the database"""
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['QUOTES'], quote)
        except Exception as e:
            self.logger.error(f"Error saving quote to database: {e}")
            
    def _save_trade_to_db(self, trade):
        """Save a trade to the database"""
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['TRADES'], trade)
        except Exception as e:
            self.logger.error(f"Error saving trade to database: {e}")
            
    def _save_greek_to_db(self, greek):
        """Save a greek to the database"""
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['GREEKS'], greek)
        except Exception as e:
            self.logger.error(f"Error saving greek to database: {e}")

    def connect(self):
        """Connect to TradeStation (HTTP streaming, not WebSocket)"""
        if not self.api:
            self.logger.error("No TradeStation API instance provided")
            return False
        
        # Ensure we're logged in
        if not self.api.check_and_refresh_session():
            if not self.api.login():
                self.logger.error("Failed to login to TradeStation")
                return False
        
        self.running = True
        
        # Start candle builder if enabled
        if self.build_candles and self.candle_builder:
            self.candle_builder.start()
            
            # Register candle callbacks
            if self.on_candle:
                self.candle_builder.register_callbacks(
                    on_completed=self.on_candle,
                    on_updated=self.on_candle
                )
        
        self.logger.info("TradeStation market data client connected")
        return True
        
    def _handle_tradestation_message(self, message):
        """Handle TradeStation format messages"""
        try:
            data = json.loads(message)
            # TradeStation sends different format than DXLink
            # Parse based on TradeStation's actual format
            if "Quotes" in data:
                for quote in data["Quotes"]:
                    self._process_tradestation_quote(quote)
            elif "Bars" in data:
                for bar in data["Bars"]:
                    self._process_tradestation_bar(bar)
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            
    def disconnect(self):
        """Disconnect from TradeStation"""
        self.running = False
        
        # Stop all streaming threads
        for thread_id in list(self.stream_threads.keys()):
            self._stop_stream(thread_id)
        
        # Stop candle builder if enabled
        if self.build_candles and self.candle_builder:
            self.candle_builder.stop()
        
        self.logger.info("TradeStation market data client disconnected")
            
    def subscribe_to_sector_etfs(self):
        """Subscribe to market data for sector ETFs"""
        sectors = self._get_sector_etfs()
        self.logger.info(f"Subscribing to sector ETFs: {', '.join(sectors)}")
        return self.subscribe(sectors, is_sector=True)
    
    def subscribe_to_mag7_stocks(self, mag7_stocks=None):
        """Subscribe to market data for Magnificent 7 stocks"""
        if mag7_stocks is None:
            mag7_stocks = self._get_mag7_stocks()


    def determine_sector_status(self, sector, price):
        """
        Determine sector status based on price movements
        
        Args:
            sector (str): Sector symbol
            price (float): Current price
            
        Returns:
                str: Status ("bullish", "bearish", or "neutral")
        """
        # In a real implementation, this would analyze price relative to moving averages, etc.
        # For now, we'll use a simple approach based on recent price changes
        
        # If we don't have stored data for comparison, just return neutral
        if sector not in self.sector_prices:
            return "neutral"
            
        # Compare to previously stored price if available
        prev_price = self.sector_prices.get(sector)
        if prev_price and prev_price > 0:
            # Calculate percent change
            pct_change = ((price - prev_price) / prev_price) * 100
            
            if pct_change > 0.1:  # 0.1% up
                return "bullish"
            elif pct_change < -0.1:  # 0.1% down
                return "bearish"
                
        return "neutral"
        
    def subscribe(self, symbols, event_types=None, is_sector=False):
        """
        Subscribe to market events for the given symbols using TradeStation HTTP streaming
        
        Args:
            symbols (list): List of symbols to subscribe to
            event_types (list): Not used for TradeStation - kept for compatibility
            is_sector (bool): Whether these are sector ETFs
                
        Returns:
            str: Stream ID for the subscription
        """
        if not self.running:
            self.logger.error("Market data client not connected")
            return None
        
        if not symbols:
            return None
        
        # Generate unique stream ID
        stream_id = f"stream_{len(self.stream_threads)}_{int(time.time())}"
        
        # Start streaming thread for these symbols
        thread = threading.Thread(
            target=self._stream_quotes,
            args=(symbols, stream_id, is_sector)
        )
        thread.daemon = True
        thread.start()
        
        self.stream_threads[stream_id] = thread
        self.active_streams[stream_id] = {
            "symbols": symbols,
            "is_sector": is_sector
        }
        
        self.logger.info(f"Subscribed to {len(symbols)} symbols on stream {stream_id}")
        return stream_id
        
    def unsubscribe(self, stream_id):
        """
        Unsubscribe from a stream
        
        Args:
            stream_id (str): Stream ID to unsubscribe from
        """
        self._stop_stream(stream_id)

    def _stop_stream(self, stream_id):
        """Stop a specific stream"""
        if stream_id in self.active_streams:
            del self.active_streams[stream_id]
        
        if stream_id in self.stream_threads:
            # Thread will stop on next iteration when it checks active_streams
            del self.stream_threads[stream_id]
    
    def _stream_quotes(self, symbols, stream_id, is_sector=False):
        """
        Stream quotes for symbols using TradeStation HTTP streaming
        
        Args:
            symbols (list): Symbols to stream
            stream_id (str): Unique stream identifier
            is_sector (bool): Whether these are sector ETFs
        """
        while self.running and stream_id in self.active_streams:
            try:
                # Use quote snapshots endpoint for continuous updates
                endpoint = f"/v2/stream/quote/snapshots/{','.join(symbols)}"
                
                headers = self.api.get_auth_headers()
                headers['Accept'] = 'application/vnd.tradestation.streams+json'
                
                url = f"{self.api.base_url}{endpoint}"
                
                # Make streaming request
                response = requests.get(url, headers=headers, stream=True, timeout=300)
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to stream quotes: {response.status_code}")
                    time.sleep(5)
                    continue
                
                # Process streaming response
                for line in response.iter_lines():
                    if not self.running or stream_id not in self.active_streams:
                        break
                    
                    if line:
                        try:
                            line_str = line.decode('utf-8').strip()
                            
                            # Skip END marker
                            if line_str == 'END':
                                break
                            
                            # Skip ERROR lines
                            if line_str.startswith('ERROR'):
                                self.logger.error(f"Stream error: {line_str}")
                                break
                            
                            # Parse JSON quote data
                            quote_data = json.loads(line_str)
                            
                            # Process quote
                            self._process_quote(quote_data, is_sector)
                            
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            self.logger.error(f"Error processing quote: {e}")
                
            except Exception as e:
                self.logger.error(f"Error in quote stream: {e}")
                time.sleep(5)

    def _process_quote(self, quote_data, is_sector=False):
        """Process a quote from TradeStation"""
        try:
            symbol = quote_data.get("Symbol")
            if not symbol:
                return
            
            # Convert to standard format
            quote = {
                "symbol": symbol,
                "bid": float(quote_data.get("Bid", 0)),
                "ask": float(quote_data.get("Ask", 0)),
                "bid_size": float(quote_data.get("BidSize", 0)),
                "ask_size": float(quote_data.get("AskSize", 0)),
                "last": float(quote_data.get("Last", 0)),
                "volume": float(quote_data.get("Volume", 0)),
                "timestamp": datetime.now().isoformat()
            }
            
            # Save to database
            if self.save_to_db:
                self._save_quote_to_db(quote)
            
            # Process for candle building
            if self.build_candles and self.candle_builder:
                self.candle_builder.process_quote(quote)
            
            # Get sector ETFs from config
            sector_etfs = self._get_sector_etfs()
            
            # Handle sector updates
            if is_sector and symbol in sector_etfs:
                price = (quote["bid"] + quote["ask"]) / 2 if quote["bid"] > 0 and quote["ask"] > 0 else quote["last"]
                
                if price > 0:
                    self.sector_prices[symbol] = price
                    
                    # Determine status
                    status = self._determine_sector_status(symbol, price)
                    
                    # Call sector update callback
                    if self.on_sector_update:
                        self.on_sector_update(symbol, status, price)
            
            # Handle Mag7 updates
            mag7_stocks = self._get_mag7_stocks()
            if symbol in mag7_stocks and self.on_mag7_update:
                price = (quote["bid"] + quote["ask"]) / 2 if quote["bid"] > 0 and quote["ask"] > 0 else quote["last"]
                if price > 0:
                    self.on_mag7_update(symbol, price)
            
            # Call quote callback
            if self.on_quote:
                self.on_quote(quote)
                
        except Exception as e:
            self.logger.error(f"Error processing quote: {e}")

    def _determine_sector_status(self, sector, price):
        """
        Determine sector status based on price movements
        
        Args:
            sector (str): Sector symbol
            price (float): Current price
            
        Returns:
            str: Status ("bullish", "bearish", or "neutral")
        """
        # Store previous prices
        if not hasattr(self, '_prev_sector_prices'):
            self._prev_sector_prices = {}
        
        prev_price = self._prev_sector_prices.get(sector, price)
        self._prev_sector_prices[sector] = price
        
        if prev_price > 0:
            pct_change = ((price - prev_price) / prev_price) * 100
            
            if pct_change > 0.1:  # 0.1% up
                return "bullish"
            elif pct_change < -0.1:  # 0.1% down
                return "bearish"
        
        return "neutral"

    def subscribe_to_candles(self, symbol, period="1d", from_time=None):
        """
        Subscribe to historical candle data using TradeStation streaming
        
        Args:
            symbol (str): Symbol to fetch candles for
            period (str): Candle period (e.g. "5m", "1h", "1d")
            from_time (int): Start time as Unix timestamp
                
        Returns:
            str: Stream ID for the subscription
        """
        if not self.running:
            return None
        
        # Generate unique stream ID
        stream_id = f"candle_stream_{symbol}_{period}_{int(time.time())}"
        
        # Start candle streaming thread
        thread = threading.Thread(
            target=self._stream_candles,
            args=(symbol, period, from_time, stream_id)
        )
        thread.daemon = True
        thread.start()
        
        self.stream_threads[stream_id] = thread
        self.active_streams[stream_id] = {
            "symbol": symbol,
            "period": period,
            "type": "candle"
        }
        
        return stream_id
    
    def _stream_candles(self, symbol, period, from_time, stream_id):
        """Stream historical candles from TradeStation"""
        try:
            # Convert period to TradeStation format
            interval, unit = self._parse_period(period)
            
            # Calculate date range
            if from_time:
                start_date = datetime.fromtimestamp(from_time).strftime("%m-%d-%Y")
            else:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%m-%d-%Y")
            
            endpoint = f"/v2/stream/barchart/{symbol}/{interval}/{unit}/{start_date}"
            
            headers = self.api.get_auth_headers()
            headers['Accept'] = 'application/vnd.tradestation.streams+json'
            
            url = f"{self.api.base_url}{endpoint}"
            
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to stream candles: {response.status_code}")
                return
            
            # Process streaming response
            for line in response.iter_lines():
                if not self.running or stream_id not in self.active_streams:
                    break
                
                if line:
                    try:
                        line_str = line.decode('utf-8').strip()
                        
                        if line_str == 'END' or line_str.startswith('ERROR'):
                            break
                        
                        # Parse candle data
                        candle_data = json.loads(line_str)
                        
                        # Convert timestamp
                        ts_str = candle_data.get('TimeStamp', '')
                        if '/Date(' in ts_str:
                            ms = int(ts_str.replace('/Date(', '').replace(')/', ''))
                            timestamp = datetime.fromtimestamp(ms / 1000)
                        else:
                            timestamp = datetime.now()
                        
                        # Create standard candle format
                        candle = {
                            "symbol": symbol,
                            "period": period,
                            "timestamp": timestamp.isoformat(),
                            "open": float(candle_data.get("Open", 0)),
                            "high": float(candle_data.get("High", 0)),
                            "low": float(candle_data.get("Low", 0)),
                            "close": float(candle_data.get("Close", 0)),
                            "volume": float(candle_data.get("TotalVolume", 0))
                        }
                        
                        # Call candle callback
                        if self.on_candle:
                            self.on_candle(candle)
                            
                    except Exception as e:
                        self.logger.error(f"Error processing candle: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error in candle stream: {e}")

    def _parse_period(self, period):
        """Parse period string to interval and unit"""
        if period.endswith('m'):
            return int(period[:-1]), "Minute"
        elif period.endswith('h'):
            return int(period[:-1]), "Hour"
        elif period.endswith('d'):
            return int(period[:-1]), "Daily"
        else:
            return 5, "Minute"  # Default
                        
    def determine_sector_status(self, sector, price):
        """
        Determine sector status based on price movements
        
        Args:
            sector (str): Sector symbol
            price (float): Current price
            
        Returns:
            str: Status ("bullish", "bearish", or "neutral")
        """
        # In a real implementation, this would analyze price relative to moving averages, etc.
        # For now, we'll use a simple approach based on recent price changes
        
        # If we don't have stored data for comparison, just return neutral
        if sector not in self.sector_prices:
            return "neutral"
            
        # Compare to previously stored price if available
        prev_price = self.sector_prices.get(sector)
        if prev_price and prev_price > 0:
            # Calculate percent change
            pct_change = ((price - prev_price) / prev_price) * 100
            
            if pct_change > 0.1:  # 0.1% up
                return "bullish"
            elif pct_change < -0.1:  # 0.1% down
                return "bearish"
                
        return "neutral"
        
    def get_quotes_from_db(self, symbol, start_time=None, end_time=None, limit=100):
        """Get quotes from the database"""
        if not self.save_to_db or not self.db:
            return []
            
        try:
            query = {"symbol": symbol}
            
            if start_time:
                if 'timestamp' not in query:
                    query['timestamp'] = {}
                query['timestamp']['$gte'] = start_time if isinstance(start_time, str) else start_time.isoformat()
                
            if end_time:
                if 'timestamp' not in query:
                    query['timestamp'] = {}
                query['timestamp']['$lte'] = end_time if isinstance(end_time, str) else end_time.isoformat()
                
            quotes = self.db.find_many(COLLECTIONS['QUOTES'], query, limit=limit)
            return sorted(quotes, key=lambda x: x['timestamp'])
            
        except Exception as e:
            self.logger.error(f"Error getting quotes from database: {e}")
            return []
 

    # def _send_setup(self):
    #     """Send SETUP message to initialize connection"""
    #     setup_msg = {
    #         "type": "SETUP",
    #         "channel": 0,
    #         "version": "0.1-DXF-JS/0.3.0",
    #         "keepaliveTimeout": 60,
    #         "acceptKeepaliveTimeout": 60
    #     }
    #     self._send_message(setup_msg)
        
    # def _authorize(self):
    #     """Send AUTH message with token"""
    #     auth_msg = {
    #         "type": "AUTH",
    #         "channel": 0,
    #         "token": self.token
    #     }
    #     self._send_message(auth_msg)
        
    # def _create_channel(self, service="FEED"):
    #     """
    #     Create a new channel for subscriptions
        
    #     Args:
    #         service (str): Service to use for the channel
            
    #     Returns:
    #         int: Channel ID
    #     """
    #     channel_id = self.channel_counter
    #     self.channel_counter += 1
        
    #     channel_msg = {
    #         "type": "CHANNEL_REQUEST",
    #         "channel": channel_id,
    #         "service": service,
    #         "parameters": {"contract": "AUTO"}
    #     }
    #     self._send_message(channel_msg)
        
    #     return channel_id
        
    # def _setup_feed(self, channel_id):
    #     """
    #     Setup feed configuration for a channel
        
    #     Args:
    #         channel_id (int): Channel ID to setup
    #     """
    #     feed_setup_msg = {
    #         "type": "FEED_SETUP",
    #         "channel": channel_id,
    #         "acceptAggregationPeriod": 0.1,
    #         "acceptDataFormat": "COMPACT",
    #         "acceptEventFields": {
    #             "Trade": ["eventType", "eventSymbol", "price", "dayVolume", "size", "time", "exchangeCode", "dayId"],
    #             "TradeETH": ["eventType", "eventSymbol", "price", "dayVolume", "size", "time", "exchangeCode", "dayId"],
    #             "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize", "time", "bidExchangeCode", "askExchangeCode"],
    #             "Greeks": ["eventType", "eventSymbol", "volatility", "delta", "gamma", "theta", "rho", "vega"],
    #             "Profile": ["eventType", "eventSymbol", "description", "shortSaleRestriction", "tradingStatus"],
    #             "Summary": ["eventType", "eventSymbol", "openInterest", "dayOpenPrice", "dayHighPrice", "dayLowPrice", "prevDayClosePrice"],
    #             "Candle": ["eventType", "eventSymbol", "time", "sequence", "count", "open", "high", "low", "close", "volume", "vwap"]
    #         }
    #     }
    #     self._send_message(feed_setup_msg)
        
    # def request_sector_updates(self):
    #     """
    #     Manually request updates for all sector ETFs to ensure continuous data
    #     """
    #     try:
    #         # Find the sector channel
    #         sector_channel_id = None
    #         for channel_id, channel_info in self.channels.items():
    #             if channel_info.get("is_sector", False):
    #                 sector_channel_id = channel_id
    #                 break
                    
    #         if not sector_channel_id:
    #             return  # No sector channel found
                
    #         # Request updates for all sectors
    #         sectors = ["XLK", "XLF", "XLV", "XLY"]
    #         requests = []
            
    #         for sector in sectors:
    #             requests.append({
    #                 "type": "Quote",
    #                 "symbol": sector
    #             })
                
    #         # Send request message
    #         if requests:
    #             request_msg = {
    #                 "type": "FEED_REQUEST",
    #                 "channel": sector_channel_id,
    #                 "requests": requests
    #             }
    #             self._send_message(request_msg)
                
    #     except Exception as e:
    #         self.logger.error(f"Error requesting sector updates: {e}")


    # def _cleanup_old_data(self):
    #     """
    #     Clean up old data to free memory
    #     """
    #     try:
    #         # Check if we need to clean up
    #         if not hasattr(self, '_last_cleanup_time'):
    #             self._last_cleanup_time = time.time()
    #             return
                
    #         # Only clean up periodically (e.g., every 10 minutes)
    #         current_time = time.time()
    #         if current_time - self._last_cleanup_time < 600:  # 600 seconds = 10 minutes
    #             return
                
    #         self.logger.info("Cleaning up old data to free memory")
            
    #         # Clean up quotes data
    #         if hasattr(self, 'candle_data'):
    #             # Only keep the last 1000 candles per key
    #             for key in list(self.candle_data.keys()):
    #                 if len(self.candle_data[key]) > 1000:
    #                     self.candle_data[key] = self.candle_data[key][-1000:]
            
    #         # Update last cleanup time
    #         self._last_cleanup_time = current_time
            
    #     except Exception as e:
    #         self.logger.error(f"Error during data cleanup: {e}")


    # def _send_message(self, message):
    #     """
    #     Send a message to the websocket
        
    #     Args:
    #         message (dict): Message to send
    #     """
    #     if not self.ws:
    #         self.logger.error("WebSocket not initialized")
    #         return
            
    #     try:
    #         self.ws.send(json.dumps(message))
    #         self.logger.debug(f"Sent: {message}")
    #     except Exception as e:
    #         self.logger.error(f"Error sending message: {e}")
            
    # def _handle_message(self, message):
    #     """
    #     Handle incoming messages from the websocket
        
    #     Args:
    #         message (str): Message received from the websocket
    #     """
    #     try:
    #         data = json.loads(message)
    #         msg_type = data.get("type")
            
    #         self.logger.debug(f"Received: {msg_type}")
            
    #         if msg_type == "AUTH_STATE":
    #             state = data.get("state")
    #             if state == "UNAUTHORIZED":
    #                 self._authorize()
    #             elif state == "AUTHORIZED":
    #                 self.logger.info("Successfully authorized")
                    
    #         elif msg_type == "FEED_DATA":
    #             self._handle_feed_data(data)
                
    #     except Exception as e:
    #         self.logger.error(f"Error handling message: {e}")
            
    # def _handle_feed_data(self, data):
    #     """
    #     Handle feed data messages
        
    #     Args:
    #         data (dict): Feed data message
    #     """
    #     channel = data.get("channel")
    #     feed_data = data.get("data", [])
        
    #     if not feed_data or len(feed_data) < 2:
    #         return
            
    #     event_type = feed_data[0]
    #     event_data = feed_data[1]
        
    #     # Get current timestamp (ISO format)
    #     timestamp = datetime.now().isoformat()
        
    #     if event_type == "Quote":
    #         # Print data structure for the first Quote
    #         if self.first_quote and len(event_data) >= 6:
    #             self.first_quote = False
    #             self.logger.debug("Quote data structure: " + json.dumps({
    #                 "eventType": event_data[0] if len(event_data) > 0 else None,
    #                 "symbol": event_data[1] if len(event_data) > 1 else None,
    #                 "bidPrice": event_data[2] if len(event_data) > 2 else None,
    #                 "askPrice": event_data[3] if len(event_data) > 3 else None,
    #                 "bidSize": event_data[4] if len(event_data) > 4 else None,
    #                 "askSize": event_data[5] if len(event_data) > 5 else None,
    #                 "time": event_data[6] if len(event_data) > 6 else None,
    #             }))
                
    #         # Parse quote data and call callback
    #         if len(event_data) >= 6:
    #             # Format: ["Quote", symbol, bidPrice, askPrice, bidSize, askSize, time]
    #             symbol = event_data[1]
                
    #             # Ensure all price values are floats, not strings
    #             try:
    #                 bid_price = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
    #                 ask_price = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
    #                 bid_size = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
    #                 ask_size = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
    #             except (ValueError, TypeError) as e:
    #                 self.logger.error(f"Error converting quote values for {symbol}: {e}")
    #                 bid_price = 0.0
    #                 ask_price = 0.0
    #                 bid_size = 0.0
    #                 ask_size = 0.0
                
    #             quote = {
    #                 "symbol": symbol,
    #                 "bid": bid_price,
    #                 "ask": ask_price,
    #                 "bid_size": bid_size,
    #                 "ask_size": ask_size,
    #                 "timestamp": timestamp
    #             }
                
    #             # Add exchange time if available
    #             if len(event_data) > 6:
    #                 quote["exchange_time"] = event_data[6]
                    
    #             # Save to database
    #             self._save_quote_to_db(quote)
                
    #             # Check if this is a sector ETF and update if so
    #             channel_info = self.channels.get(channel, {})
    #             if channel_info.get("is_sector", False) and symbol in ["XLK", "XLF", "XLV", "XLY"]:
    #                 # Calculate mid price
    #                 if bid_price > 0 and ask_price > 0:
    #                     price = (bid_price + ask_price) / 2
    #                 elif bid_price > 0:
    #                     price = bid_price
    #                 elif ask_price > 0:
    #                     price = ask_price
    #                 else:
    #                     price = 0.0
                        
    #                 # Only process if we have a valid price
    #                 if price > 0:
    #                     # Store price
    #                     self.sector_prices[symbol] = price
                        
    #                     # Calculate price change
    #                     prev_price = getattr(self, '_prev_sector_prices', {}).get(symbol, price)
    #                     if not hasattr(self, '_prev_sector_prices'):
    #                         self._prev_sector_prices = {}
                            
    #                     change_pct = ((price - prev_price) / prev_price) * 100 if prev_price > 0 else 0
                        
    #                     # Determine status based on price movement
    #                     status = "neutral"
    #                     if abs(change_pct) > 0.05:  # 0.05% threshold
    #                         status = "bullish" if change_pct > 0 else "bearish"
                        
    #                     self._prev_sector_prices[symbol] = price
                        
    #                     # Call sector update callback immediately
    #                     if self.on_sector_update:
    #                         self.on_sector_update(symbol, status, price)
                    
    #                 # Check if we've received updates for all sectors
    #                 # If all sectors updated or 2 seconds passed since first sector update
    #                 all_updated = not self.sector_updates_pending
    #                 time_for_all_updates = hasattr(self, '_sector_update_start_time') and \
    #                     (time.time() - self._sector_update_start_time) > 2.0
                        
    #                 if (all_updated or time_for_all_updates) and self.on_sector_update:
    #                     # Call with a special signal that all sectors are updated
    #                     self.on_sector_update("ALL_SECTORS_UPDATED", "", 0)
    #                     # Reset for next batch of updates
    #                     self.sector_updates_pending = set(["XLK", "XLF", "XLV", "XLY"])
    #                     if hasattr(self, '_sector_update_start_time'):
    #                         delattr(self, '_sector_update_start_time')
    #                 elif not hasattr(self, '_sector_update_start_time') and self.sector_updates_pending:
    #                     # Start the timer for sector updates
    #                     self._sector_update_start_time = time.time()
                

    #             # Check if this is a Mag7 stock and we have a callback
    #             mag7_stocks = self._get_mag7_stocks()
    #             if symbol in mag7_stocks and self.on_mag7_update:  # Use self.on_mag7_update instead of hasattr
    #                 # Calculate mid price
    #                 if bid_price > 0 and ask_price > 0:
    #                     price = (bid_price + ask_price) / 2
    #                 elif bid_price > 0:
    #                     price = bid_price
    #                 elif ask_price > 0:
    #                     price = ask_price
    #                 else:
    #                     price = 0.0
                    
    #                 if price > 0:
    #                     self.on_mag7_update(symbol, price)


    #             # Call user callback
    #             if self.on_quote:
    #                 self.on_quote(quote)
                    
    #     elif event_type == "Trade":
    #         # Print data structure for the first Trade
    #         if self.first_trade and len(event_data) >= 5:
    #             self.first_trade = False
    #             self.logger.debug("Trade data structure: " + json.dumps({
    #                 "eventType": event_data[0] if len(event_data) > 0 else None,
    #                 "symbol": event_data[1] if len(event_data) > 1 else None,
    #                 "price": event_data[2] if len(event_data) > 2 else None,
    #                 "dayVolume": event_data[3] if len(event_data) > 3 else None,
    #                 "size": event_data[4] if len(event_data) > 4 else None,
    #                 "time": event_data[5] if len(event_data) > 5 else None,
    #             }))
                
    #         # Parse trade data and call callback
    #         if len(event_data) >= 5:
    #             # Format: ["Trade", symbol, price, dayVolume, size, time]
    #             try:
    #                 price = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
    #                 volume = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
    #                 size = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
    #             except (ValueError, TypeError) as e:
    #                 self.logger.error(f"Error converting trade values: {e}")
    #                 price = 0.0
    #                 volume = 0.0
    #                 size = 0.0
                
    #             trade = {
    #                 "symbol": event_data[1],
    #                 "price": price,
    #                 "volume": volume,
    #                 "size": size,
    #                 "timestamp": timestamp
    #             }
                
    #             # Add exchange time if available
    #             if len(event_data) > 5:
    #                 trade["exchange_time"] = event_data[5]
                    
    #             # Save to database
    #             self._save_trade_to_db(trade)
                
    #             # Call user callback
    #             if self.on_trade:
    #                 self.on_trade(trade)
                
    #             # Process trade for candle building
    #             if self.build_candles and self.candle_builder:
    #                 self.candle_builder.process_trade(trade)
                    
    #     elif event_type == "Greeks":
    #         # Print data structure for the first Greek
    #         if self.first_greek and len(event_data) >= 7:
    #             self.first_greek = False
    #             self.logger.debug("Greek data structure: " + json.dumps({
    #                 "eventType": event_data[0] if len(event_data) > 0 else None,
    #                 "symbol": event_data[1] if len(event_data) > 1 else None,
    #                 "volatility": event_data[2] if len(event_data) > 2 else None,
    #                 "delta": event_data[3] if len(event_data) > 3 else None,
    #                 "gamma": event_data[4] if len(event_data) > 4 else None,
    #                 "theta": event_data[5] if len(event_data) > 5 else None,
    #                 "rho": event_data[6] if len(event_data) > 6 else None,
    #                 "vega": event_data[7] if len(event_data) > 7 else None
    #             }))
                
    #         # Parse greek data and call callback
    #         if len(event_data) >= 7:
    #             # Format: ["Greeks", symbol, volatility, delta, gamma, theta, rho, vega]
    #             try:
    #                 volatility = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
    #                 delta = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
    #                 gamma = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
    #                 theta = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
    #                 rho = float(event_data[6]) if event_data[6] and event_data[6] != "NaN" else 0.0
    #             except (ValueError, TypeError) as e:
    #                 self.logger.error(f"Error converting greek values: {e}")
    #                 volatility = 0.0
    #                 delta = 0.0
    #                 gamma = 0.0
    #                 theta = 0.0
    #                 rho = 0.0
                
    #             greek = {
    #                 "symbol": event_data[1],
    #                 "volatility": volatility,
    #                 "delta": delta,
    #                 "gamma": gamma,
    #                 "theta": theta,
    #                 "rho": rho,
    #                 "timestamp": timestamp
    #             }
                
    #             # Add vega if available
    #             if len(event_data) > 7:
    #                 try:
    #                     vega = float(event_data[7]) if event_data[7] and event_data[7] != "NaN" else 0.0
    #                     greek["vega"] = vega
    #                 except (ValueError, TypeError):
    #                     greek["vega"] = 0.0
                    
    #             # Save to database
    #             self._save_greek_to_db(greek)
                
    #             # Call user callback
    #             if self.on_greek:
    #                 self.on_greek(greek)
                    
    #     elif event_type == "Candle":
    #         # Print data structure for the first Candle
    #         if self.first_candle and len(event_data) >= 10:
    #             self.first_candle = False
    #             self.logger.debug("Candle data structure: " + json.dumps({
    #                 "eventType": event_data[0] if len(event_data) > 0 else None,
    #                 "symbol": event_data[1] if len(event_data) > 1 else None,
    #                 "time": event_data[2] if len(event_data) > 2 else None,
    #                 "sequence": event_data[3] if len(event_data) > 3 else None,
    #                 "count": event_data[4] if len(event_data) > 4 else None,
    #                 "open": event_data[5] if len(event_data) > 5 else None,
    #                 "high": event_data[6] if len(event_data) > 6 else None,
    #                 "low": event_data[7] if len(event_data) > 7 else None,
    #                 "close": event_data[8] if len(event_data) > 8 else None,
    #                 "volume": event_data[9] if len(event_data) > 9 else None,
    #                 "vwap": event_data[10] if len(event_data) > 10 else None
    #             }))
                
    #         # Parse candle data
    #         if len(event_data) >= 10:
    #             # Extract period from symbol
    #             symbol = event_data[1]
    #             period = "unknown"
    #             if "{=" in symbol:
    #                 parts = symbol.split("{=")
    #                 symbol = parts[0]
    #                 period = parts[1].rstrip("}")
                    
    #             # Format: ["Candle", symbol, time, sequence, count, open, high, low, close, volume, vwap]
    #             try:
    #                 time_value = event_data[2]
    #                 open_price = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
    #                 high_price = float(event_data[6]) if event_data[6] and event_data[6] != "NaN" else 0.0
    #                 low_price = float(event_data[7]) if event_data[7] and event_data[7] != "NaN" else 0.0
    #                 close_price = float(event_data[8]) if event_data[8] and event_data[8] != "NaN" else 0.0
    #                 volume = float(event_data[9]) if event_data[9] and event_data[9] != "NaN" else 0.0
    #             except (ValueError, TypeError) as e:
    #                 self.logger.error(f"Error converting candle values: {e}")
    #                 open_price = 0.0
    #                 high_price = 0.0
    #                 low_price = 0.0
    #                 close_price = 0.0
    #                 volume = 0.0
                
    #             candle = {
    #                 "symbol": symbol,
    #                 "period": period,
    #                 "time": time_value,
    #                 "open": open_price,
    #                 "high": high_price,
    #                 "low": low_price,
    #                 "close": close_price,
    #                 "volume": volume,
    #                 "timestamp": timestamp
    #             }
                
    #             # Call user callback
    #             if self.on_candle:
    #                 self.on_candle(candle)
                    
    # def _keepalive_loop(self):
    #     """Send keepalive messages periodically"""
    #     while self.running:
    #         try:
    #             # Send keepalive every 30 seconds
    #             time.sleep(30)
    #             if self.running:
    #                 keepalive_msg = {
    #                     "type": "KEEPALIVE",
    #                     "channel": 0
    #                 }
    #                 self._send_message(keepalive_msg)
    #         except Exception as e:
    #             self.logger.error(f"Error in keepalive loop: {e}")

    # def _start_sector_polling(self, channel_id, sectors):
    #     """
    #     Start a background thread to poll for sector updates continuously
        
    #     Args:
    #         channel_id (int): Channel ID for sector data
    #         sectors (list): List of sector symbols
    #     """
    #     def poll_sectors():
    #         if not self.running:
    #             return
                
    #         try:
    #             # Request quotes for all sectors
    #             for sector in sectors:
    #                 quote_request = {
    #                     "type": "FEED_REQUEST",
    #                     "channel": channel_id,
    #                     "requests": [{
    #                         "type": "Quote",
    #                         "symbol": sector
    #                     }]
    #                 }
    #                 self._send_message(quote_request)
    #                 time.sleep(0.1)  # Small delay between requests
                    
    #             # Schedule next poll after 2 seconds
    #             if self.running:
    #                 threading.Timer(2.0, poll_sectors).start()
                    
    #         except Exception as e:
    #             self.logger.error(f"Error in sector polling: {e}")
                
    #     # Start polling thread
    #     threading.Thread(target=poll_sectors, daemon=True).start()

    # def _schedule_sector_updates(self, channel_id):
    #     """
    #     Schedule periodic updates to ensure we're getting data for all sectors
    #     """
    #     def check_sectors():
    #         if not self.running:
    #             return
                
    #         sectors = ["XLK", "XLF", "XLV", "XLY"]
    #         for sector in sectors:
    #             # Request a quote update for this sector
    #             quote_msg = {
    #                 "type": "FEED_REQUEST",
    #                 "channel": channel_id,
    #                 "requests": [{
    #                     "type": "Quote",
    #                     "symbol": sector
    #                 }]
    #             }
    #             self._send_message(quote_msg)
                
    #         # Schedule next check
    #         threading.Timer(0.5, check_sectors).start()
        
    #     # Start the initial check
    #     check_sectors()