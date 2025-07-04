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

from Code.bot_core.candle_builder import CandleBuilder
from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class MarketDataClient:
    """Client for streaming real-time market data from TastyTrade/DXLink"""
    
    def __init__(self, api_quote_token, on_quote=None, on_trade=None, on_greek=None, 
            on_candle=None, on_sector_update=None, save_to_db=True, build_candles=True, 
            candle_periods=(1, 2, 3, 5, 15), api=None): 
        """
        Initialize the market data client
        
        Args:
            api_quote_token (dict): API quote token from TastyTrade API
            on_quote (callable): Callback for quote events
            on_trade (callable): Callback for trade events
            on_greek (callable): Callback for greek events
            on_candle (callable): Callback for candle events
            on_sector_update (callable): Callback for sector ETF updates
            save_to_db (bool): Whether to save data to database
            build_candles (bool): Whether to build candles from tick data
            candle_periods (tuple): Candle periods in minutes to build
            api: TastyTrade API instance for historical data fetching
        """
        self.token = api_quote_token.get("token")
        self.dxlink_url = api_quote_token.get("dxlink-url")
        self.level = api_quote_token.get("level", "api")
        self.api = api  # Store the API reference
        
        self.ws = None
        self.running = False
        self.keepalive_thread = None
        self.channels = {}
        self.channel_counter = 1
        
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
        
        # First record flags to print JSON structure only once
        self.first_quote = True
        self.first_trade = True
        self.first_greek = True
        self.first_candle = True
        
        # Callbacks
        self.on_quote = on_quote
        self.on_trade = on_trade
        self.on_greek = on_greek
        self.on_candle = on_candle
        self.on_sector_update = on_sector_update
        
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
            
    def _save_quote_to_db(self, quote):
        """
        Save a quote to the database
        
        Args:
            quote (dict): Quote data to save
        """
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['QUOTES'], quote)
        except Exception as e:
            self.logger.error(f"Error saving quote to database: {e}")
            
    def _save_trade_to_db(self, trade):
        """
        Save a trade to the database
        
        Args:
            trade (dict): Trade data to save
        """
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['TRADES'], trade)
        except Exception as e:
            self.logger.error(f"Error saving trade to database: {e}")
            
    def _save_greek_to_db(self, greek):
        """
        Save a greek to the database
        
        Args:
            greek (dict): Greek data to save
        """
        if not self.save_to_db or not self.db:
            return
            
        try:
            self.db.insert_one(COLLECTIONS['GREEKS'], greek)
        except Exception as e:
            self.logger.error(f"Error saving greek to database: {e}")

    def connect(self):
        """
        Connect to DXLink websocket and initialize the connection
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        if not self.token or not self.dxlink_url:
            self.logger.error("Missing token or URL for DXLink connection")
            return False
            
        # Define websocket callbacks
        def on_message(ws, message):
            # Process messages in thread pool to prevent blocking
            self.thread_pool.submit(self._handle_message, message)
            
        def on_error(ws, error):
            self.logger.error(f"WebSocket error: {error}")
            
        def on_close(ws, close_status_code, close_msg):
            self.logger.info("WebSocket connection closed")
            self.running = False
            
        def on_open(ws):
            self.logger.info("WebSocket connection opened")
            self.running = True
            
            # Setup connection
            self._send_setup()
            
        # Create websocket connection with increased buffer sizes for high throughput
        websocket.enableTrace(False)  # Disable trace for production
        self.ws = websocket.WebSocketApp(
            self.dxlink_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # Start websocket in a separate thread
        self.ws_thread = threading.Thread(target=self.ws.run_forever, 
                                         kwargs={'ping_interval': 30, 
                                                'ping_timeout': 10})
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        # Wait for connection to establish
        timeout = 10
        start_time = time.time()
        while not self.running and time.time() - start_time < timeout:
            time.sleep(0.1)
            
        if not self.running:
            self.logger.error("Failed to establish connection within timeout")
            return False
            
        # Start keepalive thread
        self.keepalive_thread = threading.Thread(target=self._keepalive_loop)
        self.keepalive_thread.daemon = True
        self.keepalive_thread.start()
        
        # Start candle builder if enabled
        if self.build_candles and self.candle_builder:
            self.candle_builder.start()
            
            # Register candle callbacks
            if self.on_candle:
                self.candle_builder.register_callbacks(
                    on_completed=self.on_candle,
                    on_updated=self.on_candle
                )
        
        return True
        
    def disconnect(self):
        """Disconnect from DXLink websocket"""
        self.running = False
        if self.ws:
            self.ws.close()
            
        # Stop candle builder if enabled
        if self.build_candles and self.candle_builder:
            self.candle_builder.stop()
            
    def _send_setup(self):
        """Send SETUP message to initialize connection"""
        setup_msg = {
            "type": "SETUP",
            "channel": 0,
            "version": "0.1-DXF-JS/0.3.0",
            "keepaliveTimeout": 60,
            "acceptKeepaliveTimeout": 60
        }
        self._send_message(setup_msg)
        
    def _authorize(self):
        """Send AUTH message with token"""
        auth_msg = {
            "type": "AUTH",
            "channel": 0,
            "token": self.token
        }
        self._send_message(auth_msg)
        
    def _create_channel(self, service="FEED"):
        """
        Create a new channel for subscriptions
        
        Args:
            service (str): Service to use for the channel
            
        Returns:
            int: Channel ID
        """
        channel_id = self.channel_counter
        self.channel_counter += 1
        
        channel_msg = {
            "type": "CHANNEL_REQUEST",
            "channel": channel_id,
            "service": service,
            "parameters": {"contract": "AUTO"}
        }
        self._send_message(channel_msg)
        
        return channel_id
        
    def _setup_feed(self, channel_id):
        """
        Setup feed configuration for a channel
        
        Args:
            channel_id (int): Channel ID to setup
        """
        feed_setup_msg = {
            "type": "FEED_SETUP",
            "channel": channel_id,
            "acceptAggregationPeriod": 0.1,
            "acceptDataFormat": "COMPACT",
            "acceptEventFields": {
                "Trade": ["eventType", "eventSymbol", "price", "dayVolume", "size", "time", "exchangeCode", "dayId"],
                "TradeETH": ["eventType", "eventSymbol", "price", "dayVolume", "size", "time", "exchangeCode", "dayId"],
                "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize", "time", "bidExchangeCode", "askExchangeCode"],
                "Greeks": ["eventType", "eventSymbol", "volatility", "delta", "gamma", "theta", "rho", "vega"],
                "Profile": ["eventType", "eventSymbol", "description", "shortSaleRestriction", "tradingStatus"],
                "Summary": ["eventType", "eventSymbol", "openInterest", "dayOpenPrice", "dayHighPrice", "dayLowPrice", "prevDayClosePrice"],
                "Candle": ["eventType", "eventSymbol", "time", "sequence", "count", "open", "high", "low", "close", "volume", "vwap"]
            }
        }
        self._send_message(feed_setup_msg)
        

    def request_sector_updates(self):
        """
        Manually request updates for all sector ETFs to ensure continuous data
        """
        try:
            # Find the sector channel
            sector_channel_id = None
            for channel_id, channel_info in self.channels.items():
                if channel_info.get("is_sector", False):
                    sector_channel_id = channel_id
                    break
                    
            if not sector_channel_id:
                return  # No sector channel found
                
            # Request updates for all sectors
            sectors = ["XLK", "XLF", "XLV", "XLY"]
            requests = []
            
            for sector in sectors:
                requests.append({
                    "type": "Quote",
                    "symbol": sector
                })
                
            # Send request message
            if requests:
                request_msg = {
                    "type": "FEED_REQUEST",
                    "channel": sector_channel_id,
                    "requests": requests
                }
                self._send_message(request_msg)
                
        except Exception as e:
            self.logger.error(f"Error requesting sector updates: {e}")

    
    def subscribe_to_sector_etfs(self):
        """
        Subscribe to market data for sector ETFs with enhanced continuous updates
        
        Returns:
            int: Channel ID for the subscription
        """
        # Define sector ETFs to track
        sectors = ["XLK", "XLF", "XLV", "XLY"]
        
        # Reset pending updates set to include all sectors
        self.sector_updates_pending = set(sectors)
        
        # Log the subscription
        self.logger.info(f"Subscribing to market data for sectors: {', '.join(sectors)}")
        
        # Create a single channel for all sector ETFs
        channel_id = self._create_channel()
        
        # Wait for channel to be opened
        time.sleep(0.5)
        
        # Setup feed for the channel
        self._setup_feed(channel_id)
        
        # Wait for feed to be configured
        time.sleep(0.5)
        
        # Build subscription list for all sectors at once
        subscriptions = []
        for symbol in sectors:
            # Subscribe with high priority
            for event_type in ["Quote", "Trade", "Summary"]:
                subscriptions.append({
                    "type": event_type,
                    "symbol": symbol
                })
                
        # Send a single subscription request for all sectors
        subscription_msg = {
            "type": "FEED_SUBSCRIPTION",
            "channel": channel_id,
            "reset": True,
            "add": subscriptions
        }
        self._send_message(subscription_msg)
        
        # Store channel information
        self.channels[channel_id] = {
            "symbols": sectors,
            "event_types": ["Quote", "Trade", "Summary"],
            "is_sector": True
        }
        
        # Start a separate thread to periodically poll for sector data
        self._start_sector_polling(channel_id, sectors)
        
        self.logger.info(f"Subscribed to all sectors simultaneously on channel {channel_id}")
        return channel_id


    def _start_sector_polling(self, channel_id, sectors):
        """
        Start a background thread to poll for sector updates continuously
        
        Args:
            channel_id (int): Channel ID for sector data
            sectors (list): List of sector symbols
        """
        def poll_sectors():
            if not self.running:
                return
                
            try:
                # Request quotes for all sectors
                for sector in sectors:
                    quote_request = {
                        "type": "FEED_REQUEST",
                        "channel": channel_id,
                        "requests": [{
                            "type": "Quote",
                            "symbol": sector
                        }]
                    }
                    self._send_message(quote_request)
                    time.sleep(0.1)  # Small delay between requests
                    
                # Schedule next poll after 2 seconds
                if self.running:
                    threading.Timer(2.0, poll_sectors).start()
                    
            except Exception as e:
                self.logger.error(f"Error in sector polling: {e}")
                
        # Start polling thread
        threading.Thread(target=poll_sectors, daemon=True).start()



    def _schedule_sector_updates(self, channel_id):
        """
        Schedule periodic updates to ensure we're getting data for all sectors
        """
        def check_sectors():
            if not self.running:
                return
                
            sectors = ["XLK", "XLF", "XLV", "XLY"]
            for sector in sectors:
                # Request a quote update for this sector
                quote_msg = {
                    "type": "FEED_REQUEST",
                    "channel": channel_id,
                    "requests": [{
                        "type": "Quote",
                        "symbol": sector
                    }]
                }
                self._send_message(quote_msg)
                
            # Schedule next check
            threading.Timer(0.5, check_sectors).start()
        
        # Start the initial check
        check_sectors()

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
        Subscribe to market events for the given symbols
        
        Args:
            symbols (list): List of streamer symbols to subscribe to
            event_types (list): List of event types to subscribe to
                Default: ["Quote", "Trade", "Summary"]
            is_sector (bool): Whether these are sector ETFs
                
        Returns:
            int: Channel ID for the subscription
        """
        if not self.running:
            self.logger.error("WebSocket connection not established")
            return None
            
        if not event_types:
            event_types = ["Quote", "Trade", "Summary"]
            
        # Create a new channel
        channel_id = self._create_channel()
        
        # Wait for channel to be opened
        time.sleep(0.5)
        
        # Setup feed for the channel
        self._setup_feed(channel_id)
        
        # Wait for feed to be configured
        time.sleep(0.5)
        
        # Build subscription list
        subscriptions = []
        for symbol in symbols:
            for event_type in event_types:
                subscriptions.append({
                    "type": event_type,
                    "symbol": symbol
                })
                
        # Send subscription request
        subscription_msg = {
            "type": "FEED_SUBSCRIPTION",
            "channel": channel_id,
            "reset": True,
            "add": subscriptions
        }
        self._send_message(subscription_msg)
        
        # Store channel information
        self.channels[channel_id] = {
            "symbols": symbols,
            "event_types": event_types,
            "is_sector": is_sector
        }
        
        self.logger.info(f"Subscribed to {len(symbols)} symbols on channel {channel_id}")
        return channel_id
        
    def unsubscribe(self, channel_id):
        """
        Unsubscribe from a channel
        
        Args:
            channel_id (int): Channel ID to unsubscribe from
        """
        if not self.running:
            self.logger.error("WebSocket connection not established")
            return
            
        if channel_id not in self.channels:
            self.logger.error(f"Channel {channel_id} not found")
            return
            
        # Build unsubscription list
        symbols = self.channels[channel_id]["symbols"]
        event_types = self.channels[channel_id]["event_types"]
        
        unsubscriptions = []
        for symbol in symbols:
            for event_type in event_types:
                unsubscriptions.append({
                    "type": event_type,
                    "symbol": symbol
                })
                
        # Send unsubscription request
        unsubscription_msg = {
            "type": "FEED_SUBSCRIPTION",
            "channel": channel_id,
            "remove": unsubscriptions
        }
        self._send_message(unsubscription_msg)
        
        # Remove channel from storage
        del self.channels[channel_id]
        self.logger.info(f"Unsubscribed from channel {channel_id}")
        
    def subscribe_to_candles(self, symbol, period="1d", from_time=None, channel_id=None):
        """
        Subscribe to historical candle data
        
        Args:
            symbol (str): Symbol to fetch candles for (e.g. "SPY")
            period (str): Candle period (e.g. "5m", "1h", "1d")
            from_time (int): Start time as Unix timestamp (seconds since epoch)
            channel_id (int): Existing channel ID (creates new if None)
                
        Returns:
            int: Channel ID for the subscription
        """
        if not self.running:
            self.logger.error("WebSocket connection not established")
            return None
            
        # If no from_time provided, use 24 hours ago
        if not from_time:
            from_time = int(time.time()) - (24 * 60 * 60)
            
        # Format candle symbol
        candle_symbol = f"{symbol}{{={period}}}"
        
        # Use existing channel or create a new one
        if not channel_id:
            channel_id = self._create_channel()
            time.sleep(0.5)
            self._setup_feed(channel_id)
            time.sleep(0.5)
            
        # Send subscription request with fromTime
        subscription_msg = {
            "type": "FEED_SUBSCRIPTION",
            "channel": channel_id,
            "reset": True,
            "add": [{
                "type": "Candle",
                "symbol": candle_symbol,
                "fromTime": from_time
            }]
        }
        self._send_message(subscription_msg)
        
        # Store channel information
        if channel_id not in self.channels:
            self.channels[channel_id] = {
                "symbols": [candle_symbol],
                "event_types": ["Candle"]
            }
        else:
            self.channels[channel_id]["symbols"].append(candle_symbol)
            if "Candle" not in self.channels[channel_id]["event_types"]:
                self.channels[channel_id]["event_types"].append("Candle")
                
        self.logger.info(f"Subscribed to candles for {symbol} with period {period} on channel {channel_id}")
        return channel_id
        
    def _send_message(self, message):
        """
        Send a message to the websocket
        
        Args:
            message (dict): Message to send
        """
        if not self.ws:
            self.logger.error("WebSocket not initialized")
            return
            
        try:
            self.ws.send(json.dumps(message))
            self.logger.debug(f"Sent: {message}")
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            
    def _handle_message(self, message):
        """
        Handle incoming messages from the websocket
        
        Args:
            message (str): Message received from the websocket
        """
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            self.logger.debug(f"Received: {msg_type}")
            
            if msg_type == "AUTH_STATE":
                state = data.get("state")
                if state == "UNAUTHORIZED":
                    self._authorize()
                elif state == "AUTHORIZED":
                    self.logger.info("Successfully authorized")
                    
            elif msg_type == "FEED_DATA":
                self._handle_feed_data(data)
                
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            
    def _handle_feed_data(self, data):
        """
        Handle feed data messages
        
        Args:
            data (dict): Feed data message
        """
        channel = data.get("channel")
        feed_data = data.get("data", [])
        
        if not feed_data or len(feed_data) < 2:
            return
            
        event_type = feed_data[0]
        event_data = feed_data[1]
        
        # Get current timestamp (ISO format)
        timestamp = datetime.now().isoformat()
        
        if event_type == "Quote":
            # Print data structure for the first Quote
            if self.first_quote and len(event_data) >= 6:
                self.first_quote = False
                self.logger.debug("Quote data structure: " + json.dumps({
                    "eventType": event_data[0] if len(event_data) > 0 else None,
                    "symbol": event_data[1] if len(event_data) > 1 else None,
                    "bidPrice": event_data[2] if len(event_data) > 2 else None,
                    "askPrice": event_data[3] if len(event_data) > 3 else None,
                    "bidSize": event_data[4] if len(event_data) > 4 else None,
                    "askSize": event_data[5] if len(event_data) > 5 else None,
                    "time": event_data[6] if len(event_data) > 6 else None,
                }))
                
            # Parse quote data and call callback
            if len(event_data) >= 6:
                # Format: ["Quote", symbol, bidPrice, askPrice, bidSize, askSize, time]
                symbol = event_data[1]
                
                # Ensure all price values are floats, not strings
                try:
                    bid_price = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
                    ask_price = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
                    bid_size = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
                    ask_size = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error converting quote values for {symbol}: {e}")
                    bid_price = 0.0
                    ask_price = 0.0
                    bid_size = 0.0
                    ask_size = 0.0
                
                quote = {
                    "symbol": symbol,
                    "bid": bid_price,
                    "ask": ask_price,
                    "bid_size": bid_size,
                    "ask_size": ask_size,
                    "timestamp": timestamp
                }
                
                # Add exchange time if available
                if len(event_data) > 6:
                    quote["exchange_time"] = event_data[6]
                    
                # Save to database
                self._save_quote_to_db(quote)
                
                # Check if this is a sector ETF and update if so
                channel_info = self.channels.get(channel, {})
                if channel_info.get("is_sector", False) and symbol in ["XLK", "XLF", "XLV", "XLY"]:
                    # Calculate mid price
                    if bid_price > 0 and ask_price > 0:
                        price = (bid_price + ask_price) / 2
                    elif bid_price > 0:
                        price = bid_price
                    elif ask_price > 0:
                        price = ask_price
                    else:
                        price = 0.0
                        
                    # Only process if we have a valid price
                    if price > 0:
                        # Store price
                        self.sector_prices[symbol] = price
                        
                        # Calculate price change
                        prev_price = getattr(self, '_prev_sector_prices', {}).get(symbol, price)
                        if not hasattr(self, '_prev_sector_prices'):
                            self._prev_sector_prices = {}
                            
                        change_pct = ((price - prev_price) / prev_price) * 100 if prev_price > 0 else 0
                        
                        # Determine status based on price movement
                        status = "neutral"
                        if abs(change_pct) > 0.05:  # 0.05% threshold
                            status = "bullish" if change_pct > 0 else "bearish"
                        
                        self._prev_sector_prices[symbol] = price
                        
                        # Call sector update callback immediately
                        if self.on_sector_update:
                            self.on_sector_update(symbol, status, price)
                    
                    # Check if we've received updates for all sectors
                    # If all sectors updated or 2 seconds passed since first sector update
                    all_updated = not self.sector_updates_pending
                    time_for_all_updates = hasattr(self, '_sector_update_start_time') and \
                        (time.time() - self._sector_update_start_time) > 2.0
                        
                    if (all_updated or time_for_all_updates) and self.on_sector_update:
                        # Call with a special signal that all sectors are updated
                        self.on_sector_update("ALL_SECTORS_UPDATED", "", 0)
                        # Reset for next batch of updates
                        self.sector_updates_pending = set(["XLK", "XLF", "XLV", "XLY"])
                        if hasattr(self, '_sector_update_start_time'):
                            delattr(self, '_sector_update_start_time')
                    elif not hasattr(self, '_sector_update_start_time') and self.sector_updates_pending:
                        # Start the timer for sector updates
                        self._sector_update_start_time = time.time()
                

                # Check if this is a Mag7 stock and we have a callback
                mag7_stocks = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"]
                if symbol in mag7_stocks and hasattr(self, 'on_mag7_update') and self.on_mag7_update:
                    # Calculate mid price
                    if bid_price > 0 and ask_price > 0:
                        price = (bid_price + ask_price) / 2
                    elif bid_price > 0:
                        price = bid_price
                    elif ask_price > 0:
                        price = ask_price
                    else:
                        price = 0.0
                    
                    if price > 0:
                        self.on_mag7_update(symbol, price)


                # Call user callback
                if self.on_quote:
                    self.on_quote(quote)
                    
        elif event_type == "Trade":
            # Print data structure for the first Trade
            if self.first_trade and len(event_data) >= 5:
                self.first_trade = False
                self.logger.debug("Trade data structure: " + json.dumps({
                    "eventType": event_data[0] if len(event_data) > 0 else None,
                    "symbol": event_data[1] if len(event_data) > 1 else None,
                    "price": event_data[2] if len(event_data) > 2 else None,
                    "dayVolume": event_data[3] if len(event_data) > 3 else None,
                    "size": event_data[4] if len(event_data) > 4 else None,
                    "time": event_data[5] if len(event_data) > 5 else None,
                }))
                
            # Parse trade data and call callback
            if len(event_data) >= 5:
                # Format: ["Trade", symbol, price, dayVolume, size, time]
                try:
                    price = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
                    volume = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
                    size = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error converting trade values: {e}")
                    price = 0.0
                    volume = 0.0
                    size = 0.0
                
                trade = {
                    "symbol": event_data[1],
                    "price": price,
                    "volume": volume,
                    "size": size,
                    "timestamp": timestamp
                }
                
                # Add exchange time if available
                if len(event_data) > 5:
                    trade["exchange_time"] = event_data[5]
                    
                # Save to database
                self._save_trade_to_db(trade)
                
                # Call user callback
                if self.on_trade:
                    self.on_trade(trade)
                
                # Process trade for candle building
                if self.build_candles and self.candle_builder:
                    self.candle_builder.process_trade(trade)
                    
        elif event_type == "Greeks":
            # Print data structure for the first Greek
            if self.first_greek and len(event_data) >= 7:
                self.first_greek = False
                self.logger.debug("Greek data structure: " + json.dumps({
                    "eventType": event_data[0] if len(event_data) > 0 else None,
                    "symbol": event_data[1] if len(event_data) > 1 else None,
                    "volatility": event_data[2] if len(event_data) > 2 else None,
                    "delta": event_data[3] if len(event_data) > 3 else None,
                    "gamma": event_data[4] if len(event_data) > 4 else None,
                    "theta": event_data[5] if len(event_data) > 5 else None,
                    "rho": event_data[6] if len(event_data) > 6 else None,
                    "vega": event_data[7] if len(event_data) > 7 else None
                }))
                
            # Parse greek data and call callback
            if len(event_data) >= 7:
                # Format: ["Greeks", symbol, volatility, delta, gamma, theta, rho, vega]
                try:
                    volatility = float(event_data[2]) if event_data[2] and event_data[2] != "NaN" else 0.0
                    delta = float(event_data[3]) if event_data[3] and event_data[3] != "NaN" else 0.0
                    gamma = float(event_data[4]) if event_data[4] and event_data[4] != "NaN" else 0.0
                    theta = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
                    rho = float(event_data[6]) if event_data[6] and event_data[6] != "NaN" else 0.0
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error converting greek values: {e}")
                    volatility = 0.0
                    delta = 0.0
                    gamma = 0.0
                    theta = 0.0
                    rho = 0.0
                
                greek = {
                    "symbol": event_data[1],
                    "volatility": volatility,
                    "delta": delta,
                    "gamma": gamma,
                    "theta": theta,
                    "rho": rho,
                    "timestamp": timestamp
                }
                
                # Add vega if available
                if len(event_data) > 7:
                    try:
                        vega = float(event_data[7]) if event_data[7] and event_data[7] != "NaN" else 0.0
                        greek["vega"] = vega
                    except (ValueError, TypeError):
                        greek["vega"] = 0.0
                    
                # Save to database
                self._save_greek_to_db(greek)
                
                # Call user callback
                if self.on_greek:
                    self.on_greek(greek)
                    
        elif event_type == "Candle":
            # Print data structure for the first Candle
            if self.first_candle and len(event_data) >= 10:
                self.first_candle = False
                self.logger.debug("Candle data structure: " + json.dumps({
                    "eventType": event_data[0] if len(event_data) > 0 else None,
                    "symbol": event_data[1] if len(event_data) > 1 else None,
                    "time": event_data[2] if len(event_data) > 2 else None,
                    "sequence": event_data[3] if len(event_data) > 3 else None,
                    "count": event_data[4] if len(event_data) > 4 else None,
                    "open": event_data[5] if len(event_data) > 5 else None,
                    "high": event_data[6] if len(event_data) > 6 else None,
                    "low": event_data[7] if len(event_data) > 7 else None,
                    "close": event_data[8] if len(event_data) > 8 else None,
                    "volume": event_data[9] if len(event_data) > 9 else None,
                    "vwap": event_data[10] if len(event_data) > 10 else None
                }))
                
            # Parse candle data
            if len(event_data) >= 10:
                # Extract period from symbol
                symbol = event_data[1]
                period = "unknown"
                if "{=" in symbol:
                    parts = symbol.split("{=")
                    symbol = parts[0]
                    period = parts[1].rstrip("}")
                    
                # Format: ["Candle", symbol, time, sequence, count, open, high, low, close, volume, vwap]
                try:
                    time_value = event_data[2]
                    open_price = float(event_data[5]) if event_data[5] and event_data[5] != "NaN" else 0.0
                    high_price = float(event_data[6]) if event_data[6] and event_data[6] != "NaN" else 0.0
                    low_price = float(event_data[7]) if event_data[7] and event_data[7] != "NaN" else 0.0
                    close_price = float(event_data[8]) if event_data[8] and event_data[8] != "NaN" else 0.0
                    volume = float(event_data[9]) if event_data[9] and event_data[9] != "NaN" else 0.0
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error converting candle values: {e}")
                    open_price = 0.0
                    high_price = 0.0
                    low_price = 0.0
                    close_price = 0.0
                    volume = 0.0
                
                candle = {
                    "symbol": symbol,
                    "period": period,
                    "time": time_value,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "timestamp": timestamp
                }
                
                # Call user callback
                if self.on_candle:
                    self.on_candle(candle)
                    
    def _keepalive_loop(self):
        """Send keepalive messages periodically"""
        while self.running:
            try:
                # Send keepalive every 30 seconds
                time.sleep(30)
                if self.running:
                    keepalive_msg = {
                        "type": "KEEPALIVE",
                        "channel": 0
                    }
                    self._send_message(keepalive_msg)
            except Exception as e:
                self.logger.error(f"Error in keepalive loop: {e}")
                
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
        """
        Get quotes from the database
        
        Args:
            symbol (str): Symbol to get quotes for
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            limit (int): Maximum number of quotes to return
            
        Returns:
            list: List of quotes from the database
        """
        if not self.save_to_db or not self.db:
            return []
            
        try:
            # Build query
            query = {"symbol": symbol}
            
            if start_time:
                if 'timestamp' not in query:
                    query['timestamp'] = {}
                query['timestamp']['$gte'] = start_time if isinstance(start_time, str) else start_time.isoformat()
                
            if end_time:
                if 'timestamp' not in query:
                    query['timestamp'] = {}
                query['timestamp']['$lte'] = end_time if isinstance(end_time, str) else end_time.isoformat()
                
            # Query database
            quotes = self.db.find_many(COLLECTIONS['QUOTES'], query, limit=limit)
            
            # Sort by timestamp
            return sorted(quotes, key=lambda x: x['timestamp'])
        except Exception as e:
            self.logger.error(f"Error getting quotes from database: {e}")
            return []


    
    def _cleanup_old_data(self):
        """
        Clean up old data to free memory
        """
        try:
            # Check if we need to clean up
            if not hasattr(self, '_last_cleanup_time'):
                self._last_cleanup_time = time.time()
                return
                
            # Only clean up periodically (e.g., every 10 minutes)
            current_time = time.time()
            if current_time - self._last_cleanup_time < 600:  # 600 seconds = 10 minutes
                return
                
            self.logger.info("Cleaning up old data to free memory")
            
            # Clean up quotes data
            if hasattr(self, 'candle_data'):
                # Only keep the last 1000 candles per key
                for key in list(self.candle_data.keys()):
                    if len(self.candle_data[key]) > 1000:
                        self.candle_data[key] = self.candle_data[key][-1000:]
            
            # Update last cleanup time
            self._last_cleanup_time = current_time
            
        except Exception as e:
            self.logger.error(f"Error during data cleanup: {e}")