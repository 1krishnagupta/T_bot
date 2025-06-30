# File: Code/bot_core/candle_builder.py

import time
import threading
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Callable, Tuple, Any

from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class CandleBuilder:
    """
    Builds and manages OHLC candles of different time intervals from tick data
    Stores data in MongoDB
    """
    
    def __init__(self, periods=(1, 2, 3, 5, 15), save_to_db=True):
        """
        Initialize the candle builder
        
        Args:
            periods (tuple): Candle periods in minutes to build
            save_to_db (bool): Whether to save data to database
        """
        self.periods = periods  # Candle periods in minutes
        
        # Store the active candles for each period and symbol
        self.current_candles = {}
        
        # Store the last price for each symbol
        self.last_prices = {}
        
        # Store completed candles for reference
        self.completed_candles = defaultdict(list)
        self.max_completed_candles = 100  # Max number of completed candles to keep in memory
        
        # Database configuration
        self.save_to_db = save_to_db
        self.db = get_mongodb_handler() if save_to_db else None
        
        # Create candles collection if it doesn't exist
        if self.save_to_db:
            self.db.create_collection(COLLECTIONS['CANDLES'])
            # Create indexes for faster queries
            self.db.create_index(COLLECTIONS['CANDLES'], [("symbol", 1), ("period", 1), ("start_time", 1)])
        
        # Logger
        self.logger = logging.getLogger("CandleBuilder")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            today = datetime.now().strftime("%Y-%m-%d")
            log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
            os.makedirs(log_folder, exist_ok=True)
            log_file = os.path.join(log_folder, f"candle_builder_{today}.log")
            
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
        # Initialize candle processing thread
        self.running = False
        self.processing_thread = None
        
        # Callbacks for candle events
        self.on_candle_completed = None
        self.on_candle_updated = None
        
    def start(self):
        """Start the candle builder"""
        if self.running:
            return
            
        self.running = True
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self._processing_loop)
        self.processing_thread.daemon = True
        self.processing_thread.start()
        
        self.logger.info(f"Started candle builder for periods: {self.periods} minutes")
        return True
        
    def stop(self):
        """Stop the candle builder"""
        self.running = False
        
        # Wait for processing thread to finish
        if self.processing_thread:
            self.processing_thread.join(timeout=2.0)
            
        self.logger.info("Stopped candle builder")
        
    def register_callbacks(self, on_completed=None, on_updated=None):
        """
        Register callbacks for candle events
        
        Args:
            on_completed (callable): Called when a candle is completed
            on_updated (callable): Called when a candle is updated
        """
        self.on_candle_completed = on_completed
        self.on_candle_updated = on_updated
        
    def process_quote(self, quote):
        """
        Process a quote update
        
        Args:
            quote (dict): Quote data with symbol, bid, ask, etc.
        """
        symbol = quote.get('symbol')
        if not symbol:
            return
            
        # Use mid price for candle updates
        bid = quote.get('bid', 0)
        ask = quote.get('ask', 0)
        
        if bid > 0 and ask > 0:
            mid_price = (bid + ask) / 2
        elif bid > 0:
            mid_price = bid
        elif ask > 0:
            mid_price = ask
        else:
            return  # Invalid quote
            
        # Update last price
        self.last_prices[symbol] = mid_price
        
        # Update candles
        self._update_candles(symbol, mid_price, quote.get('timestamp'))
        
    def process_trade(self, trade):
        """
        Process a trade update
        
        Args:
            trade (dict): Trade data with symbol, price, size, etc.
        """
        symbol = trade.get('symbol')
        price = trade.get('price')
        size = trade.get('size', 0)
        
        if not symbol or not price:
            return
            
        # Update last price
        self.last_prices[symbol] = price
        
        # Update candles
        self._update_candles(symbol, price, trade.get('timestamp'), size)
        
    def _update_candles(self, symbol, price, timestamp=None, volume=0):
        """
        Update all candles for a symbol with a new price
        
        Args:
            symbol (str): Instrument symbol
            price (float): Current price
            timestamp (str): ISO timestamp (uses current time if None)
            volume (float): Volume of the update (for trades)
        """
        if not timestamp:
            timestamp = datetime.now().isoformat()
            
        # Parse timestamp to datetime
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp)
            except ValueError:
                dt = datetime.now()
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            dt = datetime.now()
            
        # Update each time period
        for period in self.periods:
            # Get candle key
            candle_key = (symbol, period)
            
            # Get or create candle
            if candle_key not in self.current_candles:
                # Calculate candle start time (floor to the nearest period)
                minutes = dt.minute
                candle_minute = (minutes // period) * period
                candle_start = dt.replace(minute=candle_minute, second=0, microsecond=0)
                
                # Create new candle
                self.current_candles[candle_key] = {
                    'symbol': symbol,
                    'period': f"{period}m",
                    'start_time': candle_start.isoformat(),
                    'end_time': (candle_start + timedelta(minutes=period)).isoformat(),
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'tick_count': 1,
                    'last_update': dt.isoformat()
                }
            else:
                # Update existing candle
                candle = self.current_candles[candle_key]
                
                # Check if we need to complete this candle
                candle_end = datetime.fromisoformat(candle['end_time'])
                if dt >= candle_end:
                    # Complete current candle
                    completed_candle = candle.copy()
                    
                    # Notify completion
                    if self.on_candle_completed:
                        self.on_candle_completed(completed_candle)
                        
                    # Save to database
                    if self.save_to_db:
                        self._save_candle_to_db(completed_candle)
                        
                    # Add to completed candles history
                    self.completed_candles[candle_key].append(completed_candle)
                    
                    # Limit completed candles history
                    if len(self.completed_candles[candle_key]) > self.max_completed_candles:
                        self.completed_candles[candle_key].pop(0)
                        
                    # Calculate new candle start time (floor to the nearest period)
                    minutes = dt.minute
                    candle_minute = (minutes // period) * period
                    candle_start = dt.replace(minute=candle_minute, second=0, microsecond=0)
                    
                    # Create new candle
                    self.current_candles[candle_key] = {
                        'symbol': symbol,
                        'period': f"{period}m",
                        'start_time': candle_start.isoformat(),
                        'end_time': (candle_start + timedelta(minutes=period)).isoformat(),
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': volume,
                        'tick_count': 1,
                        'last_update': dt.isoformat()
                    }
                else:
                    # Update high/low
                    candle['high'] = max(candle['high'], price)
                    candle['low'] = min(candle['low'], price)
                    candle['close'] = price
                    candle['volume'] += volume
                    candle['tick_count'] += 1
                    candle['last_update'] = dt.isoformat()
                    
                    # Notify update
                    if self.on_candle_updated:
                        self.on_candle_updated(candle)
                        
    def _processing_loop(self):
        """Background thread to check for candle completions"""
        while self.running:
            try:
                # Current time
                now = datetime.now()
                
                # Check all active candles
                for candle_key, candle in list(self.current_candles.items()):
                    # Check if candle is complete
                    candle_end = datetime.fromisoformat(candle['end_time'])
                    if now >= candle_end:
                        # Complete this candle
                        completed_candle = candle.copy()
                        
                        # Notify completion
                        if self.on_candle_completed:
                            self.on_candle_completed(completed_candle)
                            
                        # Save to database
                        if self.save_to_db:
                            self._save_candle_to_db(completed_candle)
                            
                        # Add to completed candles history
                        self.completed_candles[candle_key].append(completed_candle)
                        
                        # Limit completed candles history
                        if len(self.completed_candles[candle_key]) > self.max_completed_candles:
                            self.completed_candles[candle_key].pop(0)
                            
                        # Remove from active candles
                        # Don't create a new one here, it will be created on next tick
                        del self.current_candles[candle_key]
                
                # Sleep for a short time
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error in candle processing loop: {e}")
                time.sleep(1.0)
                
    def _save_candle_to_db(self, candle):
        """
        Save a completed candle to the database
        
        Args:
            candle (dict): Candle data to save
        """
        if not self.save_to_db or not self.db:
            return
            
        try:
            # Save to candles collection
            self.db.insert_one(COLLECTIONS['CANDLES'], candle)
            
            self.logger.debug(f"Saved {candle['period']} candle for {candle['symbol']} to database")
        except Exception as e:
            self.logger.error(f"Error saving candle to database: {e}")
            
    def get_current_candle(self, symbol, period):
        """
        Get the current active candle for a symbol and period
        
        Args:
            symbol (str): Instrument symbol
            period (int): Candle period in minutes
            
        Returns:
            dict: Current candle or None if not found
        """
        candle_key = (symbol, period)
        return self.current_candles.get(candle_key)
        
    def get_candle_history(self, symbol, period, count=10):
        """
        Get historical candles for a symbol and period
        
        Args:
            symbol (str): Instrument symbol
            period (int): Candle period in minutes
            count (int): Maximum number of candles to return
            
        Returns:
            list: List of historical candles (most recent first)
        """
        candle_key = (symbol, period)
        candles = self.completed_candles.get(candle_key, [])
        return list(reversed(candles[-count:]))
    
    def get_candles_from_db(self, symbol, period, start_time=None, end_time=None, limit=100):
        """
        Get historical candles from the database
        
        Args:
            symbol (str): Instrument symbol
            period (str): Candle period (e.g., "1m", "5m")
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            limit (int): Maximum number of candles to return
            
        Returns:
            list: List of candles from the database
        """
        if not self.save_to_db or not self.db:
            return []
            
        try:
            # Build query
            query = {
                "symbol": symbol,
                "period": period if isinstance(period, str) else f"{period}m"
            }
            
            if start_time:
                if 'start_time' not in query:
                    query['start_time'] = {}
                query['start_time']['$gte'] = start_time if isinstance(start_time, str) else start_time.isoformat()
                
            if end_time:
                if 'start_time' not in query:
                    query['start_time'] = {}
                query['start_time']['$lte'] = end_time if isinstance(end_time, str) else end_time.isoformat()
                
            # Query database
            candles = self.db.find_many(COLLECTIONS['CANDLES'], query, limit=limit)
            
            # Sort by start_time
            return sorted(candles, key=lambda x: x['start_time'])
        except Exception as e:
            self.logger.error(f"Error getting candles from database: {e}")
            return []