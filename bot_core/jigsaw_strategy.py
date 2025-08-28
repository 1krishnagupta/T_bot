# Code/bot_core/jigsaw_strategy.py

import time
import logging
import os
import json
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from Code.bot_core.mag7_strategy import Mag7Strategy
from Code.bot_core.position_manager import PositionManager

class JigsawStrategy:
    """
    Implementation of the Jigsaw Flow trading strategy
    """
    
    def __init__(self, instrument_fetcher, market_data_client, order_manager, config=None):
        """
        Initialize the strategy
        
        Args:
            instrument_fetcher: InstrumentFetcher instance
            market_data_client: MarketDataClient instance
            order_manager: OrderManager instance
            config (dict): Strategy configuration
        """
        self.instrument_fetcher = instrument_fetcher
        self.market_data = market_data_client
        self.order_manager = order_manager
        
        # Setup logging FIRST before any other operations
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"jigsaw_strategy_{today}.log")
        
        self.logger = logging.getLogger("JigsawStrategy")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Load configuration
        self.config = config or {}
        self.trading_config = self.config.get("trading_config", {})
        
        # Strategy state variables
        self.active_trades = {}
        self.position_manager = PositionManager()
        self._sync_positions_from_manager()
        
        # Get sector configuration from config
        self.sector_etfs = self.trading_config.get("sector_etfs", ["XLK", "XLF", "XLV", "XLY"])
        self.sector_weights = self.trading_config.get("sector_weights", {
            "XLK": 32,
            "XLF": 14,
            "XLV": 11,
            "XLY": 11
        })
        
        # Initialize sector status for configured sectors
        self.sector_status = {sector: "neutral" for sector in self.sector_etfs}
        self.sector_prices = {}
        
        # Price data for analysis
        self.price_data = {}  # OHLCV data by symbol and timeframe
        self.indicators = {}  # Technical indicators by symbol
        
        # Market times
        self.market_open_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        self.market_close_time = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0)
        
        # Initialize Mag7 strategy if configured
        self.mag7_strategy = None
        if self.trading_config.get("use_mag7_confirmation", False):
            self.mag7_strategy = Mag7Strategy(market_data_client, config)
                    
        # Strategy initialized flag
        self.initialized = False
        
        
    
    def initialize(self):
        """Initialize the strategy and subscribe to market data"""
        if self.initialized:
            return
            
        try:
            # Get watchlist tickers from config input by user through UI
            self.tickers = self.trading_config.get("tickers", ["SPY", "QQQ", "AAPL", "MSFT", "TSLA"])
            if not isinstance(self.tickers, list):
                self.tickers = [t.strip() for t in self.tickers.split(',')]
            
            # Check which strategy is being used
            use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
            
            if use_mag7:
                # Initialize Mag7 strategy if enabled
                if self.mag7_strategy:
                    self.mag7_strategy.initialize()
                    self.logger.info("Initialized Mag7 strategy")
                
                # Get Mag7 stocks from config
                mag7_stocks = self.trading_config.get("mag7_stocks", 
                    ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
                
                # Subscribe using the dedicated method
                if hasattr(self.market_data, 'subscribe_to_mag7_stocks'):
                    # Subscribe using the dedicated method
                    channel_id = self.market_data.subscribe_to_mag7_stocks(mag7_stocks)
                    self.logger.info(f"Subscribed to Mag7 stocks using dedicated method, channel: {channel_id}")
                    
                    # IMPORTANT: Set the callback for Mag7 updates
                    self.market_data.on_mag7_update = self.update_mag7_status
                else:
                    # Fallback: Subscribe manually if method doesn't exist
                    self.logger.warning("subscribe_to_mag7_stocks method not found, subscribing manually")
                    for stock in mag7_stocks:
                        streamer_symbol = self.instrument_fetcher.get_streamer_symbol(stock)
                        self.market_data.subscribe(
                            [streamer_symbol],
                            event_types=["Quote", "Trade", "Summary"]
                        )
                
            else:
                # Initialize sector status tracking for real market data
                self.sector_status = {
                    "XLK": "neutral",
                    "XLF": "neutral",
                    "XLV": "neutral",
                    "XLY": "neutral"
                }
                self.sector_prices = {}
                
                # Subscribe to real sector ETF data
                self.market_data.subscribe_to_sector_etfs()
                self.logger.info("Subscribed to sector ETFs for sector alignment strategy")
                
                # IMPORTANT: Set the callback for sector updates
                if hasattr(self.market_data, 'on_sector_update'):
                    self.market_data.on_sector_update = self.update_sector_status
            
            # Subscribe to real market data for watchlist tickers from config
            for ticker in self.tickers:
                streamer_symbol = self.instrument_fetcher.get_streamer_symbol(ticker)
                self.market_data.subscribe(
                    [streamer_symbol],
                    event_types=["Quote", "Trade", "Summary"]
                )
            
            self.initialized = True
            strategy_type = "Mag7" if use_mag7 else "Sector Alignment"
            self.logger.info(f"Jigsaw strategy initialized successfully with {strategy_type} confirmation")
            
        except Exception as e:
            self.logger.error(f"Error initializing strategy: {e}")



    
    def update_sector_status(self, sector, status, price):
        """
        Update sector status with real-time market data
        
        Args:
            sector (str): Sector symbol (e.g., "XLK")
            status (str): Status ("bullish", "bearish", "neutral")
            price (float): Current price
        """
        # Only process sector updates if NOT using Mag7 strategy
        if self.trading_config.get("use_mag7_confirmation", False):
            return  # Ignore sector updates when using Mag7
        
        # Store the sector status and price from real market data
        self.sector_status[sector] = status
        self.sector_prices[sector] = price
        
        # Log the update
        self.logger.info(f"Sector {sector} status updated to {status} at price {price:.2f}")
        
        # Check for potential trade setups after sector update
        self.check_for_trade_setups()

    
    def update_mag7_status(self, symbol, price):
        """
        Update Mag7 stock status if Mag7 strategy is enabled
        
        Args:
            symbol (str): Stock symbol
            price (float): Current price
        """
        # Only process Mag7 updates if using Mag7 strategy
        if not self.trading_config.get("use_mag7_confirmation", False):
            return  # Ignore Mag7 updates when using sector alignment
            
        if self.mag7_strategy and symbol in self.mag7_strategy.mag7_stocks:
            self.mag7_strategy.update_mag7_status(symbol, price)
            
            # Check for trade setups after Mag7 update
            self.check_for_trade_setups()


    def check_for_trade_setups(self):
        """Check for potential trade setups based on real market conditions"""
        # Check which strategy is active
        use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
        
        if use_mag7:
            # Check Mag7 alignment
            if self.mag7_strategy:
                aligned, direction, percentage = self.mag7_strategy.check_mag7_alignment()
                if not aligned:
                    return
                
                self.logger.info(f"Mag7 alignment detected: {direction} with {percentage:.1f}% alignment")
        else:
            # Check sector alignment
            sector_aligned, direction, weight = self.detect_sector_alignment()
            if not sector_aligned:
                return
                
            self.logger.info(f"Sector alignment detected: {direction} with {weight}% weight")
        
        # We have alignment, now check for compression in our watchlist
        for ticker in self.tickers:
            compression_detected, comp_direction = self.detect_compression(ticker)
            
            # Make sure compression direction matches alignment direction
            if compression_detected and comp_direction == direction:
                # We have a potential trade setup!
                strategy_name = "Mag7" if use_mag7 else "Sector"
                self.logger.info(f"TRADE SIGNAL: {comp_direction.upper()} compression breakout on {ticker} confirmed with {strategy_name} alignment")
                
                # If we're not already in a trade for this ticker, enter it
                if ticker not in self.active_trades:
                    self.enter_trade(ticker, comp_direction)


    def _calculate_market_condition(self):
        """Calculate overall market condition based on sector statuses and weights"""
        bullish_weight = 0
        bearish_weight = 0
        neutral_weight = 0
        
        for sector, status in self.sector_status.items():
            weight = self.sector_weights.get(sector, 0)
            
            if status == "bullish":
                bullish_weight += weight
            elif status == "bearish":
                bearish_weight += weight
            else:
                neutral_weight += weight
                
        # Determine market condition based on dominant weight
        if bullish_weight > 43:  # As per spec, 43% is the threshold
            self.market_condition = "bullish"
        elif bearish_weight > 43:
            self.market_condition = "bearish"
        else:
            self.market_condition = "neutral"
            
        # Log the updated market condition
        self.logger.info(f"Market condition updated: Bullish {bullish_weight}%, Bearish {bearish_weight}%, Neutral {neutral_weight}%")
        self.logger.info(f"Overall market condition: {self.market_condition.upper()}")
        
        # After market condition update, check for trading opportunities
        self.scan_for_trades()
    
    def update_compression_status(self, detected, direction=None):
        """
        Update compression detection status
        
        Args:
            detected (bool): Whether compression is detected
            direction (str): Direction of compression ("bullish", "bearish", or None)
        """
        self.compression_detected = detected
        if direction:
            self.compression_direction = direction
            
        # Log the update
        if detected:
            self.logger.info(f"Compression detected with direction: {direction}")
        else:
            self.logger.info("No compression detected")
    
    
    def _calculate_bollinger_band_width(self, data, window=20, num_std=2):
        """
        Calculate Bollinger Band width
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            window (int): Window for moving average
            num_std (int): Number of standard deviations
            
        Returns:
            float: Bollinger Band width as percentage of middle band
        """
        # Calculate middle band (simple moving average)
        middle_band = data['close'].rolling(window=window).mean()
        
        # Calculate standard deviation
        std = data['close'].rolling(window=window).std()
        
        # Calculate upper and lower bands
        upper_band = middle_band + (std * num_std)
        lower_band = middle_band - (std * num_std)
        
        # Calculate width as percentage of middle band
        width = (upper_band - lower_band) / middle_band
        
        # Return the most recent width value
        return width.iloc[-1]
    
    def _calculate_vwap(self, data):
        """
        Calculate Volume Weighted Average Price (VWAP)
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            
        Returns:
            float: VWAP
        """
        # Calculate typical price
        typical_price = (data['high'] + data['low'] + data['close']) / 3
        
        # Calculate VWAP
        vwap = (typical_price * data['volume']).cumsum() / data['volume'].cumsum()
        
        # Return the most recent VWAP value
        return vwap.iloc[-1]
    
    def get_price_data(self, symbol, timeframe="5m", bars=50):
        """
        Get price data for a symbol
        
        Args:
            symbol (str): Symbol to get data for
            timeframe (str): Timeframe to use
            bars (int): Number of bars to get
            
        Returns:
            DataFrame: Price data with OHLCV columns or None if data not available
        """
        key = f"{symbol}_{timeframe}"
        
        # Check if we already have cached data
        if key in self.price_data:
            return self.price_data[key]
            
        # Try to get data from database
        try:
            # Convert timeframe string to period
            if timeframe.endswith('m'):
                period = int(timeframe[:-1])
            elif timeframe.endswith('h'):
                period = int(timeframe[:-1]) * 60
            else:
                period = 5  # Default to 5 minutes
                
            # Get candles from database
            candles = self.market_data.candle_builder.get_candles_from_db(
                symbol=symbol,
                period=period,
                limit=bars
            )
            
            if not candles or len(candles) == 0:
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            
            # Ensure required columns exist
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    return None
                    
            # Cache the data
            self.price_data[key] = df
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting price data for {symbol} {timeframe}: {e}")
            return None
    
    def calculate_stochastic(self, data, k_period=5, d_period=3, smooth=2):
        """
        Calculate Stochastic Oscillator (Barry Burns' method)
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            k_period (int): K period
            d_period (int): D period
            smooth (int): Smoothing factor
            
        Returns:
            tuple: (K, D)
        """
        # Calculate highest high and lowest low over look back period
        high_high = data['high'].rolling(window=k_period).max()
        low_low = data['low'].rolling(window=k_period).min()
        
        # Calculate raw K
        raw_k = 100 * (data['close'] - low_low) / (high_high - low_low)
        
        # Calculate smoothed K
        k = raw_k.rolling(window=smooth).mean()
        
        # Calculate D
        d = k.rolling(window=d_period).mean()
        
        # Return most recent values
        return k.iloc[-1], d.iloc[-1]
    
    def check_heiken_ashi_signal(self, data, direction):
        """
        Check for a Heiken Ashi signal in the given direction
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            direction (str): "bullish" or "bearish"
            
        Returns:
            bool: True if signal is present, False otherwise
        """
        # Calculate Heiken Ashi candles
        ha = self._calculate_heiken_ashi(data)
        
        # Get last candle
        last_candle = ha.iloc[-1]
        
        # Get wick tolerance from config
        wick_tolerance_pct = self.trading_config.get("ha_wick_tolerance", 0.1)
        candle_range = last_candle['high'] - last_candle['low']
        wick_tolerance = candle_range * wick_tolerance_pct if candle_range > 0 else 0.0001
        
        if direction == "bullish":
            # Check for small or no lower wick (strong bullish candle)
            return (abs(last_candle['open'] - last_candle['low']) < wick_tolerance and 
                    last_candle['close'] > last_candle['open'])
        elif direction == "bearish":
            # Check for small or no upper wick (strong bearish candle)
            return (abs(last_candle['open'] - last_candle['high']) < wick_tolerance and 
                    last_candle['close'] < last_candle['open'])
                    
        return False
    
    def _calculate_heiken_ashi(self, data):
        """
        Calculate Heiken Ashi candles
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            
        Returns:
            DataFrame: Heiken Ashi candles
        """
        ha = pd.DataFrame(index=data.index)
        
        # Calculate Heiken Ashi values
        ha['open'] = ((data['open'].shift(1) + data['close'].shift(1)) / 2).fillna(data['open'])
        ha['close'] = (data['open'] + data['high'] + data['low'] + data['close']) / 4
        ha['high'] = data[['high', 'open', 'close']].max(axis=1)
        ha['low'] = data[['low', 'open', 'close']].min(axis=1)
        
        return ha
    
    def check_ema_alignment(self, data, direction, ema_period=15):
        """
        Check if price is aligned with EMA in the given direction
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            direction (str): "bullish" or "bearish"
            ema_period (int): EMA period
            
        Returns:
            bool: True if price is aligned with EMA, False otherwise
        """
        # Calculate EMA
        ema = data['close'].ewm(span=ema_period, adjust=False).mean()
        
        # Get last values
        last_close = data['close'].iloc[-1]
        last_ema = ema.iloc[-1]
        
        # Check alignment
        if direction == "bullish":
            return last_close > last_ema
        elif direction == "bearish":
            return last_close < last_ema
            
        return False
    
    def check_adx_filter(self, data, threshold=20):
        """
        Check if ADX is above threshold (indicating trending market)
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            threshold (int): ADX threshold
            
        Returns:
            bool: True if ADX is above threshold, False otherwise
        """
        # Get ADX threshold from config
        adx_minimum = self.trading_config.get("adx_minimum", 20)
        
        # Calculate ADX
        adx = self._calculate_adx(data)
        
        # Check if ADX is above threshold
        return adx > adx_minimum
    
    def _calculate_adx(self, data, period=14):
        """
        Calculate Average Directional Index (ADX)
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            period (int): ADX period
            
        Returns:
            float: ADX value
        """
        # Calculate True Range
        data = data.copy()
        data['h-l'] = data['high'] - data['low']
        data['h-pc'] = abs(data['high'] - data['close'].shift(1))
        data['l-pc'] = abs(data['low'] - data['close'].shift(1))
        data['tr'] = data[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        
        # Calculate Directional Movement
        data['up'] = data['high'] - data['high'].shift(1)
        data['down'] = data['low'].shift(1) - data['low']
        
        data['plus_dm'] = np.where((data['up'] > data['down']) & (data['up'] > 0), data['up'], 0)
        data['minus_dm'] = np.where((data['down'] > data['up']) & (data['down'] > 0), data['down'], 0)
        
        # Calculate Directional Indicators
        data['plus_di'] = 100 * (data['plus_dm'].ewm(alpha=1/period, adjust=False).mean() / 
                               data['tr'].ewm(alpha=1/period, adjust=False).mean())
        data['minus_di'] = 100 * (data['minus_dm'].ewm(alpha=1/period, adjust=False).mean() / 
                                data['tr'].ewm(alpha=1/period, adjust=False).mean())
        
        # Calculate Directional Movement Index
        data['dx'] = 100 * abs(data['plus_di'] - data['minus_di']) / (data['plus_di'] + data['minus_di'])
        
        # Calculate ADX
        data['adx'] = data['dx'].ewm(alpha=1/period, adjust=False).mean()
        
        # Return most recent ADX value
        return data['adx'].iloc[-1]
    
    def is_trading_allowed(self):
        """
        Check if trading is allowed based on time and other constraints
        
        Returns:
            bool: True if trading is allowed, False otherwise
        """
        try:
            # Get current time in ET (market timezone)
            import pytz
            et_tz = pytz.timezone('US/Eastern')
            utc_now = datetime.now(pytz.UTC)
            now = utc_now.astimezone(et_tz)
            
            # Market hours in ET
            market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            
            # Check if it's a weekday
            if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
                self.logger.info("Trading not allowed: Market is closed (weekend)")
                return False
            
            # Check if market is open
            if now < market_open or now > market_close:
                self.logger.info(f"Trading not allowed: Market is closed (current ET time: {now.strftime('%H:%M:%S')})")
                return False
                
            # Check no-trade window after market open
            no_trade_window = self.trading_config.get("no_trade_window_minutes", 3)
            if now < (market_open + timedelta(minutes=no_trade_window)):
                self.logger.info(f"Trading not allowed: Within {no_trade_window} minute no-trade window after market open")
                return False
                
            # Check cutoff time for new entries
            cutoff_time_str = self.trading_config.get("cutoff_time", "15:15")
            cutoff_hour, cutoff_minute = map(int, cutoff_time_str.split(':'))
            cutoff_time = now.replace(hour=cutoff_hour, minute=cutoff_minute, second=0, microsecond=0)
            
            if now > cutoff_time:
                self.logger.info(f"Trading not allowed: After cutoff time {cutoff_time_str}")
                return False
                
            # Check auto-close time
            auto_close_minutes = self.trading_config.get("auto_close_minutes", 15)
            auto_close_time = market_close - timedelta(minutes=auto_close_minutes)
            
            if now > auto_close_time:
                self.logger.info(f"Trading not allowed: Within {auto_close_minutes} minutes of market close")
                return False
                
            return True
            
        except ImportError:
            self.logger.error("pytz not installed. Please install it with: pip install pytz")
            # Fallback to original logic without timezone
            now = datetime.now()
            self.logger.warning("Using local time instead of ET. This may cause issues.")
            
            # Continue with basic checks using local time
            if now.hour < 9 or (now.hour == 9 and now.minute < 30) or now.hour >= 16:
                self.logger.info("Trading not allowed: Outside market hours (local time)")
                return False
                
            return True
        
        
    def scan_for_trades(self):
        """
        Scan for trading opportunities based on the Core Logic Flow from the spec
        """
        # Check if trading is allowed
        if not self.is_trading_allowed():
            return
            
        # Check for sector alignment (Sector Confirmation Engine)
        sector_aligned, direction, weight = self.detect_sector_alignment()
        if not sector_aligned:
            self.logger.info("No sector alignment detected, no trade opportunities")
            return
        
        self.logger.info(f"Sector alignment detected: {direction.upper()} with {weight}% weight")
        
        # Scan tickers for trading opportunities
        for ticker in self.tickers:
            # Check if we already have an active trade for this ticker
            if ticker in self.active_trades:
                continue
                
            # 1. Check for compression (Compression Detection)
            compression_detected, comp_direction = self.detect_compression(ticker)
            
            # Only proceed if compression detected and direction aligns with sector direction
            if not compression_detected or comp_direction != direction:
                continue
                
            self.logger.info(f"Compression detected for {ticker} in {comp_direction} direction")
            
            # 2. Get price data for different timeframes
            data_1m = self.get_price_data(ticker, "1m")
            data_5m = self.get_price_data(ticker, "5m")
            
            if data_1m is None or data_5m is None:
                continue
                
            # 3. Check for Momentum & Trend Confirmation
            # 3a. Calculate Stochastic Oscillator (Barry Burns' method)
            k, d = self.calculate_stochastic(data_5m, k_period=5, d_period=3, smooth=2)
            
            # Get stochastic thresholds from config
            stoch_bullish_threshold = self.trading_config.get("stoch_bullish_threshold", 20)
            stoch_bearish_threshold = self.trading_config.get("stoch_bearish_threshold", 80)
            
            # Check if momentum is aligned with direction
            stoch_aligned = False
            if direction == "bullish" and k > stoch_bullish_threshold:
                stoch_aligned = True
            elif direction == "bearish" and k < stoch_bearish_threshold:
                stoch_aligned = True
                
            if not stoch_aligned:
                self.logger.info(f"Stochastic not aligned for {ticker}: K={k:.2f}, D={d:.2f}")
                continue
                
            # 3b. Check Trend Alignment
            vwap = self._calculate_vwap(data_5m)
            ema_period = self.trading_config.get("ema_value", 15)
            ema = data_5m['close'].ewm(span=ema_period, adjust=False).mean().iloc[-1]
            last_close = data_5m['close'].iloc[-1]
            
            trend_aligned = False
            if direction == "bullish" and last_close > vwap and last_close > ema:
                trend_aligned = True
            elif direction == "bearish" and last_close < vwap and last_close < ema:
                trend_aligned = True
                
            if not trend_aligned:
                self.logger.info(f"Trend not aligned for {ticker}: Price={last_close:.2f}, VWAP={vwap:.2f}, EMA={ema:.2f}")
                continue
                
            # 4. Check for Entry Trigger
            # 4a. Confirm Heiken Ashi candle formation
            ha_signal = self.check_heiken_ashi_signal(data_1m, direction)
            if not ha_signal:
                self.logger.info(f"No Heiken Ashi confirmation for {ticker}")
                continue
                
            # 4b. Check for volume spike
            volume_spike = self._check_volume_spike(data_1m)
            
            # 4c. Check ADX Filter if enabled
            adx_filter = self.trading_config.get("adx_filter", True)
            if adx_filter:
                adx_min = self.trading_config.get("adx_minimum", 20)  # From PDF
                adx = self._calculate_adx(data_5m)
                
                if adx < adx_min:
                    self.logger.info(f"ADX filter rejected trade for {ticker}: ADX={adx:.2f} < {adx_min}")
                    continue
            
            # All criteria met, prepare to enter trade
            self.logger.info(f"Trade signal detected for {ticker}: {direction.upper()}")
            self.logger.info(f"Sector Alignment: {sector_aligned} with {weight}% weight")
            self.logger.info(f"Compression: {compression_detected}")
            self.logger.info(f"Stochastic: K={k:.2f}, D={d:.2f}")
            self.logger.info(f"Trend Alignment: Price={last_close:.2f}, VWAP={vwap:.2f}, EMA={ema:.2f}")
            self.logger.info(f"Heiken Ashi Signal: {ha_signal}")
            self.logger.info(f"Volume Spike: {volume_spike}")
            
            # Enter trade - this now enters a real trade
            self.enter_trade(ticker, direction)
    
    # Helper methods to extract option details:
    def _extract_expiry_from_symbol(self, option_symbol):
        """Extract expiry date from option symbol"""
        # Format: "SPY 241129C00420000"
        try:
            parts = option_symbol.split()
            if len(parts) >= 2:
                date_str = parts[1][:6]  # First 6 chars are YYMMDD
                return f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
        except:
            return "Unknown"
        
    def _extract_strike_from_symbol(self, option_symbol):
        """Extract strike price from option symbol"""
        # Format: "SPY 241129C00420000"
        try:
            parts = option_symbol.split()
            if len(parts) >= 2:
                # Find C or P position
                for i, char in enumerate(parts[1]):
                    if char in ['C', 'P']:
                        strike_str = parts[1][i+1:]
                        # Convert to price (last 3 digits are decimals)
                        strike = float(strike_str) / 1000
                        return f"${strike:.2f}"
        except:
            return "Unknown"


    def enter_trade(self, symbol, direction):
        """
        Enter a trade for the given symbol and direction
        
        Args:
            symbol (str): Symbol to trade
            direction (str): "bullish" or "bearish"
        """
        try:
            # Check if trading is allowed based on time and other constraints
            if not self.is_trading_allowed():
                self.logger.info(f"Trading not allowed for {symbol}")
                return
                
            # Check if we're already in a trade for this symbol
            if symbol in self.active_trades:
                self.logger.info(f"Already in a trade for {symbol}")
                return
                
            # Get current price
            current_price = self._get_current_price(symbol)
            if not current_price:
                self.logger.error(f"Could not get current price for {symbol}")
                return
                
            # Find appropriate option contract
            contract = self._find_option_contract(symbol, direction, current_price)
            if not contract:
                self.logger.error(f"Could not find appropriate option contract for {symbol}")
                return
                
            # Get number of contracts from config
            contracts_per_trade = self.trading_config.get("contracts_per_trade", 1)
            
            # Create order - LIVE MARKET ORDER
            if direction == "bullish":
                order = self.order_manager.create_equity_option_order(
                    symbol=contract,
                    quantity=contracts_per_trade,
                    direction="Buy to Open",
                    price=None,  # Use market order for faster entry
                    order_type="Market"
                )
            else:
                # For bearish trades, use puts
                order = self.order_manager.create_equity_option_order(
                    symbol=contract,
                    quantity=contracts_per_trade,
                    direction="Buy to Open",
                    price=None,  # Use market order for faster entry
                    order_type="Market"
                )
                    
            # Submit order - this places a REAL ORDER
            self.logger.info(f"Submitting LIVE order: {json.dumps(order)}")
            result = self.order_manager.submit_order(order)
            
            # Check if order was submitted successfully
            if isinstance(result, dict) and "error" in result:
                self.logger.error(f"Error submitting order for {symbol}: {result['error']}")
                return
            
            # Log successful order submission
            self.logger.info(f"LIVE order submitted successfully: {json.dumps(result)}")
                
            # Get order ID
            order_id = None
            if isinstance(result, dict) and "order" in result:
                order_data = result.get("order", {})
                order_id = order_data.get("id")
                
            if not order_id:
                self.logger.error(f"Could not get order ID for {symbol}")
                return
                
            # Calculate stop level
            stop_level = self._calculate_stop_level(symbol, direction, current_price)
            
            # Place stop order
            stop_order_id = self._place_initial_stop_order(
                symbol, 
                stop_level, 
                direction, 
                contract, 
                contracts_per_trade
            )
            
            # Add to active trades
            self.active_trades[symbol] = {
                "ticker": symbol,
                "option_symbol": contract,  # Full option symbol like "SPY 241129C00420000"
                "type": "Long Call" if direction == "bullish" else "Long Put",
                "entry_time": datetime.now().strftime("%H:%M:%S"),
                "underlying_price": str(current_price),
                "entry_price": "Pending",  # Will be updated when order fills
                "current_price": "Pending",
                "pl": "$0.00 (0.0%)",
                "stop": f"${stop_level:.2f}",
                "status": "Open",
                "order_id": order_id,
                "stop_order_id": stop_order_id,
                "contract": contract,
                "quantity": contracts_per_trade,
                "expiry": self._extract_expiry_from_symbol(contract),
                "strike": self._extract_strike_from_symbol(contract),
                "option_type": "Call" if direction == "bullish" else "Put"
            }
            
            # Save to position manager for persistence
            self.position_manager.add_position(symbol, self.active_trades[symbol])
            
            self.logger.info(f"Entered {direction.upper()} trade for {symbol} with {contracts_per_trade} contracts of {contract}")
            
            # Set up trailing stop
            self._setup_trailing_stop(symbol)
            
        except Exception as e:
            self.logger.error(f"Error entering trade for {symbol}: {e}")
    
    def _place_initial_stop_order(self, symbol, stop_price, direction, contract, quantity):
        """
        Place initial stop order when entering a trade
        
        Args:
            symbol (str): Symbol for the trade
            stop_price (float): Stop price
            direction (str): "bullish" or "bearish" 
            contract (str): Option contract symbol
            quantity (int): Number of contracts
            
        Returns:
            str: Stop order ID or None if failed
        """
        try:
            # Determine order direction (sell to close for long, buy to close for short)
            order_direction = "Sell to Close" if direction == "bullish" else "Buy to Close"
            
            # Create stop order
            stop_order = {
                "time-in-force": "GTC",  # Good Till Canceled
                "order-type": "Stop",
                "stop-trigger-price": str(stop_price),
                "legs": [
                    {
                        "instrument-type": "Equity Option",
                        "symbol": contract,
                        "quantity": quantity,
                        "action": order_direction
                    }
                ]
            }
            
            # Submit order
            result = self.order_manager.submit_order(stop_order)
            
            if isinstance(result, dict) and "error" not in result:
                order_data = result.get("order", {})
                stop_order_id = order_data.get("id")
                
                if stop_order_id:
                    self.logger.info(f"Placed initial stop order {stop_order_id} at {stop_price} for {symbol}")
                    return stop_order_id
            
            error_msg = result.get("error", "Unknown error") if isinstance(result, dict) else "Failed to submit order"
            self.logger.error(f"Failed to place initial stop order for {symbol}: {error_msg}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error placing initial stop order for {symbol}: {e}")
            return None
    
    def _get_current_price(self, symbol):
        """
        Get current price for a symbol
        
        Args:
            symbol (str): Symbol to get price for
            
        Returns:
            float: Current price or None if not available
        """
        try:
            # Try to get price from instrument fetcher
            price = self.instrument_fetcher.get_current_price(symbol)
            if price:
                return price
                
            # If not available, try to get from market data client
            quotes = self.market_data.get_quotes_from_db(symbol, limit=1)
            if quotes and len(quotes) > 0:
                quote = quotes[0]
                
                # Calculate mid price
                bid = float(quote.get("bid", 0))
                ask = float(quote.get("ask", 0))
                
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
                elif bid > 0:
                    return bid
                elif ask > 0:
                    return ask
                    
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting current price for {symbol}: {e}")
            return None
    
    def _find_option_contract(self, symbol, direction, current_price):
        """
        Find appropriate option contract for a trade
        
        Args:
            symbol (str): Underlying symbol
            direction (str): "bullish" or "bearish"
            current_price (float): Current price of underlying
            
        Returns:
            str: Option contract symbol or None if not found
        """
        try:
            # Get option chain
            option_chain = self.instrument_fetcher.fetch_nested_option_chains(symbol)
            if not option_chain:
                return None
                
            # Get expirations
            expirations = option_chain.get("expirations", [])
            if not expirations:
                return None
                
            # Sort expirations by date (ascending)
            expirations.sort(key=lambda x: x.get("expiration-date", ""))
            
            # Get nearest expiration (0DTE or shortest expiry)
            nearest_expiration = expirations[0]
            
            # Get strikes
            strikes = nearest_expiration.get("strikes", [])
            if not strikes:
                return None
                
            # Sort strikes by distance from current price
            if direction == "bullish":
                # For bullish trades, look for calls
                strikes.sort(key=lambda x: abs(float(x.get("strike-price", 0)) - current_price))
                
                # Get nearest strike that has a delta of about 0.60
                for strike in strikes:
                    # In a real implementation, we would check the delta
                    # For now, just use the strike that's slightly out-of-the-money
                    strike_price = float(strike.get("strike-price", 0))
                    if strike_price >= current_price and strike_price <= current_price * 1.03:
                        return strike.get("call", "")
                        
                # If no suitable strike found, use the nearest one
                return strikes[0].get("call", "")
                
            else:
                # For bearish trades, look for puts
                strikes.sort(key=lambda x: abs(float(x.get("strike-price", 0)) - current_price))
                
                # Get nearest strike that has a delta of about 0.60
                for strike in strikes:
                    # In a real implementation, we would check the delta
                    # For now, just use the strike that's slightly out-of-the-money
                    strike_price = float(strike.get("strike-price", 0))
                    if strike_price <= current_price and strike_price >= current_price * 0.97:
                        return strike.get("put", "")
                        
                # If no suitable strike found, use the nearest one
                return strikes[0].get("put", "")
                
        except Exception as e:
            self.logger.error(f"Error finding option contract for {symbol}: {e}")
            return None
    
    def _calculate_stop_level(self, symbol, direction, current_price):
        """
        Calculate stop level for a trade
        
        Args:
            symbol (str): Symbol being traded
            direction (str): "bullish" or "bearish"
            current_price (float): Current price of underlying
            
        Returns:
            float: Stop level
        """
        # Get stop loss method from configuration
        stop_loss_method = self.trading_config.get("stop_loss_method", "ATR Multiple")
        
        if stop_loss_method == "Fixed Percentage":
            # Get percentage from config (as a percentage value, e.g., 1.0 for 1%)
            percentage = self.trading_config.get("fixed_stop_percentage", 1.0)
            
            # Calculate stop level based on fixed percentage
            if direction == "bullish":
                return current_price * (1 - percentage / 100)
            else:
                return current_price * (1 + percentage / 100)
        
        elif stop_loss_method == "ATR Multiple":
            # Get data for ATR calculation
            data = self.get_price_data(symbol, "5m")
            if data is None:
                # Default to a 1% stop if no data available
                return current_price * 0.99 if direction == "bullish" else current_price * 1.01
                
            # Get ATR
            atr = self._calculate_atr(data)
            
            # Get ATR multiple from config (default to 1.5 if not specified)
            atr_multiple = self.trading_config.get("atr_multiple", 1.5)
            
            # Calculate stop level based on ATR multiple
            if direction == "bullish":
                return current_price - (atr * atr_multiple)
            else:
                return current_price + (atr * atr_multiple)
        
        elif stop_loss_method == "Structure-based":
            # Get price data
            data = self.get_price_data(symbol, "5m")
            if data is None:
                # Default to a 1% stop if no data available
                return current_price * 0.99 if direction == "bullish" else current_price * 1.01
            
            # For bullish trades, use previous swing low
            if direction == "bullish":
                # Look back up to 10 candles to find swing low
                lookback = min(10, len(data))
                if lookback < 3:
                    return current_price * 0.99  # Default if not enough data
                    
                # Find local minima (where current low is lower than previous and next)
                swing_lows = []
                for i in range(1, lookback - 1):
                    if data['low'].iloc[i] < data['low'].iloc[i-1] and data['low'].iloc[i] < data['low'].iloc[i+1]:
                        swing_lows.append(data['low'].iloc[i])
                
                # Use most recent swing low, or default to 1% if none found
                if swing_lows:
                    return min(swing_lows)
                else:
                    return current_price * 0.99
            
            # For bearish trades, use previous swing high
            else:
                # Look back up to 10 candles to find swing high
                lookback = min(10, len(data))
                if lookback < 3:
                    return current_price * 1.01  # Default if not enough data
                    
                # Find local maxima (where current high is higher than previous and next)
                swing_highs = []
                for i in range(1, lookback - 1):
                    if data['high'].iloc[i] > data['high'].iloc[i-1] and data['high'].iloc[i] > data['high'].iloc[i+1]:
                        swing_highs.append(data['high'].iloc[i])
                
                # Use most recent swing high, or default to 1% if none found
                if swing_highs:
                    return max(swing_highs)
                else:
                    return current_price * 1.01
        
        # Fallback to a default 1% stop
        return current_price * 0.99 if direction == "bullish" else current_price * 1.01


    
    def _calculate_atr(self, data, period=14):
        """
        Calculate Average True Range (ATR)
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            period (int): ATR period
            
        Returns:
            float: ATR value
        """
        # Calculate True Range
        data = data.copy()
        data['h-l'] = data['high'] - data['low']
        data['h-pc'] = abs(data['high'] - data['close'].shift(1))
        data['l-pc'] = abs(data['low'] - data['close'].shift(1))
        data['tr'] = data[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        
        # Calculate ATR
        atr = data['tr'].rolling(window=period).mean()
        
        # Return most recent ATR value
        return atr.iloc[-1]
    
    def _schedule_trailing_stop_check(self, symbol):
        """
        Schedule a check for trailing stop adjustments
        
        Args:
            symbol (str): Symbol being traded
        """
        # This would normally be done with a timer or in a separate thread
        # For now, just mark that trailing stop checks should be performed
        if symbol in self.active_trades:
            self.active_trades[symbol]["check_trailing_stop"] = True


    def _setup_trailing_stop(self, symbol):
        """
        Set up trailing stop for a trade
        
        Args:
            symbol (str): Symbol being traded
        """
        if symbol not in self.active_trades:
            return
            
        trade = self.active_trades[symbol]
        
        # Get trailing stop method from config
        trailing_method = self.trading_config.get("trailing_stop_method", "Heiken Ashi Candle Trail (1-3 candle lookback)")
        
        # Log the trailing stop setup
        self.logger.info(f"Setting up trailing stop for {symbol} using method: {trailing_method}")
        
        # In a real implementation, set up trailing stop based on the method
        trade["trailing_method"] = trailing_method
        
        # Initial stop level calculation
        direction = "bullish" if trade["type"] == "Long" else "bearish"
        current_price = float(trade["entry_price"])
        stop_level = self._calculate_stop_level(symbol, direction, current_price)
        trade["stop"] = stop_level
        
        # Schedule a check for trailing stop adjustments
        self._schedule_trailing_stop_check(symbol)

    
    def manage_active_trades(self):
        """
        Manage active trades (update stops, check exits, etc.)
        """
        # Get current time
        now = datetime.now()
        
        # Check auto-close condition
        auto_close_minutes = self.trading_config.get("auto_close_minutes", 15)
        auto_close_time = self.market_close_time - timedelta(minutes=auto_close_minutes)
        
        if now > auto_close_time:
            # Close all open positions
            for symbol in list(self.active_trades.keys()):
                self.exit_trade(symbol, reason="Auto-close before market close")
            return
            
        # Check failsafe condition
        failsafe_minutes = self.trading_config.get("failsafe_minutes", 20)
        
        # Check each active trade
        for symbol in list(self.active_trades.keys()):
            trade = self.active_trades[symbol]
            
            # Check trade time
            entry_time = datetime.fromisoformat(trade["entry_time"].replace('Z', '+00:00'))
            if now - entry_time > timedelta(minutes=failsafe_minutes):
                self.exit_trade(symbol, reason=f"Failsafe exit after {failsafe_minutes} minutes")
                continue
                
            # Update current price and P&L
            current_price = self._get_current_price(symbol)
            if current_price:
                trade["current_price"] = f"${current_price:.2f}"
                
                # Calculate P&L if entry price is known
                if trade["entry_price"] != "Pending":
                    entry_price = float(trade["entry_price"].replace("$", ""))
                    if trade["type"] in ["Long", "Long Call"]:
                        pnl = (current_price - entry_price) * float(trade.get("quantity", 1)) * 100
                        pnl_pct = ((current_price - entry_price) / entry_price) * 100
                    else:
                        pnl = (entry_price - current_price) * float(trade.get("quantity", 1)) * 100
                        pnl_pct = ((entry_price - current_price) / entry_price) * 100
                    
                    trade["pl"] = f"${pnl:.2f} ({pnl_pct:.1f}%)"
                    trade["unrealized_pnl"] = pnl
                    
                    # Update position manager with current price and P&L
                    self.position_manager.update_position(symbol, {
                        "current_price": current_price,
                        "pl": trade["pl"],
                        "unrealized_pnl": pnl
                    })
                
            # Check exit conditions based on trailing method
            trailing_method = trade.get("trailing_method", "")
            
            # Get price data for analysis
            data_1m = self.get_price_data(symbol, "1m")
            data_5m = self.get_price_data(symbol, "5m")
            
            if data_1m is not None:
                # Exit Condition: Opposing Heiken Ashi signal
                opposing_signal = self.check_heiken_ashi_signal(
                    data_1m, 
                    "bearish" if trade["type"] == "Long" else "bullish"
                )
                if opposing_signal:
                    self.exit_trade(symbol, reason="Opposing Heiken Ashi signal")
                    continue
                    
            if data_5m is not None:
                # Exit Condition: Opposing Stochastic crossover
                k, d = self.calculate_stochastic(data_5m)
                
                # Get exit thresholds from config
                stoch_exit_overbought = self.trading_config.get("stoch_exit_overbought", 80)
                stoch_exit_oversold = self.trading_config.get("stoch_exit_oversold", 20)
                
                if trade["type"] == "Long" and k > stoch_exit_overbought and k < d:
                    self.exit_trade(symbol, reason="Stochastic overbought and crossing down")
                    continue
                elif trade["type"] == "Short" and k < stoch_exit_oversold and k > d:
                    self.exit_trade(symbol, reason="Stochastic oversold and crossing up")
                    continue
                
                # Exit Condition: VWAP or EMA crossover against trade
                vwap = self._calculate_vwap(data_5m)
                ema = data_5m['close'].ewm(span=self.trading_config.get("ema_value", 15), adjust=False).mean().iloc[-1]
                current_price = self._get_current_price(symbol)
                
                if current_price:
                    if trade["type"] == "Long" and current_price < min(vwap, ema):
                        self.exit_trade(symbol, reason="Price crossed below VWAP and EMA")
                        continue
                    elif trade["type"] == "Short" and current_price > max(vwap, ema):
                        self.exit_trade(symbol, reason="Price crossed above VWAP and EMA")
                        continue
            
            # Exit Condition: Re-entry into compression zone
            compression_detected, _ = self.detect_compression(symbol)
            if compression_detected:
                self.exit_trade(symbol, reason="Re-entry into compression zone")
                continue

            # Check and update trailing stops if necessary
            if trade.get("check_trailing_stop", False):
                self._update_trailing_stop(symbol)



    def _update_trailing_stop(self, symbol):
        """
        Update trailing stop for a trade based on selected method
        
        Args:
            symbol (str): Symbol being traded
        """
        if symbol not in self.active_trades:
            return
            
        trade = self.active_trades[symbol]
        trailing_method = trade.get("trailing_method")
        
        # Get current price
        current_price = self._get_current_price(symbol)
        if not current_price:
            return
            
        direction = "bullish" if trade["type"] == "Long" else "bearish"
        current_stop = float(trade["stop"])
        
        # Calculate new stop based on trailing method
        new_stop = current_stop
        
        if trailing_method == "Heiken Ashi Candle Trail (1-3 candle lookback)":
            # Get HA data
            data = self.get_price_data(symbol, "5m")
            if data is not None:
                ha = self._calculate_heiken_ashi(data)
                
                # For long trades, use the low of the previous 1-3 HA candles
                if direction == "bullish":
                    lookback = min(3, len(ha) - 1)
                    candle_stop = min(ha['low'].iloc[-lookback:])
                    
                    # Only update if new stop is higher
                    if candle_stop > current_stop:
                        new_stop = candle_stop
                
                # For short trades, use the high of the previous 1-3 HA candles
                else:
                    lookback = min(3, len(ha) - 1)
                    candle_stop = max(ha['high'].iloc[-lookback:])
                    
                    # Only update if new stop is lower
                    if candle_stop < current_stop:
                        new_stop = candle_stop
        
        elif trailing_method == "EMA Trail (e.g., EMA(9) trailing stop)":
            # Get data and calculate EMA
            data = self.get_price_data(symbol, "5m")
            if data is not None:
                ema_period = 9  # Could be configurable
                ema = data['close'].ewm(span=ema_period, adjust=False).mean()
                
                # Get the latest EMA value
                ema_value = ema.iloc[-1]
                
                # For long trades, use EMA as stop if it's higher than current stop
                if direction == "bullish" and ema_value > current_stop:
                    new_stop = ema_value
                
                # For short trades, use EMA as stop if it's lower than current stop
                elif direction == "bearish" and ema_value < current_stop:
                    new_stop = ema_value
        
        elif trailing_method == "% Price Trail (e.g., 1.5% below current price)":
            # Default to 1.5% trail
            trail_percentage = 1.5
            
            # Calculate stop based on percentage trail
            if direction == "bullish":
                price_stop = current_price * (1 - trail_percentage / 100)
                
                # Only update if new stop is higher
                if price_stop > current_stop:
                    new_stop = price_stop
            else:
                price_stop = current_price * (1 + trail_percentage / 100)
                
                # Only update if new stop is lower
                if price_stop < current_stop:
                    new_stop = price_stop
        
        elif trailing_method == "ATR-Based Trail (1.5x ATR)":
            # Get data and calculate ATR
            data = self.get_price_data(symbol, "5m")
            if data is not None:
                atr = self._calculate_atr(data)
                atr_multiple = 1.5  # Could be configurable
                
                # Calculate stop based on ATR trail
                if direction == "bullish":
                    atr_stop = current_price - (atr * atr_multiple)
                    
                    # Only update if new stop is higher
                    if atr_stop > current_stop:
                        new_stop = atr_stop
                else:
                    atr_stop = current_price + (atr * atr_multiple)
                    
                    # Only update if new stop is lower
                    if atr_stop < current_stop:
                        new_stop = atr_stop
        
        elif trailing_method == "Fixed Tick/Point Trail (custom value)":
            # Default to 5 points trail
            trail_points = 5.0
            
            # Calculate stop based on fixed point trail
            if direction == "bullish":
                tick_stop = current_price - trail_points
                
                # Only update if new stop is higher
                if tick_stop > current_stop:
                    new_stop = tick_stop
            else:
                tick_stop = current_price + trail_points
                
                # Only update if new stop is lower
                if tick_stop < current_stop:
                    new_stop = tick_stop
        
        # Update stop if it changed
        if new_stop != current_stop:
            trade["stop"] = new_stop
            self.logger.info(f"Updated trailing stop for {symbol} to {new_stop} using {trailing_method}")
            
            # Update in position manager for persistence
            self.position_manager.update_position(symbol, {
                "stop": new_stop,
                "trailing_method": trailing_method,
                "last_stop_update": datetime.now().isoformat()
            })
            
            # For real broker, update the actual stop order
            try:
                # Check if stop order ID exists
                if "stop_order_id" in trade:
                    # Cancel the existing stop order
                    self.order_manager.cancel_order(trade["stop_order_id"])
                    self.logger.info(f"Cancelled existing stop order {trade['stop_order_id']} for {symbol}")
                    
                    # Create a new stop order
                    new_stop_order = self._create_stop_order(symbol, new_stop, direction)
                    result = self.order_manager.submit_order(new_stop_order)
                    
                    if isinstance(result, dict) and "error" not in result:
                        order_data = result.get("order", {})
                        stop_order_id = order_data.get("id")
                        
                        if stop_order_id:
                            trade["stop_order_id"] = stop_order_id
                            self.logger.info(f"Created new stop order {stop_order_id} at {new_stop} for {symbol}")
                            
                            # Update stop order ID in position manager
                            self.position_manager.update_position(symbol, {
                                "stop_order_id": stop_order_id
                            })
                        else:
                            self.logger.error(f"Failed to get stop order ID for {symbol}")
                    else:
                        error_msg = result.get("error", "Unknown error")
                        self.logger.error(f"Failed to create new stop order for {symbol}: {error_msg}")
                else:
                    # No existing stop order, create a new one
                    new_stop_order = self._create_stop_order(symbol, new_stop, direction)
                    result = self.order_manager.submit_order(new_stop_order)
                    
                    if isinstance(result, dict) and "error" not in result:
                        order_data = result.get("order", {})
                        stop_order_id = order_data.get("id")
                        
                        if stop_order_id:
                            trade["stop_order_id"] = stop_order_id
                            self.logger.info(f"Created initial stop order {stop_order_id} at {new_stop} for {symbol}")
                            
                            # Update stop order ID in position manager
                            self.position_manager.update_position(symbol, {
                                "stop_order_id": stop_order_id
                            })
                        else:
                            self.logger.error(f"Failed to get stop order ID for {symbol}")
                    else:
                        error_msg = result.get("error", "Unknown error")
                        self.logger.error(f"Failed to create initial stop order for {symbol}: {error_msg}")
            except Exception as e:
                self.logger.error(f"Error updating stop order for {symbol}: {e}")
    

    
    def _create_stop_order(self, symbol, stop_price, direction):
        """
        Create a stop order for a symbol
        
        Args:
            symbol (str): Symbol to create stop order for
            stop_price (float): Stop price
            direction (str): "bullish" or "bearish"
            
        Returns:
            dict: Stop order object
        """
        trade = self.active_trades.get(symbol)
        if not trade:
            return None
            
        # Get contract symbol from trade
        contract = trade.get("contract")
        if not contract:
            return None
            
        # Get quantity from trade
        quantity = trade.get("quantity", 1)
        
        # Determine order direction (sell to close for long, buy to close for short)
        order_direction = "Sell to Close" if direction == "bullish" else "Buy to Close"
        
        # Create stop order
        stop_order = {
            "time-in-force": "GTC",  # Good Till Canceled
            "order-type": "Stop", 
            "stop-trigger-price": str(stop_price),
            "legs": [
                {
                    "instrument-type": "Equity Option",
                    "symbol": contract,
                    "quantity": quantity,
                    "action": order_direction
                }
            ]
        }
        
        return stop_order


    def exit_trade(self, symbol, reason="Manual exit"):
        """
        Exit a trade
        
        Args:
            symbol (str): Symbol to exit
            reason (str): Reason for exit
        """
        if symbol not in self.active_trades:
            return
            
        trade = self.active_trades[symbol]
        
        try:
            # Create exit order
            if trade["direction"] == "bullish":
                order = self.order_manager.create_equity_option_order(
                    symbol=trade["contract"],
                    quantity=trade["quantity"],
                    direction="Sell to Close",
                    price=None,  # Use market order for faster exit
                    order_type="Market"
                )
            else:
                order = self.order_manager.create_equity_option_order(
                    symbol=trade["contract"],
                    quantity=trade["quantity"],
                    direction="Sell to Close",
                    price=None,  # Use market order for faster exit
                    order_type="Market"
                )
                
            # Submit order
            result = self.order_manager.submit_order(order)
            
            # Check if order was submitted successfully
            if "error" in result:
                self.logger.error(f"Error exiting trade for {symbol}: {result['error']}")
                return
                
            # Remove from active trades
            exit_trade = self.active_trades.pop(symbol)
            
            # Close position in position manager
            self.position_manager.close_position(symbol, {
                "exit_time": datetime.now().isoformat(),
                "exit_reason": reason,
                "exit_price": exit_trade.get("current_price", "Unknown"),
                "final_pl": exit_trade.get("pl", "$0.00 (0.0%)")
            })
            
            # Log the exit
            self.logger.info(f"Exited trade for {symbol}: {reason}")
            
            return exit_trade
            
        except Exception as e:
            self.logger.error(f"Error exiting trade for {symbol}: {e}")


    def detect_sector_alignment(self):
        """
        Detect sector alignment using real market data OR Mag7 alignment
        
        Returns:
            tuple: (alignment_detected, direction, combined_weight)
        """
        try:
            # Check if we should use Mag7 instead of sectors
            if self.mag7_strategy and self.trading_config.get("use_mag7_confirmation", False):
                # Use Mag7 alignment
                aligned, direction, percentage = self.mag7_strategy.check_mag7_alignment()
                self.logger.info(f"Using Mag7 alignment: aligned={aligned}, direction={direction}, percentage={percentage}%")
                return aligned, direction, percentage
            
            # Original sector alignment logic - only execute if NOT using Mag7
            # Check if we have sector data
            if not hasattr(self, 'sector_status') or not self.sector_status:
                self.logger.warning("No sector status data available")
                return False, "neutral", 0
                
            # Log current sector status for debugging
            self.logger.info(f"Checking sector alignment. Current statuses: {self.sector_status}")
            
            # Use the sector weights from trading configuration
            sector_weights = {
                "XLK": 32,  # Tech
                "XLF": 14,  # Financials
                "XLV": 11,  # Health Care
                "XLY": 11   # Consumer Discretionary
            }
            
            # Check if XLK is bullish or bearish
            xlk_status = self.sector_status.get("XLK", "neutral")
            
            if xlk_status == "neutral":
                self.logger.info("XLK is neutral, no alignment possible")
                return False, "neutral", 0
            
            # Count aligned sectors
            aligned_sectors = []
            
            for sector, status in self.sector_status.items():
                if sector != "XLK" and status == xlk_status:
                    aligned_sectors.append(sector)
                    
            self.logger.info(f"XLK is {xlk_status}, aligned sectors: {aligned_sectors}")
            
            # Check if we have at least one other sector aligned with XLK
            if not aligned_sectors:
                self.logger.info("No other sectors aligned with XLK")
                return False, "neutral", 0
                
            # Calculate combined weight
            combined_weight = sector_weights["XLK"]
            for sector in aligned_sectors:
                combined_weight += sector_weights.get(sector, 0)
                
            self.logger.info(f"Combined weight: {combined_weight}% (threshold: 43%)")
            
            # Check if weight exceeds 43% threshold
            if combined_weight >= 43:
                self.logger.info(f"Sector alignment detected: {xlk_status} with {combined_weight}% weight")
                return True, xlk_status, combined_weight
            else:
                self.logger.info(f"Combined weight {combined_weight}% below 43% threshold")
                return False, "neutral", 0
                
        except Exception as e:
            self.logger.error(f"Error in sector alignment detection: {e}")
            return False, "neutral", 0


    def detect_compression(self, symbol, timeframe=None):
        """
        Detect price compression using real market data
        """
        try:
            # Get timeframe from config if not provided
            if timeframe is None:
                timeframe = "5m"
            
            # Log for debugging
            self.logger.info(f"Checking compression for {symbol} on {timeframe} timeframe")
            
            # Get price data from candle builder or database
            if self.market_data and self.market_data.candle_builder:
                # Try to get recent candles from candle builder
                candles = self.market_data.candle_builder.get_candle_history(symbol, 5, count=30)
                
                if not candles or len(candles) < 20:
                    # Fallback to database
                    candles_db = self.market_data.candle_builder.get_candles_from_db(
                        symbol, "5m", limit=50
                    )
                    if candles_db and len(candles_db) >= 20:
                        df = pd.DataFrame(candles_db)
                    else:
                        self.logger.warning(f"Insufficient data for compression detection on {symbol}")
                        return False, "neutral"
                else:
                    df = pd.DataFrame(candles)
            else:
                self.logger.warning("No market data client available for compression detection")
                return False, "neutral"
                
            # Ensure numeric types
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # 1. Calculate Bollinger Bands width
            bb_width = self._calculate_bollinger_band_width(df)
            bb_width_threshold = float(self.trading_config.get("bb_width_threshold", 0.05))
            bb_compression = bb_width < bb_width_threshold
            
            self.logger.debug(f"BB Width: {bb_width:.4f}, Threshold: {bb_width_threshold}, Compressed: {bb_compression}")
            
            # 2. Calculate Donchian Channel contraction
            high_20 = df['high'].rolling(20).max().iloc[-1]
            low_20 = df['low'].rolling(20).min().iloc[-1]
            dc_range = high_20 - low_20
            avg_range = (df['high'] - df['low']).mean()
            dc_threshold = float(self.trading_config.get("donchian_contraction_threshold", 0.6))
            dc_compression = dc_range < (avg_range * dc_threshold)
            
            self.logger.debug(f"DC Range: {dc_range:.4f}, Avg Range: {avg_range:.4f}, Compressed: {dc_compression}")
            
            # 3. Volume analysis
            volume_compression = False
            if 'volume' in df.columns and df['volume'].sum() > 0:
                recent_vol = df['volume'].iloc[-5:].mean()
                avg_vol = df['volume'].mean()
                vol_threshold = float(self.trading_config.get("volume_squeeze_threshold", 0.3))
                volume_compression = recent_vol < (avg_vol * vol_threshold)
                self.logger.debug(f"Recent Vol: {recent_vol:.0f}, Avg Vol: {avg_vol:.0f}, Compressed: {volume_compression}")
            
            # Need 2 out of 3 for compression
            compression_count = sum([bb_compression, dc_compression, volume_compression])
            compression_detected = compression_count >= 2
            
            self.logger.info(f"Compression indicators for {symbol}: BB={bb_compression}, DC={dc_compression}, Vol={volume_compression}, Total={compression_count}/3")
            
            if compression_detected:
                # Determine direction based on recent price action
                last_close = df['close'].iloc[-1]
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                
                if last_close > sma_20:
                    self.logger.info(f"Compression detected for {symbol} with bullish bias")
                    return True, "bullish"
                else:
                    self.logger.info(f"Compression detected for {symbol} with bearish bias")
                    return True, "bearish"
            else:
                self.logger.info(f"No compression detected for {symbol}")
                return False, "neutral"
                
        except Exception as e:
            self.logger.error(f"Error in compression detection for {symbol}: {e}")
            return False, "neutral"
    
    def _calculate_donchian_channel(self, data, window=20):
        """
        Calculate Donchian Channel
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            window (int): Lookback period
            
        Returns:
            tuple: (upper_band, middle_band, lower_band)
        """
        # Calculate upper and lower bands
        upper_band = data['high'].rolling(window=window).max()
        lower_band = data['low'].rolling(window=window).min()
        
        # Calculate middle band (average of upper and lower)
        middle_band = (upper_band + lower_band) / 2
        
        # Return most recent values
        return upper_band.iloc[-1], middle_band.iloc[-1], lower_band.iloc[-1]
        
    def _calculate_volume_squeeze(self, data, bb_window=20, kc_window=20, kc_mult=1.5):
        """
        Calculate Volume Squeeze Pro indicator
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            bb_window (int): Bollinger Bands window
            kc_window (int): Keltner Channel window
            kc_mult (float): Keltner Channel multiplier
            
        Returns:
            bool: True if squeeze is on, False otherwise
        """
        # Calculate Bollinger Bands
        bb_middle = data['close'].rolling(window=bb_window).mean()
        bb_std = data['close'].rolling(window=bb_window).std()
        bb_upper = bb_middle + (bb_std * 2)
        bb_lower = bb_middle - (bb_std * 2)
        
        # Calculate Keltner Channels
        kc_middle = data['close'].rolling(window=kc_window).mean()
        kc_range = data['high'].rolling(window=kc_window).max() - data['low'].rolling(window=kc_window).min()
        kc_upper = kc_middle + (kc_range * kc_mult)
        kc_lower = kc_middle - (kc_range * kc_mult)
        
        # Check if Bollinger Bands are inside Keltner Channels (squeeze condition)
        squeeze_on = (bb_upper.iloc[-1] <= kc_upper.iloc[-1]) and (bb_lower.iloc[-1] >= kc_lower.iloc[-1])
        
        return squeeze_on



    def _check_volume_spike(self, data):
        """
        Check for a volume spike as part of breakout confirmation
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            
        Returns:
            bool: True if volume spike is detected, False otherwise
        """
        try:
            # Get last few candles
            lookback = min(10, len(data))
            if lookback < 3:
                return False
                
            # Calculate average volume over previous candles
            avg_volume = data['volume'].iloc[-lookback:-1].mean()
            current_volume = data['volume'].iloc[-1]
            
            # Check if current volume is significantly higher than average
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
            
            # Threshold for volume spike (configurable)
            volume_spike_threshold = self.trading_config.get("volume_spike_threshold", 1.5)
            
            return volume_ratio > volume_spike_threshold
            
        except Exception as e:
            self.logger.error(f"Error checking volume spike: {e}")
            return False


    
    def _check_pivot_zone_continuation(self, symbol, direction):
        """
        Check for pivot zone continuation pattern
        
        Args:
            symbol (str): Symbol to check
            direction (str): "bullish" or "bearish"
            
        Returns:
            bool: True if pivot zone continuation is detected, False otherwise
        """
        try:
            # Get price data for analysis
            data_5m = self.get_price_data(symbol, "5m")
            if data_5m is None or len(data_5m) < 20:
                return False
                
            # Check for breakout first
            compression_detected, comp_direction = self.detect_compression(symbol)
            if not compression_detected or comp_direction != direction:
                return False
                
            # Check for pullback to pivot level (previous resistance or support)
            vwap = self._calculate_vwap(data_5m)
            last_close = data_5m['close'].iloc[-1]
            
            # For bullish continuation, price should pull back to support (VWAP)
            if direction == "bullish":
                pullback_to_pivot = abs(last_close - vwap) / vwap < 0.005  # Within 0.5% of VWAP
                
                # Check renewed momentum with Heiken Ashi
                renewed_momentum = self.check_heiken_ashi_signal(data_5m, "bullish")
                
                return pullback_to_pivot and renewed_momentum
                
            # For bearish continuation, price should pull back to resistance (VWAP)
            elif direction == "bearish":
                pullback_to_pivot = abs(last_close - vwap) / vwap < 0.005  # Within 0.5% of VWAP
                
                # Check renewed momentum with Heiken Ashi
                renewed_momentum = self.check_heiken_ashi_signal(data_5m, "bearish")
                
                return pullback_to_pivot and renewed_momentum
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking pivot zone continuation: {e}")
            return False


    
    def _check_vwap_reclaim_rejection(self, symbol):
        """
        Check for VWAP reclaim or rejection pattern
        
        Args:
            symbol (str): Symbol to check
            
        Returns:
            tuple: (detected, direction) where direction is "bullish" or "bearish"
        """
        try:
            # Get price data for analysis
            data_5m = self.get_price_data(symbol, "5m")
            if data_5m is None or len(data_5m) < 10:
                return False, "neutral"
                
            # Calculate VWAP
            vwap = self._calculate_vwap(data_5m)
            
            # Get last few candles
            prev_close = data_5m['close'].iloc[-2]
            current_close = data_5m['close'].iloc[-1]
            current_open = data_5m['open'].iloc[-1]
            
            # Check for VWAP reclaim (bullish)
            if prev_close < vwap and current_close > vwap:
                # Confirm with sector alignment
                sector_aligned, direction, _ = self.detect_sector_alignment()
                if sector_aligned and direction == "bullish":
                    return True, "bullish"
                    
            # Check for VWAP rejection (bearish)
            if prev_close > vwap and current_close < vwap:
                # Confirm with sector alignment
                sector_aligned, direction, _ = self.detect_sector_alignment()
                if sector_aligned and direction == "bearish":
                    return True, "bearish"
                    
            return False, "neutral"
            
        except Exception as e:
            self.logger.error(f"Error checking VWAP reclaim/rejection: {e}")
            return False, "neutral"

    

    def check_data_synchronization(self):
        """
        Check if all data sources are synchronized
        
        Returns:
            bool: True if data is synchronized, False otherwise
        """
        try:
            # Check if sector data is available
            if not self.sector_prices or len(self.sector_prices) < 4:
                self.logger.warning("Sector data not synchronized")
                return False
                
            # Check timestamps of sector updates
            current_time = time.time()
            max_age_seconds = 60  # Maximum age of data (1 minute)
            
            for sector, status in self.sector_status.items():
                if sector not in self.sector_prices:
                    self.logger.warning(f"Missing price data for sector {sector}")
                    return Falsea
                    
                # Check if we have timestamp info
                if hasattr(self, '_sector_update_times') and sector in self._sector_update_times:
                    last_update = self._sector_update_times[sector]
                    if current_time - last_update > max_age_seconds:
                        self.logger.warning(f"Sector {sector} data is stale")
                        return False
            
            # Check ticker data for watched instruments
            for ticker in self.tickers[:2]:  # Check at least the main indices (SPY, QQQ)
                data_1m = self.get_price_data(ticker, "1m")
                if data_1m is None or len(data_1m) < 5:
                    self.logger.warning(f"Missing recent 1m data for {ticker}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking data synchronization: {e}")
            return False




    def _sync_positions_from_manager(self):
        """Sync positions from position manager to active_trades"""
        try:
            all_positions = self.position_manager.get_all_positions()
            
            # Clear and reload active trades
            self.active_trades = {}
            
            for symbol, position in all_positions.items():
                # Convert position manager format to active_trades format
                self.active_trades[symbol] = position
                
            self.logger.info(f"Synced {len(self.active_trades)} active positions from position manager")
            
            # Log position details
            for symbol, trade in self.active_trades.items():
                self.logger.info(f"Active position: {symbol} - {trade.get('type')} - Entry: {trade.get('entry_time')}")
                
        except Exception as e:
            self.logger.error(f"Error syncing positions from manager: {e}")


    def recover_positions_on_startup(self):
        """
        Recover positions from database on startup and sync with broker
        """
        try:
            self.logger.info("Recovering positions on startup...")
            
            # Get positions from broker
            if self.order_manager and self.order_manager.api:
                # Get account positions from broker
                broker_positions = self._get_broker_positions()
                
                # Sync with position manager
                if broker_positions:
                    self.position_manager.sync_with_broker(broker_positions)
                
            # Clean up any stale positions (older than 24 hours)
            self.position_manager.cleanup_stale_positions(24)
            
            # Sync to active_trades
            self._sync_positions_from_manager()
            
            self.logger.info(f"Position recovery complete. Active positions: {len(self.active_trades)}")
            
        except Exception as e:
            self.logger.error(f"Error recovering positions: {e}")
    
    def _get_broker_positions(self):
        """Get current positions from broker"""
        try:
            # This would call the broker API to get positions
            # For now, return empty list - implement based on your broker API
            return []
        except Exception as e:
            self.logger.error(f"Error getting broker positions: {e}")
            return []
        

    def sync_positions_with_broker(self):
        """Periodically sync positions with broker (call this every few minutes)"""
        try:
            # Get broker positions
            broker_positions = self._get_broker_positions()
            
            # Sync with position manager
            self.position_manager.sync_with_broker(broker_positions)
            
            # Update active_trades
            self._sync_positions_from_manager()
            
            # Export backup
            backup_path = os.path.join(
                os.path.dirname(__file__), 
                '..', '..', 
                'backups', 
                f'positions_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            self.position_manager.export_positions(backup_path)
            
        except Exception as e:
            self.logger.error(f"Error syncing positions: {e}")