# Code/bot_core/mag7_strategy.py

import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class Mag7Strategy:
    """
    Implementation of the Magnificent 7 trading strategy
    """
    
    def __init__(self, market_data_client, config=None):
        """
        Initialize the Mag7 strategy
        
        Args:
            market_data_client: MarketDataClient instance
            config (dict): Strategy configuration
        """
        self.market_data = market_data_client
        self.config = config or {}
        self.trading_config = self.config.get("trading_config", {})
        
        # Get Mag7 stocks from config instead of hardcoding
        self.mag7_stocks = self.trading_config.get("mag7_stocks", 
            ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
        
        # Track status of each Mag7 stock
        self.mag7_status = {}
        self.mag7_prices = {}
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"mag7_strategy_{today}.log")
        
        self.logger = logging.getLogger("Mag7Strategy")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def initialize(self):
        """Initialize the Mag7 strategy and subscribe to market data"""
        try:
            # Initialize status tracking
            for stock in self.mag7_stocks:
                self.mag7_status[stock] = "neutral"
                self.mag7_prices[stock] = 0.0
            
            # Subscribe to Mag7 stocks data
            self.subscribe_to_mag7_stocks()
            
            self.logger.info("Mag7 strategy initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing Mag7 strategy: {e}")
            return False
    
    def subscribe_to_mag7_stocks(self):
        """Subscribe to market data for Magnificent 7 stocks"""
        if not self.market_data:
            self.logger.error("No market data client available")
            return
        
        # Subscribe to all Mag7 stocks
        for stock in self.mag7_stocks:
            try:
                # Get streamer symbol
                streamer_symbol = stock  # For equities, usually same as symbol
                
                # Subscribe to quotes and trades
                self.market_data.subscribe(
                    [streamer_symbol],
                    event_types=["Quote", "Trade", "Summary"]
                )
                
                self.logger.info(f"Subscribed to market data for {stock}")
            except Exception as e:
                self.logger.error(f"Error subscribing to {stock}: {e}")
    
    def update_mag7_status(self, symbol, price):
        """
        Update status for a Mag7 stock
        
        Args:
            symbol (str): Stock symbol
            price (float): Current price
        """
        if symbol not in self.mag7_stocks:
            return
        
        # Get previous price
        prev_price = self.mag7_prices.get(symbol, price)
        
        # Calculate price change
        if prev_price > 0:
            pct_change = ((price - prev_price) / prev_price) * 100
            
            # Determine status based on price movement
            if pct_change > 0.1:  # 0.1% up
                status = "bullish"
            elif pct_change < -0.1:  # 0.1% down
                status = "bearish"
            else:
                status = "neutral"
        else:
            status = "neutral"
        
        # Update tracking
        self.mag7_status[symbol] = status
        self.mag7_prices[symbol] = price
        
        self.logger.debug(f"Updated {symbol}: price={price:.2f}, status={status}")
    
    def check_mag7_alignment(self) -> Tuple[bool, str, float]:
        """
        Check for Magnificent 7 alignment based on threshold
        
        Returns:
            tuple: (aligned, direction, percentage_aligned)
        """
        # Get threshold from config
        threshold_pct = float(self.trading_config.get("mag7_threshold", 60))
        
        # Count bullish and bearish stocks
        bullish_count = sum(1 for status in self.mag7_status.values() if status == "bullish")
        bearish_count = sum(1 for status in self.mag7_status.values() if status == "bearish")
        
        # Calculate percentages
        total_stocks = len(self.mag7_stocks)
        bullish_pct = (bullish_count / total_stocks) * 100
        bearish_pct = (bearish_count / total_stocks) * 100
        
        self.logger.info(f"Mag7 alignment: {bullish_count} bullish ({bullish_pct:.1f}%), "
                        f"{bearish_count} bearish ({bearish_pct:.1f}%), threshold={threshold_pct}%")
        
        # Check if we meet threshold
        if bullish_pct >= threshold_pct:
            return True, "bullish", bullish_pct
        elif bearish_pct >= threshold_pct:
            return True, "bearish", bearish_pct
        else:
            return False, "neutral", max(bullish_pct, bearish_pct)
    
    def get_mag7_stocks_by_status(self, status: str) -> List[str]:
        """
        Get list of Mag7 stocks with a specific status
        
        Args:
            status: "bullish", "bearish", or "neutral"
            
        Returns:
            List of symbols with that status
        """
        return [symbol for symbol, s in self.mag7_status.items() if s == status]
    
    def should_use_mag7(self) -> bool:
        """
        Check if Mag7 confirmation should be used based on config
        
        Returns:
            bool: True if Mag7 should be used instead of sectors
        """
        return self.trading_config.get("use_mag7_confirmation", False)
    
    def get_alignment_info(self) -> Dict:
        """
        Get detailed alignment information for logging/display
        
        Returns:
            dict: Alignment details
        """
        aligned, direction, percentage = self.check_mag7_alignment()
        
        return {
            "aligned": aligned,
            "direction": direction,
            "percentage": percentage,
            "threshold": float(self.trading_config.get("mag7_threshold", 60)),
            "bullish_stocks": self.get_mag7_stocks_by_status("bullish"),
            "bearish_stocks": self.get_mag7_stocks_by_status("bearish"),
            "neutral_stocks": self.get_mag7_stocks_by_status("neutral"),
            "prices": self.mag7_prices.copy()
        }
    
    def analyze_mag7_for_backtesting(self, mag7_data: Dict, idx: int) -> Tuple[bool, str, float]:
        """
        Analyze Mag7 alignment for backtesting using historical data
        
        Args:
            mag7_data: Dictionary of DataFrames for each Mag7 stock
            idx: Current index in the data
            
        Returns:
            tuple: (aligned, direction, percentage_aligned)
        """
        if not mag7_data or idx < 5:
            return False, "neutral", 0
        
        # Get threshold from config
        threshold_pct = float(self.trading_config.get("mag7_threshold", 60))
        
        # Analyze each stock
        stock_statuses = {}
        
        for symbol, df in mag7_data.items():
            if symbol not in self.mag7_stocks:
                continue
                
            if len(df) <= idx:
                continue
            
            # Get current and average price
            current_price = df.iloc[idx]['close']
            avg_5 = df.iloc[idx-5:idx]['close'].mean()
            
            # Determine status
            if current_price > avg_5 * 1.002:  # 0.2% above average
                stock_statuses[symbol] = "bullish"
            elif current_price < avg_5 * 0.998:  # 0.2% below average
                stock_statuses[symbol] = "bearish"
            else:
                stock_statuses[symbol] = "neutral"
        
        # Count statuses
        bullish_count = sum(1 for status in stock_statuses.values() if status == "bullish")
        bearish_count = sum(1 for status in stock_statuses.values() if status == "bearish")
        
        # Calculate percentages
        total_stocks = len(self.mag7_stocks)
        bullish_pct = (bullish_count / total_stocks) * 100
        bearish_pct = (bearish_count / total_stocks) * 100
        
        # Check alignment
        if bullish_pct >= threshold_pct:
            return True, "bullish", bullish_pct
        elif bearish_pct >= threshold_pct:
            return True, "bearish", bearish_pct
        else:
            return False, "neutral", max(bullish_pct, bearish_pct)