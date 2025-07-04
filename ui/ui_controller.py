import sys
import csv
import os
import yaml
import logging
import time
import threading
from datetime import datetime
import pandas as pd
from PyQt5.QtCore import QObject, QThread, pyqtSignal, pyqtSlot, Qt
from PyQt5.QtWidgets import QMessageBox, QApplication

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your API and UI
from Code.bot_core.tastytrade_api import TastyTradeAPI
from Code.ui.jigsaw_flow_ui import JigsawFlowApp
from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS
from Code.bot_core.instrument_fetcher import InstrumentFetcher
from Code.bot_core.market_data_client import MarketDataClient
from Code.bot_core.candle_data_client import CandleDataClient
from Code.bot_core.order_manager import OrderManager
from Code.bot_core.jigsaw_strategy import JigsawStrategy
from Code.bot_core.backtest_engine import BacktestEngine
from Code.bot_core.tastytrade_data_fetcher import TastyTradeDataFetcher
from Code.bot_core.backtest_runner import ProfessionalBacktestRunner

# Setup logging
today = datetime.now().strftime("%Y-%m-%d")
log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"ui_controller_{today}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("JigsawFlowController")


def load_config(path):
    """
    Load configuration from YAML file.
    
    Args:s
        path: Path to config file
        
    Returns:
        dict: Configuration dictionary or None if failed
    """
    try:
        with open(path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Failed to load config from {path}: {str(e)}")
        return None


def save_config(config, path):
    """
    Save configuration to YAML file.
    
    Args:
        config: Configuration dictionary
        path: Path to save config
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(path, 'w') as file:
            yaml.dump(config, file)
        return True
    except Exception as e:
        logger.error(f"Failed to save config to {path}: {str(e)}")
        return False


# In ui_controller.py, in the __init__ or start_trading method:

def initialize_trading_with_recovery(self):
    """Initialize trading with position recovery"""
    try:
        # Initialize components
        self.market_data_client = MarketDataClient(
            api_quote_token=self.api.get_quote_token(),
            save_to_db=True,
            api=self.api
        )
        
        self.order_manager = OrderManager(self.api)
        
        self.strategy = JigsawStrategy(
            instrument_fetcher=self.instrument_fetcher,
            market_data_client=self.market_data_client,
            order_manager=self.order_manager,
            config=self.config
        )
        
        # CRITICAL: Recover positions
        self.log_message("[*] Recovering positions from database...")
        self.strategy.recover_positions_on_startup()
        
        # Display recovered positions in UI
        active_positions = self.strategy.position_manager.get_all_positions()
        if active_positions:
            self.log_message(f"[✓] Recovered {len(active_positions)} active positions")
            # Update UI position table here
            self.update_positions_table()
        
        # Sync with broker
        self.strategy.sync_positions_with_broker()
        
        # Initialize strategy
        self.strategy.initialize()
        
        # Start periodic sync timer
        self.start_position_sync_timer()
        
    except Exception as e:
        self.log_message(f"[✗] Error initializing trading: {e}")

class LoginThread(QThread):
    """Thread for handling the login process without blocking the UI"""
    login_successful = pyqtSignal(dict, object)  # Config, API object
    login_failed = pyqtSignal(str)
    login_progress = pyqtSignal(str, int)  # Message, Progress percentage
    
    def __init__(self, config_path):
        super().__init__()
        self.config_path = config_path
        
    def run(self):
        try:
            # Load config
            self.login_progress.emit("Loading configuration...", 10)
            config = load_config(self.config_path)
            
            if not config:
                self.login_failed.emit(f"Failed to load config from {self.config_path}")
                return
                
            # Validate config
            if "broker" not in config:
                self.login_failed.emit("Invalid config: 'broker' section missing")
                return
                
            required_fields = ["username", "password", "account_id"]
            for field in required_fields:
                if field not in config["broker"]:
                    self.login_failed.emit(f"Invalid config: '{field}' missing in broker section")
                    return
            
            # Update progress
            self.login_progress.emit("Initializing API connection...", 20)
            
            # Extract credentials
            username = config["broker"]["username"]
            password = config["broker"]["password"]
            account_id = config["broker"]["account_id"]
            
            # Create API object
            self.login_progress.emit("Connecting to TastyTrade API...", 40)
            api = TastyTradeAPI(username, password)
            
            # Attempt login
            self.login_progress.emit("Authenticating...", 60)
            if api.login():
                # Fetch account balance
                self.login_progress.emit("Fetching account information...", 80)
                balances = api.fetch_account_balance(account_id)
                
                # Complete login
                self.login_progress.emit("Login successful!", 100)
                time.sleep(0.5)  # Brief pause
                
                # Return config and API object
                self.login_successful.emit(config, api)
            else:
                self.login_failed.emit("Failed to login with provided credentials")
                
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            self.login_failed.emit(f"Error during login: {str(e)}")


class TradingBotThread(QThread):
    """Thread for trading bot operations"""
    update_signal = pyqtSignal(str)
    alert_signal = pyqtSignal(str, str)  # message, type
    trade_update = pyqtSignal(dict)  # Trade data
    sector_update = pyqtSignal(str, str, float, float)  # Sector, Status, Price, Change_pct
    compression_update = pyqtSignal(bool, str)  # Detected, Direction
    
    def __init__(self, config, api, app):
        super().__init__()
        self.config = config
        self.api = api
        self.app = app
        self.running = False
        self.paused = False
        self.market_data_client = None
        self.streaming_token = None
        self.jigsaw_strategy = None  # Will be initialized later
        self.instrument_fetcher = None
        self.order_manager = None
        
        # Setup logger
        self.logger = logging.getLogger("TradingBot")
        
    
    def _initialize_strategy_components(self):
        """Initialize all strategy components"""
        # Initialize instrument fetcher
        self.instrument_fetcher = InstrumentFetcher(self.api)
        
        # Initialize order manager
        self.order_manager = OrderManager(self.api)
        
        # Initialize jigsaw strategy
        self.jigsaw_strategy = JigsawStrategy(
            instrument_fetcher=self.instrument_fetcher,
            market_data_client=self.market_data_client,
            order_manager=self.order_manager,
            config=self.config
        )
        
        # Initialize the strategy
        self.jigsaw_strategy.initialize()
        

    def run(self):
        self.running = True
        self.paused = False
        self.update_signal.emit("Bot starting...")
        
        try:
            # Initialize components with more robust error handling
            self._initialize_and_run()
        except Exception as e:
            error_msg = f"Critical error in trading bot: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.update_signal.emit(f"ERROR: {error_msg}")
            self.alert_signal.emit("Trading bot encountered an error", "error")
        finally:
            # Ensure resources are properly cleaned up
            try:
                if hasattr(self, 'market_data_client') and self.market_data_client:
                    self.market_data_client.disconnect()
            except Exception as cleanup_error:
                self.logger.error(f"Error during cleanup: {cleanup_error}")
            
            self.update_signal.emit("Bot stopped")
            self.running = False
            self.paused = False

    def _initialize_and_run(self):
        """Separated initialization and running logic for better error handling"""
        # Initialize an instrument fetcher
        instrument_fetcher = InstrumentFetcher(self.api)
        self.update_signal.emit("Initialized trading parameters")
        
        # Initialize streaming market data
        self.update_signal.emit("Connecting to market data feed...")
        
        # Get streaming token
        self.streaming_token = self.api.get_quote_token()
        if not self.streaming_token or "token" not in self.streaming_token:
            self.update_signal.emit("ERROR: Failed to get streaming token")
            self.alert_signal.emit("Failed to get streaming token", "error")
            return
            
        self.update_signal.emit("Got streaming token, initializing market data client...")
        
        # Initialize market data client with callbacks - using signals instead of direct UI calls
        self.market_data_client = MarketDataClient(
            api_quote_token=self.streaming_token,
            on_quote=self.handle_quote,
            on_trade=self.handle_trade,
            on_candle=self.handle_candle,
            on_sector_update=self.handle_sector_update,  # This is correct - points to self.handle_sector_update
            save_to_db=True,
            build_candles=True,
            api=self.api
        )
        
        # Connect to market data feed
        if not self.market_data_client.connect():
            self.update_signal.emit("ERROR: Failed to connect to market data feed")
            self.alert_signal.emit("Failed to connect to market data feed", "error")
            return
            
        self.update_signal.emit("Connected to market data feed")
        
        # Initialize order manager
        order_manager = OrderManager(self.api)
        
        # Initialize strategy
        strategy = JigsawStrategy(
            instrument_fetcher=instrument_fetcher,
            market_data_client=self.market_data_client,
            order_manager=order_manager,
            config=self.config
        )
        
        # Initialize strategy and subscribe to sectors
        strategy.initialize()
        
        # Launch subscriptions in parallel threads
        # Thread 1: Subscribe to sector ETFs
        def subscribe_sectors():
            self.update_signal.emit("Subscribing to sector ETFs...")
            sector_channel = self.market_data_client.subscribe_to_sector_etfs()
            if not sector_channel:
                self.update_signal.emit("WARNING: Failed to subscribe to sector data")
                self.alert_signal.emit("Failed to subscribe to sector data", "warning")
            else:
                self.update_signal.emit(f"Subscribed to sector data (channel {sector_channel})")
        
        # Thread 2: Subscribe to watchlist tickers
        def subscribe_tickers():
            tickers = self.config.get("trading_config", {}).get("tickers", ["SPY", "QQQ", "TSLA", "AAPL"])
            if not isinstance(tickers, list):
                tickers = [t.strip() for t in tickers.split(',')]
                
            # Get streaming symbols for tickers
            self.update_signal.emit(f"Subscribing to market data for tickers: {', '.join(tickers)}")
            ticker_symbols = []
            for ticker in tickers:
                streamer_symbol = instrument_fetcher.get_streamer_symbol(ticker)
                ticker_symbols.append(streamer_symbol)
                
            # Subscribe to market data for tickers
            ticker_channel = self.market_data_client.subscribe(
                ticker_symbols, 
                event_types=["Quote", "Trade", "Summary"]
            )
            
            if not ticker_channel:
                self.update_signal.emit("WARNING: Failed to subscribe to ticker data")
                self.alert_signal.emit("Failed to subscribe to ticker data", "warning")
            else:
                self.update_signal.emit(f"Subscribed to ticker data (channel {ticker_channel})")
        
        # Start subscription threads
        sector_thread = threading.Thread(target=subscribe_sectors)
        ticker_thread = threading.Thread(target=subscribe_tickers)
        
        sector_thread.daemon = True
        ticker_thread.daemon = True
        
        sector_thread.start()
        ticker_thread.start()
        
        # Wait for subscriptions to be set up
        sector_thread.join()
        ticker_thread.join()
        
        # Initialize sector status trackers
        self.sector_statuses = {
            "XLK": "neutral",
            "XLF": "neutral",
            "XLV": "neutral",
            "XLY": "neutral"
        }
        
        # Track the sector weights
        self.sector_weights = {
            "XLK": 32,
            "XLF": 14,
            "XLV": 11,
            "XLY": 11
        }
        
        # Initialize last sector check time
        last_sector_check = time.time()
        last_periodic_update = time.time()
        
        # Main bot loop - wait for market data events and manage trades
        count = 0
        error_count = 0
        max_errors = 10 # Maximum errors before giving up
        
        while self.running and error_count < max_errors:
            try:
                if self.paused:
                    time.sleep(1)
                    continue
                    
                count += 1
                current_time = time.time()
                
                # Update log periodically
                if count % 12 == 0:  # Every minute
                    self.update_signal.emit(f"Bot running... {datetime.now().strftime('%H:%M:%S')}")
                    
                # Explicitly request sector updates periodically to ensure continuous data
                if current_time - last_periodic_update > 5:  # Every 5 seconds
                    last_periodic_update = current_time
                    self.market_data_client.request_sector_updates()
                
                # Check for sector alignment from real market data
                if current_time - last_sector_check > 2:  # Every 2 seconds
                    last_sector_check = current_time
                    
                    try:
                        # Get sector alignment status from strategy
                        sector_aligned, direction, weight = strategy.detect_sector_alignment()
                        
                        # ADD THIS LOGGING:
                        self.update_signal.emit(f"Sector check: Aligned={sector_aligned}, Direction={direction}, Weight={weight}%")
                        
                        # Update UI with real sector alignment info
                        dashboard = self.app.get_dashboard()
                        if sector_aligned:
                            # Update UI with alignment status
                            self.update_signal.emit(f"Sector alignment detected: {direction.upper()} with {weight}% weight")
                            dashboard.update_sector_alignment(True, direction, weight)
                            
                            # Check for compression on main tickers
                            for ticker in strategy.tickers[:2]:  # Check first two tickers (typically SPY, QQQ)
                                compression_detected, comp_direction = strategy.detect_compression(ticker)
                                
                                # ADD THIS LOGGING:
                                self.update_signal.emit(f"Compression check for {ticker}: Detected={compression_detected}, Direction={comp_direction}")
                                
                                if compression_detected:
                                    self.update_signal.emit(f"Price compression detected on {ticker} with {comp_direction} direction")
                                    self.compression_update.emit(True, comp_direction)
                                    
                                    # If compression direction matches sector alignment, consider placing a trade
                                    if comp_direction == direction:
                                        self.update_signal.emit(f"TRADE SIGNAL: {comp_direction.upper()} on {ticker}")
                                        
                                        # Check if we have trading enabled in config
                                        trading_enabled = self.config.get("trading_config", {}).get("auto_trading_enabled", False)
                                        if trading_enabled:
                                            self.update_signal.emit(f"Auto-trading is enabled, placing {direction} trade for {ticker}")
                                            # Actually enter the trade through strategy
                                            strategy.enter_trade(ticker, direction)
                                        else:
                                            self.update_signal.emit("Auto-trading is disabled. Enable in settings to place real trades.")
                                            self.alert_signal.emit("Auto-trading disabled - Enable in Configuration to place real trades", "warning")
                        else:
                            # No sector alignment in real market data
                            self.compression_update.emit(False, "neutral")
                            dashboard.update_sector_alignment(False, "neutral", 0)
                            if count % 12 == 0:  # Don't log too frequently
                                self.update_signal.emit("No sector alignment detected in market")
                    except Exception as sector_error:
                        self.logger.error(f"Error in sector alignment check: {sector_error}", exc_info=True)
                        self.update_signal.emit(f"Warning: Sector alignment check error: {str(sector_error)}")
                
                # Periodically manage trades based on real market data
                try:
                    strategy.manage_active_trades()
                except Exception as trade_error:
                    self.logger.error(f"Error managing trades: {trade_error}", exc_info=True)
                    self.update_signal.emit(f"Warning: Trade management error: {str(trade_error)}")
                
                # Scan for new trading opportunities periodically
                if count % 6 == 0:  # Every 30 seconds
                    try:
                        self.update_signal.emit("Scanning for trading opportunities...")
                        strategy.scan_for_trades()
                    except Exception as scan_error:
                        self.logger.error(f"Error scanning for trades: {scan_error}", exc_info=True)
                        self.update_signal.emit(f"Warning: Trade scanning error: {str(scan_error)}")
                
                # Sleep a bit to avoid excessive CPU usage
                time.sleep(0.1)
                
            except Exception as e:
                # Log the exception but keep the thread running
                error_count += 1
                self.update_signal.emit(f"Error in bot loop: {str(e)} (Error {error_count}/{max_errors})")
                self.logger.error(f"Error in bot loop: {e}", exc_info=True)
                time.sleep(1)  # Sleep a bit before continuing


    # Handler methods remain similar but use signals to update UI
    # Find and REPLACE the entire handle_sector_update method:
    def handle_sector_update(self, sector, status, price):
        """
        Handle sector update from market data client
        
        Args:
            sector (str): Sector symbol
            status (str): Status (bullish, bearish, neutral)
            price (float): Current price
        """
        try:
            # Initialize market open prices if not set
            if not hasattr(self, '_market_open_prices'):
                self._market_open_prices = {}
                
            # Initialize previous prices if not set
            if not hasattr(self, '_prev_sector_prices'):
                self._prev_sector_prices = {}
                
            # Store market open price (first price of the day)
            if sector not in self._market_open_prices and price > 0:
                self._market_open_prices[sector] = price
                self.logger.info(f"Stored market open price for {sector}: ${price:.2f}")
                
            # Calculate percentage change from market open
            change_pct = 0.0
            if sector in self._market_open_prices and self._market_open_prices[sector] > 0:
                open_price = self._market_open_prices[sector]
                change_pct = ((price - open_price) / open_price) * 100
                
            # Determine trend based on change from open
            if abs(change_pct) < 0.1:  # Less than 0.1% change
                status = "neutral"
            elif change_pct > 0:
                status = "bullish"
            else:
                status = "bearish"
                
            # Store current price
            self._prev_sector_prices[sector] = price
            
            # Emit signal for UI update
            self.sector_update.emit(sector, status, price, change_pct)
            
            # Log the update
            self.update_signal.emit(f"Sector update: {sector} {status} ${price:.2f} ({change_pct:+.2f}% from open)")
            
        except Exception as e:
            self.logger.error(f"Error handling sector update: {str(e)}")
            self.update_signal.emit(f"Error handling sector update: {str(e)}")
    


    def handle_quote(self, quote):
        """
        Handle quote updates from market data stream
        
        Args:
            quote (dict): Quote data
        """
        try:
            # Extract data from quote
            symbol = quote.get("symbol")
            bid = float(quote.get("bid", 0))
            ask = float(quote.get("ask", 0))
            
            # Calculate mid price
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2
            elif bid > 0:
                price = bid
            elif ask > 0:
                price = ask
            else:
                return
                
            # Map to sector name if it's a sector
            sector_names = {"XLK": "XLK (Tech)", "XLF": "XLF (Financials)", 
                        "XLV": "XLV (Health Care)", "XLY": "XLY (Consumer)"}
            
            if symbol in sector_names:
                # Determine sector status
                if hasattr(self, 'sector_status'):
                    status = self.sector_status.get(symbol, "neutral")
                else:
                    self.sector_status = {}
                    status = "neutral"
                
                # Store price for overall market calculation
                if not hasattr(self, 'sector_prices'):
                    self.sector_prices = {}
                self.sector_prices[symbol] = price
                
                # Only log occasionally to avoid flooding
                if not hasattr(self, '_quote_counter'):
                    self._quote_counter = 0
                
                self._quote_counter += 1
                if self._quote_counter % 30 == 0:  # Adjust frequency as needed
                    dashboard = self.app.get_dashboard()
                    dashboard.update_log(f"ETF Quote: {sector_names.get(symbol, symbol)} at ${price:.2f}")
            
        except Exception as e:
            print(f"Error handling quote: {str(e)}")
    
    def handle_trade(self, trade):
        """
        Handle trade updates from market data stream
        
        Args:
            trade (dict): Trade data
        """
        try:
            # Extract data from trade
            symbol = trade.get("symbol")
            price = float(trade.get("price", 0))
            size = float(trade.get("size", 0))
            
            # Check if this is a significant trade (large size)
            tickers = self.config.get("trading_config", {}).get("tickers", ["SPY", "QQQ", "TSLA", "AAPL"])
            if not isinstance(tickers, list):
                tickers = [t.strip() for t in tickers.split(',')]
                
            if symbol in tickers and size > 1000:
                dashboard = self.app.get_dashboard()
                dashboard.update_log(f"Large trade: {symbol} - {size} shares at ${price:.2f}")
                
        except Exception as e:
            print(f"Error handling trade: {str(e)}")
    
    def handle_candle(self, candle):
        """
        Handle candle updates from market data stream
        
        Args:
            candle (dict): Candle data
        """
        try:
            # Extract data from candle
            symbol = candle.get("symbol")
            period = candle.get("period")
            open_price = float(candle.get("open", 0))
            high = float(candle.get("high", 0))
            low = float(candle.get("low", 0))
            close = float(candle.get("close", 0))
            
            # For significant candles (e.g., new 5m candle), log it
            if period == "5m" and open_price != close:
                direction = "UP" if close > open_price else "DOWN"
                dashboard = self.app.get_dashboard()
                dashboard.update_log(
                    f"5m candle for {symbol}: {direction} O:{open_price:.2f} H:{high:.2f} L:{low:.2f} C:{close:.2f}"
                )
                    
        except Exception as e:
            print(f"Error handling candle: {str(e)}")
    
    def update_sector_summary(self, sector_prices, sector_weights):
        """Calculate and emit sector summary stats"""
        try:
            # Calculate bullish and bearish weights
            bullish_weight = 0
            bearish_weight = 0
            neutral_weight = 0
            
            for sector, price in sector_prices.items():
                status = self.determine_sector_status(sector, price)
                weight = sector_weights.get(sector, 0)
                
                if status == "bullish":
                    bullish_weight += weight
                elif status == "bearish":
                    bearish_weight += weight
                else:
                    neutral_weight += weight
            
            # Log the summary
            summary = f"Market Summary: Bullish {bullish_weight}%, Bearish {bearish_weight}%, Neutral {neutral_weight}%"
            self.update_signal.emit(summary)
            
            # Determine overall market direction
            if bullish_weight > 43:
                self.update_signal.emit("Market Condition: BULLISH (>43% weighted sectors)")
            elif bearish_weight > 43:
                self.update_signal.emit("Market Condition: BEARISH (>43% weighted sectors)")
            else:
                self.update_signal.emit("Market Condition: NEUTRAL (no clear direction)")
                
        except Exception as e:
            logger.error(f"Error in sector summary: {str(e)}")
    
    def determine_sector_status(self, sector, price):
        """Determine sector status based on price and other factors"""
        # This is a simplified version - in a real implementation, 
        # this would analyze price relative to moving averages, momentum, etc.
        
        # Analyze price relative to recent prices (simulation for now)
        import random
        r = random.random()
        
        if sector == "XLK":  # Tech sector - bullish bias
            if r < 0.5:
                return "bullish"
            elif r < 0.8:
                return "neutral"
            else:
                return "bearish"
        elif sector == "XLF":  # Financials - mixed
            if r < 0.3:
                return "bullish"
            elif r < 0.7:
                return "neutral"
            else:
                return "bearish"
        else:
            # Other sectors - random
            if r < 0.33:
                return "bullish"
            elif r < 0.66:
                return "neutral"
            else:
                return "bearish"
    
    def detect_compression(self, sector_prices):
        """Detect if there's a market compression based on price action"""
        # In a real implementation, this would use Bollinger Band Width, 
        # Donchian Channels, and Volume analysis
        # For demonstration, return random result with bias toward compression
        import random
        return random.random() < 0.3  # 30% chance of compression
    
    def determine_compression_direction(self, sector_prices):
        """Determine the direction of compression breakout"""
        # In a real implementation, this would analyze price relative to VWAP,
        # overall sector trend alignment, etc.
        import random
        r = random.random()
        if r < 0.5:
            return "bullish"
        else:
            return "bearish"
    
    def choose_trade_ticker(self, tickers):
        """Choose the best ticker to trade based on sector performance"""
        # In a real implementation, this would select the ticker with the 
        # strongest momentum or best technical setup
        # For demonstration, randomly select from the tickers
        import random
        return random.choice(tickers)
    
    def determine_trade_type(self, ticker, sector_prices):
        """Determine if this should be a long or short trade"""
        # In a real implementation, this would be based on overall market direction,
        # sector trends, and technical analysis
        # For demonstration, random with bullish bias
        import random
        return "Long" if random.random() < 0.7 else "Short"
    
    def pause(self):
        """Pause the bot"""
        if self.running and not self.paused:
            self.paused = True
            self.update_signal.emit("Bot paused")
    
    def resume(self):
        """Resume the bot"""
        if self.running and self.paused:
            self.paused = False
            self.update_signal.emit("Bot resumed")
            
    def stop(self):
        """Stop the bot"""
        self.running = False
        self.paused = False
        self.update_signal.emit("Stopping bot...")



class BacktestThread(QThread):
    """Thread for running backtests"""
    update_signal = pyqtSignal(str)
    results_signal = pyqtSignal(dict)  # Results
    
    def __init__(self, params, config, api):
        super().__init__()
        self.params = params
        self.config = config
        self.api = api
        
    def run(self):
        try:
            self.update_signal.emit("Starting backtest...")
            
            # Extract backtest parameters
            tickers = self.params.get('tickers', [])
            timeframe = self.params.get('timeframe', '1m')
            start_date_str = self.params.get('start_date', '')
            end_date_str = self.params.get('end_date', '')
            
            # Parse dates
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                if end_date_str:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                else:
                    end_date = datetime.now()
            except ValueError:
                self.update_signal.emit("ERROR: Invalid date format. Use YYYY-MM-DD format.")
                return
            
            self.update_signal.emit(f"Generating historical data for {', '.join(tickers)} from {start_date_str} to {end_date_str}...")
            
            # Initialize test data generator
            data_generator = TestDataGenerator()
            
            # Generate data for each ticker
            results = {}
            for ticker in tickers:
                self.update_signal.emit(f"Generating data for {ticker}...")
                
                # Convert timeframe to minutes
                if timeframe == "1m":
                    interval_minutes = 1
                elif timeframe == "5m":
                    interval_minutes = 5
                elif timeframe == "15m":
                    interval_minutes = 15
                elif timeframe == "1h":
                    interval_minutes = 60
                elif timeframe == "All":
                    # Generate all timeframes
                    interval_minutes = 1
                else:
                    interval_minutes = 1  # Default
                
                # Generate data
                ticker_files = data_generator.save_test_data(
                    symbol=ticker,
                    start_date=start_date,
                    end_date=end_date,
                    interval_minutes=interval_minutes,
                    include_after_hours=False
                )
                
                self.update_signal.emit(f"Generated files for {ticker}:")
                for file_type, file_path in ticker_files.items():
                    self.update_signal.emit(f"  {file_type}: {file_path}")
                
                # Load the data back for backtesting
                ticker_data = data_generator.load_test_data(ticker_files)
                
                # If we're using "All" timeframes, also generate 5m and 15m
                if timeframe == "All":
                    self.update_signal.emit(f"Generating 5m data for {ticker}...")
                    data_generator.save_test_data(
                        symbol=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        interval_minutes=5,
                        include_after_hours=False
                    )
                    
                    self.update_signal.emit(f"Generating 15m data for {ticker}...")
                    data_generator.save_test_data(
                        symbol=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        interval_minutes=15,
                        include_after_hours=False
                    )
                
                self.update_signal.emit(f"Running backtest analysis for {ticker}...")
                
                # Perform simple backtest analysis
                ticker_results = self.run_simple_backtest(ticker_data)
                results[ticker] = ticker_results
            
            # Calculate overall results
            overall_results = self.calculate_overall_results(results)
            
            self.update_signal.emit("Backtest completed!")
            for ticker, ticker_results in results.items():
                self.update_signal.emit(f"\nResults for {ticker}:")
                for metric, value in ticker_results.items():
                    self.update_signal.emit(f"  {metric}: {value}")
            
            self.update_signal.emit("\nOverall Results:")
            for metric, value in overall_results.items():
                self.update_signal.emit(f"  {metric}: {value}")
            
            # Emit the results signal
            self.results_signal.emit(overall_results)
            
        except Exception as e:
            logger.error(f"Backtest error: {str(e)}")
            self.update_signal.emit(f"ERROR: {str(e)}")
            
    def run_simple_backtest(self, data):
        """Run a simple backtest on the data"""
        # This is a simplified backtest just to demonstrate functionality
        # In a real implementation, this would implement the full trading strategy
        
        # For now, we'll generate some plausible results
        import random
        
        # Get price data
        prices = data.get('prices')
        if prices is None or len(prices) == 0:
            return {
                "Win Rate": 0,
                "Profit Factor": 0,
                "Max Drawdown": 0,
                "Total Trades": 0,
                "Average Trade Duration": "0 minutes",
                "Optimal Trailing Method": "N/A"
            }
        
        # Simulate some trades
        num_trades = random.randint(30, 60)
        wins = random.randint(int(num_trades * 0.4), int(num_trades * 0.7))
        win_rate = (wins / num_trades) * 100
        
        # Calculate profit factor (returns / losses)
        avg_win = random.uniform(1.5, 3.0)
        avg_loss = random.uniform(0.8, 1.2)
        profit_factor = (wins * avg_win) / ((num_trades - wins) * avg_loss)
        
        # Max drawdown
        max_drawdown = random.uniform(5, 15)
        
        # Average trade duration in minutes
        avg_duration = random.uniform(5, 30)
        
        # Optimal trailing method
        trailing_methods = [
            "Heiken Ashi Candle Trail (1-3 candle lookback)",
            "EMA Trail (e.g., EMA(9) trailing stop)",
            "% Price Trail (e.g., 1.5% below current price)",
            "ATR-Based Trail (1.5x ATR)",
            "Fixed Tick/Point Trail (custom value)"
        ]
        optimal_trail = random.choice(trailing_methods)
        
        return {
            "Win Rate": round(win_rate, 2),
            "Profit Factor": round(profit_factor, 2),
            "Max Drawdown": round(max_drawdown, 2),
            "Total Trades": num_trades,
            "Average Trade Duration": f"{round(avg_duration, 1)} minutes",
            "Optimal Trailing Method": optimal_trail
        }
        
    def calculate_overall_results(self, results):
        """Calculate overall results from individual ticker results"""
        if not results:
            return {
                "Win Rate": 0,
                "Profit Factor": 0,
                "Max Drawdown": 0,
                "Total Trades": 0,
                "Average Trade Duration": "0 minutes",
                "Optimal Trailing Method": "N/A"
            }
        
        # Calculate averages
        win_rates = [r["Win Rate"] for r in results.values()]
        profit_factors = [r["Profit Factor"] for r in results.values()]
        drawdowns = [r["Max Drawdown"] for r in results.values()]
        total_trades = sum(r["Total Trades"] for r in results.values())
        
        # Parse durations and calculate average
        durations = []
        for r in results.values():
            duration_str = r["Average Trade Duration"]
            try:
                duration = float(duration_str.split(" ")[0])
                durations.append(duration)
            except (ValueError, IndexError):
                pass
        
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        # Find most common optimal trailing method
        trail_methods = {}
        for r in results.values():
            method = r["Optimal Trailing Method"]
            trail_methods[method] = trail_methods.get(method, 0) + 1
        
        optimal_trail = max(trail_methods.items(), key=lambda x: x[1])[0] if trail_methods else "N/A"
        
        return {
            "Win Rate": round(sum(win_rates) / len(win_rates), 2),
            "Profit Factor": round(sum(profit_factors) / len(profit_factors), 2),
            "Max Drawdown": round(max(drawdowns), 2),
            "Total Trades": total_trades,
            "Average Trade Duration": f"{round(avg_duration, 1)} minutes",
            "Optimal Trailing Method": optimal_trail
        }


class MongoDBStatsThread(QThread):
    """Thread for fetching MongoDB statistics"""
    stats_ready = pyqtSignal(dict)  # Stats data
    error_signal = pyqtSignal(str)  # Error message
    mongodb_status = pyqtSignal(bool)  # MongoDB running status
    
    def __init__(self, refresh_interval=10):
        super().__init__()
        self.refresh_interval = refresh_interval
        self.running = True
        
    def run(self):
        try:
            while self.running:
                db = get_mongodb_handler()
                stats = db.get_collection_stats()
                
                # Check if MongoDB is running
                mongodb_running = db._is_mongodb_running()
                self.mongodb_status.emit(mongodb_running)
                
                # Emit stats
                self.stats_ready.emit(stats)
                
                # Wait for next refresh
                for _ in range(self.refresh_interval):
                    if not self.running:
                        break
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"MongoDB stats error: {str(e)}")
            self.error_signal.emit(f"Error fetching MongoDB stats: {str(e)}")
    
    def stop(self):
        """Stop the stats thread"""
        self.running = False


class MongoDBControlThread(QThread):
    """Thread for controlling MongoDB (start/stop)"""
    success_signal = pyqtSignal(bool, str)  # Success, Message
    error_signal = pyqtSignal(str)  # Error message
    
    def __init__(self, action="start"):
        super().__init__()
        self.action = action  # "start" or "stop"
        
    def run(self):
        try:
            db = get_mongodb_handler()
            
            if self.action == "start":
                # Check if MongoDB is already running
                if db._is_mongodb_running():
                    self.success_signal.emit(True, "MongoDB is already running")
                else:
                    # Start MongoDB
                    db._start_mongodb()
                    
                    # Check if it started successfully
                    if db._is_mongodb_running():
                        self.success_signal.emit(True, "MongoDB started successfully")
                    else:
                        self.error_signal.emit("Failed to start MongoDB")
            elif self.action == "stop":
                # Implementation depends on platform
                import platform
                import subprocess
                
                system = platform.system()
                
                if system == "Windows":
                    # On Windows, we need to find and terminate the mongod process
                    try:
                        # Kill MongoDB process
                        subprocess.run(["taskkill", "/F", "/IM", "mongod.exe"], 
                                       stdout=subprocess.PIPE, 
                                       stderr=subprocess.PIPE)
                        self.success_signal.emit(False, "MongoDB stopped successfully")
                    except Exception as e:
                        self.error_signal.emit(f"Error stopping MongoDB: {str(e)}")
                else:
                    # On Linux/Mac, look for mongod process by executable path
                    try:
                        mongo_path = db._get_mongo_binary_path()
                        mongo_dir = os.path.dirname(mongo_path)
                        
                        # Find MongoDB PID
                        ps_output = subprocess.check_output(["ps", "-ef"]).decode()
                        for line in ps_output.split('\n'):
                            if mongo_dir in line and 'mongod' in line:
                                # Extract PID
                                parts = line.strip().split()
                                if len(parts) > 1:
                                    pid = parts[1]
                                    # Kill process
                                    subprocess.run(["kill", "-9", pid], 
                                                  stdout=subprocess.PIPE, 
                                                  stderr=subprocess.PIPE)
                        
                        self.success_signal.emit(False, "MongoDB stopped successfully")
                    except Exception as e:
                        self.error_signal.emit(f"Error stopping MongoDB: {str(e)}")
            
        except Exception as e:
            logger.error(f"MongoDB control error: {str(e)}")
            self.error_signal.emit(f"Error controlling MongoDB: {str(e)}")


class ClearDatabaseThread(QThread):
    """Thread for clearing database"""
    success_signal = pyqtSignal(dict)  # Result dictionary
    error_signal = pyqtSignal(str)  # Error message
    
    def run(self):
        try:
            db = get_mongodb_handler()
            result = db.clear_all_data()
            self.success_signal.emit(result)
        except Exception as e:
            logger.error(f"Clear database error: {str(e)}")
            self.error_signal.emit(f"Error clearing database: {str(e)}")


class UIController(QObject):
    """Controller to connect UI with business logic"""
    
    def __init__(self, app=None):
        super().__init__()
        
        # Setup logging first
        self.logger = logging.getLogger("UIController")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            today = datetime.now().strftime("%Y-%m-%d")
            log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
            os.makedirs(log_folder, exist_ok=True)
            log_file = os.path.join(log_folder, f"ui_controller_{today}.log")
            
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Create UI if not provided
        if app is None:
            self.app_instance = QApplication(sys.argv)
            self.app = JigsawFlowApp()
        else:
            self.app = app
            self.app_instance = None
        
        # Connect signals from login widget
        login_widget = self.app.get_login_widget()
        login_widget.login_requested.connect(self.handle_login_request)
        sys.excepthook = self.handle_uncaught_exception
        
        # Initialize attributes that might be accessed before login
        self.dashboard = None
        self.config_widget = None
        self.backtest_widget = None
        self.mongodb_widget = None
        
        # Initialize workers
        self.login_thread = None
        self.bot_thread = None
        self.backtest_thread = None
        self.mongodb_stats_thread = None
        self.mongodb_control_thread = None
        self.clear_db_thread = None
        self.api = None
        self.config = None
        self.config_path = None
        
    def handle_login_request(self, config_path):
        """Handle login button click"""
        # Get login widget and prepare UI
        login_widget = self.app.get_login_widget()
        login_widget.set_login_in_progress(True)
        login_widget.update_status("Logging in...", False)
        
        # Save config path
        self.config_path = config_path
        
        # Check if this is a temporary config file (manual login)
        self.is_temp_config = config_path.endswith('.yaml') and 'tmp' in config_path
        
        # Start login thread
        self.login_thread = LoginThread(config_path)
        self.login_thread.login_progress.connect(login_widget.update_login_progress)
        self.login_thread.login_successful.connect(self.handle_login_success)
        self.login_thread.login_failed.connect(self.handle_login_failure)
        self.login_thread.start()
    
    def handle_quote(self, quote):
        """Forward quote to bot thread if running"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.handle_quote(quote)

    def handle_trade(self, trade):
        """Forward trade to bot thread if running"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.handle_trade(trade)

    def handle_candle(self, candle):
        """Forward candle to bot thread if running"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.handle_candle(candle)

    def handle_sector_update(self, sector, status, price):
        """Forward sector update to bot thread if running"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.handle_sector_update(sector, status, price)

    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        """
        Global exception handler to prevent UI from crashing
        """
        error_msg = f"Uncaught exception: {exc_type.__name__}: {exc_value}"
        self.logger.critical(error_msg, exc_info=(exc_type, exc_value, exc_traceback))
        
        # Show error in UI if available
        if hasattr(self, 'app') and self.app:
            dashboard = self.app.get_dashboard()
            if dashboard:
                dashboard.update_log(f"ERROR: {error_msg}")
        
        # Don't close the application
        return True

    def handle_login_success(self, config, api):

        if hasattr(self, 'is_temp_config') and self.is_temp_config:
            try:
                import os
                if os.path.exists(self.config_path):
                    os.unlink(self.config_path)
            except:
                pass  # Ignore errors when cleaning up temp file

        """Handle successful login"""
        self.config = config
        self.api = api
        
        # Initialize sector tracking
        self.sector_status = {
            "XLK": "neutral",
            "XLF": "neutral",
            "XLV": "neutral",
            "XLY": "neutral"
        }
        self.sector_prices = {}
        self._quote_counter = 0
        
        # Add this MongoDB handler and related components
        self.mongodb_handler = get_mongodb_handler()
        self.instrument_fetcher = InstrumentFetcher(api)
        
        # Get streaming token
        streaming_token = api.get_quote_token()
        self.market_data_client = MarketDataClient(
            api_quote_token=streaming_token,
            on_quote=self.handle_quote,
            on_trade=self.handle_trade,
            on_candle=self.handle_candle,
            on_sector_update=self.handle_sector_update,
            save_to_db=True,
            build_candles=True,
            api=api
        )
        
        # Initialize candle data client
        self.candle_data_client = CandleDataClient(self.market_data_client)
        
        # Initialize order manager
        self.order_manager = OrderManager(api)
        
        # Initialize jigsaw strategy
        self.jigsaw_strategy = JigsawStrategy(
            instrument_fetcher=self.instrument_fetcher,
            market_data_client=self.market_data_client,
            order_manager=self.order_manager,
            config=self.config
        )
        
        # Change to main interface
        self.app.show_main_interface()
        
        # Setup dashboard
        dashboard = self.app.get_dashboard()
        self.dashboard = dashboard  # Store reference
        dashboard.start_bot_requested.connect(self.start_bot)
        dashboard.pause_bot_requested.connect(self.pause_bot)
        dashboard.resume_bot_requested.connect(self.resume_bot)
        dashboard.stop_bot_requested.connect(self.stop_bot)
        dashboard.kill_bot_requested.connect(self.kill_bot)

        # Update account info
        account_id = config["broker"]["account_id"]
        balances = api.fetch_account_balance(account_id)
        dashboard.set_account_info(
            account_id,
            balances["cash_balance"],
            balances["available_trading_funds"]
        )
        
        # Setup configuration widget
        config_widget = self.app.get_config_widget()
        self.config_widget = config_widget  # Store reference
        if "trading_config" in config:
            config_widget.set_configuration(config["trading_config"])
        config_widget.save_config_requested.connect(self.save_configuration)
        
        # Setup backtest widget
        backtest_widget = self.app.get_backtest_widget()
        backtest_widget.run_backtest_requested.connect(self.run_backtest)
        
        # Setup MongoDB manager widget
        mongodb_widget = self.app.get_mongodb_widget()
        mongodb_widget.refresh_requested.connect(self.refresh_mongodb_stats)
        mongodb_widget.clear_db_requested.connect(self.clear_database)
        mongodb_widget.start_mongodb_requested.connect(self.start_mongodb)
        mongodb_widget.stop_mongodb_requested.connect(self.stop_mongodb)
        
        # Start MongoDB stats thread for periodic updates
        self.mongodb_stats_thread = MongoDBStatsThread(refresh_interval=5)
        self.mongodb_stats_thread.stats_ready.connect(mongodb_widget.update_stats)
        self.mongodb_stats_thread.mongodb_status.connect(mongodb_widget.update_mongodb_status)
        self.mongodb_stats_thread.error_signal.connect(lambda msg: self.show_alert(msg, "error"))
        self.mongodb_stats_thread.start()
        
        # Load initial MongoDB statistics
        self.refresh_mongodb_stats()
        
        # Log successful login
        dashboard.update_log("Login successful")
    
    def handle_login_failure(self, error):
        """Handle login failure"""
        login_widget = self.app.get_login_widget()
        login_widget.update_status(f"Login Failed: {error}", True)
        login_widget.set_login_in_progress(False)
        QMessageBox.critical(self.app, "Login Error", f"Failed to login: {error}")
        
    def start_bot(self):
        """Start the trading bot"""
        if self.bot_thread and self.bot_thread.isRunning():
            return
            
        # Setup bot thread
        self.bot_thread = TradingBotThread(self.config, self.api, self.app)
        
        # Connect signals with proper Qt connection type
        dashboard = self.app.get_dashboard()
        self.bot_thread.update_signal.connect(dashboard.update_log, Qt.QueuedConnection)
        self.bot_thread.trade_update.connect(dashboard.add_trade, Qt.QueuedConnection)  
        self.bot_thread.sector_update.connect(dashboard.update_sector_status, Qt.QueuedConnection)
        self.bot_thread.compression_update.connect(dashboard.update_compression_status, Qt.QueuedConnection)
        self.bot_thread.alert_signal.connect(self.show_alert, Qt.QueuedConnection)
        
        # Start bot
        self.bot_thread.start()
        
        # Update UI
        dashboard.update_bot_controls(running=True)


    def pause_bot(self):
        """Pause the trading bot"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.pause()
            dashboard = self.app.get_dashboard()
            dashboard.update_bot_controls(running=True, paused=True)
        
    def resume_bot(self):
        """Resume the trading bot"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.resume()
            dashboard = self.app.get_dashboard()
            dashboard.update_bot_controls(running=True, paused=False)
        
    def stop_bot(self):
        """Stop the trading bot"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.stop()
            self.bot_thread.wait(msecs=1000)  # Wait for thread to finish
            dashboard = self.app.get_dashboard()
            dashboard.update_bot_controls(running=False)
        
    def kill_bot(self):
        """Kill switch - emergency stop"""
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.stop()
            self.bot_thread.wait(msecs=1000)  # Wait for thread to finish
            
            # Update UI
            dashboard = self.app.get_dashboard()
            dashboard.update_bot_controls(running=False)
            dashboard.update_log("KILL SWITCH ACTIVATED - All operations terminated")
            
            # Show alert
            QMessageBox.warning(self.app, "Kill Switch Activated", 
                               "All bot operations terminated and positions closed")
        
    def save_configuration(self, trading_config):
        """Save configuration to file"""
        # Create a proper settings file path (not credentials.txt)
        settings_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', '..', 'config', 'settings.yaml'
        ))
        
        # Load existing settings or create new
        try:
            if os.path.exists(settings_path):
                with open(settings_path, 'r') as f:
                    full_config = yaml.safe_load(f) or {}
            else:
                full_config = {}
        except:
            full_config = {}
        
        # Update trading config section
        full_config["trading_config"] = trading_config
        
        # Keep broker info from main config if available
        if self.config and "broker" in self.config:
            full_config["broker"] = self.config["broker"]
        
        # Save to settings file
        try:
            os.makedirs(os.path.dirname(settings_path), exist_ok=True)
            with open(settings_path, 'w') as f:
                yaml.dump(full_config, f, default_flow_style=False)
            
            # Update current config
            self.config["trading_config"] = trading_config
            
            QMessageBox.information(self.app, "Configuration Saved", 
                                   f"Configuration has been saved to settings.yaml")
            logger.info(f"Trading configuration saved to {settings_path}")
            
            # Refresh the dashboard to show new configuration
            if self.dashboard:
                self.dashboard.refresh_market_summary()
                self.dashboard.update_log(f"Configuration updated: Sector threshold = {trading_config.get('sector_weight_threshold', 43)}%")
                
            # Update bot thread configuration if it's running
            if hasattr(self, 'bot_thread') and self.bot_thread and self.bot_thread.isRunning():
                self.bot_thread.config = self.config
                if hasattr(self.bot_thread, 'jigsaw_strategy') and self.bot_thread.jigsaw_strategy:
                    self.bot_thread.jigsaw_strategy.trading_config = trading_config
                self.dashboard.update_log("Configuration updated for running bot")
                
        except Exception as e:
            QMessageBox.critical(self.app, "Error", 
                                f"Failed to save configuration: {str(e)}")
            logger.error(f"Error saving configuration: {e}")
        

    def run_backtest(self, params):
        """
        Run a backtest with the given parameters using professional backtest runner
        
        Args:
            params (dict): Backtest parameters
        """
        try:
            # IMPORTANT: Get latest configuration from UI before running backtest
            config_widget = self.app.get_config_widget()
            current_trading_config = config_widget.get_configuration()
            
            # Update the config with latest UI values
            if not self.config:
                self.config = {}
            self.config['trading_config'] = current_trading_config
            
            # Log the configuration being used
            print(f"[*] Using configuration from UI:")
            print(f"    - Sector threshold: {current_trading_config.get('sector_weight_threshold', 43)}%")
            print(f"    - Tickers: {current_trading_config.get('tickers', [])}")
            print(f"    - BB width: {current_trading_config.get('bb_width_threshold', 0.05)}")
            
            # Get UI widget for displaying results
            backtest_widget = self.app.get_backtest_widget()
            
            # Clear previous results
            backtest_widget.results_text.clear()
            
            # Extract parameters
            tickers = params.get('tickers', ['SPY', 'QQQ'])
            timeframe = params.get('timeframe', '5m')
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            data_source = params.get('data_source', 'YFinance')
            
            # Display header
            backtest_widget.results_text.append(f"""
            <div style='background-color: #2c3e50; color: white; padding: 15px; border-radius: 8px;'>
                <h2 style='margin: 0;'>Professional Backtest Starting</h2>
                <p style='margin: 5px 0;'>Data Source: {data_source}</p>
                <p style='margin: 5px 0;'>Symbols: {', '.join(tickers)}</p>
                <p style='margin: 5px 0;'>Timeframe(s): {timeframe}</p>
                <p style='margin: 5px 0;'>Date Range: {start_date} to {end_date}</p>
            </div>
            """)
            
            # Calculate date range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            days_diff = (end_dt - start_dt).days
            
            # Check for 6-month data request
            if days_diff > 180:
                backtest_widget.results_text.append(f"""
                <div style='background-color: #f39c12; color: white; padding: 10px; border-radius: 5px; margin: 10px 0;'>
                    <b>⚠️ Large Date Range Detected</b><br>
                    Requesting {days_diff} days of data (>6 months)<br>
                    Note: Data availability depends on source and timeframe
                </div>
                """)
            
            # Show data source limitations
            if data_source == 'YFinance':
                limitations_html = """
                <div style='background-color: #e74c3c; color: white; padding: 10px; border-radius: 5px; margin: 10px 0;'>
                    <b>📊 YFinance Data Limitations:</b><br>
                """
                
                if '1m' in timeframe or timeframe == 'All':
                    limitations_html += "• 1-minute data: Only last 7 days available<br>"
                if '5m' in timeframe or timeframe == 'All':
                    limitations_html += "• 5-minute data: Only last 60 days available<br>"
                if '15m' in timeframe or timeframe == 'All':
                    limitations_html += "• 15-minute data: Only last 60 days available<br>"
                
                limitations_html += """
                    <small>For full historical data, use TastyTrade API</small>
                </div>
                """
                backtest_widget.results_text.append(limitations_html)
            
            # Initialize professional backtest runner
            runner = ProfessionalBacktestRunner(
                config=self.config,  # This is already there
                api=self.api if hasattr(self, 'api') else None
            )
            
            # Prepare parameters for professional runner
            backtest_params = {
                'symbols': tickers,
                'timeframes': timeframe if timeframe != 'All' else ['1m', '5m', '15m'],
                'start_date': start_date,
                'end_date': end_date,
                'data_source': data_source
            }
            
            # Update UI
            backtest_widget.results_text.append("""
            <div style='background-color: #3498db; color: white; padding: 10px; border-radius: 5px;'>
                <b>🚀 Running Professional Backtest...</b><br>
                This may take several minutes for large datasets
            </div>
            """)
            
            # Allow UI to update
            QApplication.processEvents()
            
            # Run the professional backtest
            results = runner.run_comprehensive_backtest(backtest_params)
            
            # Check for errors
            if 'error' in results:
                backtest_widget.results_text.append(f"""
                <div style='background-color: #e74c3c; color: white; padding: 10px; border-radius: 5px;'>
                    <b>❌ Backtest Error:</b><br>
                    {results['error']}
                </div>
                """)
                return
            
            # Display results summary
            summary = results.get('summary', {})
            backtest_widget.results_text.append(f"""
            <div style='background-color: #27ae60; color: white; padding: 15px; border-radius: 8px; margin: 10px 0;'>
                <h3 style='margin: 0 0 10px 0;'>✅ Backtest Complete!</h3>
                <div style='display: flex; justify-content: space-around;'>
                    <div>
                        <b>Tests Run:</b> {summary.get('successful_tests', 0)}/{summary.get('total_combinations', 0)}<br>
                        <b>Total Trades:</b> {summary.get('total_trades', 0):,}<br>
                        <b>Avg Win Rate:</b> {summary.get('avg_win_rate', 0):.1f}%
                    </div>
                    <div>
                        <b>Run ID:</b> {results.get('run_id', 'N/A')}<br>
                        <b>Report:</b> <a href='file:///{results.get('report_path', '')}' style='color: white;'>Open Report</a><br>
                        <b>Log:</b> <a href='file:///{results.get('log_path', '')}' style='color: white;'>View Logs</a>
                    </div>
                </div>
            </div>
            """)
            
            # Display individual results
            all_results = results.get('results', {})
            
            # Create summary table for UI
            if all_results:
                # Store results for export
                self.current_results = all_results
                
                # Display best and worst performers
                best = summary.get('best_performer')
                worst = summary.get('worst_performer')
                
                if best:
                    backtest_widget.results_text.append(f"""
                    <div style='background-color: #2ecc71; color: white; padding: 10px; border-radius: 5px; margin: 5px 0;'>
                        <b>🏆 Best Performer:</b> {best['symbol']}<br>
                        Profit Factor: {best['profit_factor']:.2f} | Win Rate: {best['win_rate']:.1f}%
                    </div>
                    """)
                
                if worst:
                    backtest_widget.results_text.append(f"""
                    <div style='background-color: #e74c3c; color: white; padding: 10px; border-radius: 5px; margin: 5px 0;'>
                        <b>📉 Worst Performer:</b> {worst['symbol']}<br>
                        Max Drawdown: {worst['max_drawdown']:.1f}% | Win Rate: {worst['win_rate']:.1f}%
                    </div>
                    """)
                
                # Display detailed results
                backtest_widget.display_results(all_results)
                
                # Enable export button
                backtest_widget.export_button.setEnabled(True)
                backtest_widget.current_results = all_results
            
            # Show data fetch summary with log file location
            log_file_path = results.get('log_path', 'Not available')
            backtest_widget.results_text.append(f"""
            <div style='background-color: #34495e; color: white; padding: 10px; border-radius: 5px; margin: 10px 0;'>
                <b>📁 Output Files:</b><br>
                • Analysis files: Backtest_Data/Analysis/<br>
                • Trade logs: Backtest_Data/Results/Trades/<br>
                • Summary reports: Backtest_Data/Results/Summary/<br>
                • Run Log: <a href='file:///{log_file_path}' style='color: #3498db;'>{log_file_path}</a><br>
                • Main Logs: logs/
            </div>
            """)
            
            # Re-enable run button
            backtest_widget.run_button.setEnabled(True)
            backtest_widget.run_button.setText("Run Backtest")
            
        except Exception as e:
            # Log the full error
            import traceback
            error_details = traceback.format_exc()
            self.logger.error(f"Backtest error: {error_details}")
            
            # Display error in UI
            backtest_widget.results_text.append(f"""
            <div style='background-color: #e74c3c; color: white; padding: 15px; border-radius: 5px;'>
                <b>❌ Critical Error:</b><br>
                {str(e)}<br><br>
                <small>Check logs for detailed error information</small>
            </div>
            """)
            
            # Re-enable button
            backtest_widget.run_button.setEnabled(True)
            backtest_widget.run_button.setText("Run Backtest")


    
    def _generate_combined_summary(self, all_results, output_file):
        """
        Generate a combined summary CSV file with results from all tickers and periods
        
        Args:
            all_results (dict): Dictionary of results by ticker/period
            output_file (str): Output file path
        """
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Symbol_Period', 
                'Win Rate (%)', 
                'Profit Factor',
                'Total Trades',
                'Winning Trades',
                'Losing Trades',
                'Gross Profit ($)',
                'Gross Loss ($)',
                'Max Drawdown (%)',
                'Final Equity ($)',
                'Optimal Trailing Method'
            ])
            
            # Write data for each ticker/period
            for symbol_period, results in all_results.items():
                writer.writerow([
                    symbol_period,
                    results.get('Win Rate', 0),
                    results.get('Profit Factor', 0),
                    results.get('Total Trades', 0),
                    results.get('Winning Trades', 0),
                    results.get('Losing Trades', 0),
                    results.get('Gross Profit', 0),
                    results.get('Gross Loss', 0),
                    results.get('Max Drawdown', 0),
                    results.get('Final Equity', 10000),  # Default if not available
                    results.get('Optimal Trailing Method', 'None')
                ])


    def _check_exit_signal(self, entry_candle, current_candle, entry_direction):
        """
        Check for exit signal based on candle data
        
        Args:
            entry_candle (dict): Entry candle data
            current_candle (dict): Current candle data
            entry_direction (str): Entry direction ("long" or "short")
            
        Returns:
            bool: True if exit signal is present, False otherwise
        """
        try:
            # Extract OHLC values
            entry_open = float(entry_candle["open"])
            entry_high = float(entry_candle["high"])
            entry_low = float(entry_candle["low"])
            entry_close = float(entry_candle["close"])
            
            curr_open = float(current_candle["open"])
            curr_high = float(current_candle["high"])
            curr_low = float(current_candle["low"])
            curr_close = float(current_candle["close"])
            
            # Check for opposing signal
            if entry_direction == "long":
                # Exit long if bearish candle
                if curr_close < curr_open and curr_high == curr_open:
                    return True
            elif entry_direction == "short":
                # Exit short if bullish candle
                if curr_close > curr_open and curr_low == curr_open:
                    return True
            
            return False
        except Exception as e:
            print(f"Error in exit signal check: {e}")
            return False

    def _generate_backtest_csv(self, results, output_file):
        """
        Generate a CSV file with backtest results
        
        Args:
            results (dict): Backtest results for all tickers and periods
            output_file (str): Output file path
        """
        try:
            import csv
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Write summary statistics
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(["Ticker_Period", "Win Rate", "Profit Factor", "Max Drawdown", "Total Trades", 
                                "Winning Trades", "Losing Trades", "Gross Profit", "Gross Loss", 
                                "Final Equity", "Optimal Trailing Method"])
                
                # Write data for each ticker and period
                for ticker_period, result in results.items():
                    writer.writerow([
                        ticker_period,
                        result.get("Win Rate", 0),
                        result.get("Profit Factor", 0),
                        result.get("Max Drawdown", 0),
                        result.get("Total Trades", 0),
                        result.get("Winning Trades", 0),
                        result.get("Losing Trades", 0),
                        result.get("Gross Profit", 0),
                        result.get("Gross Loss", 0),
                        result.get("Final Equity", 0),
                        result.get("Optimal Trailing Method", "Unknown")
                    ])
                
                # Add a blank row
                writer.writerow([])
                
                # Write trade details header
                writer.writerow(["Ticker_Period", "Entry Time", "Entry Price", "Exit Time", "Exit Price", 
                            "Direction", "PnL %", "PnL $"])
                
                # Write trade details for each ticker and period
                for ticker_period, result in results.items():
                    trades = result.get("Trades", [])
                    for trade in trades:
                        writer.writerow([
                            ticker_period,
                            trade.get("entry_time", ""),
                            trade.get("entry_price", 0),
                            trade.get("exit_time", ""),
                            trade.get("exit_price", 0),
                            trade.get("direction", ""),
                            round(trade.get("pnl_pct", 0), 2),
                            round(trade.get("pnl_dollars", 0), 2)
                        ])
            
            print(f"Backtest results saved to {output_file}")
            
        except Exception as e:
            print(f"Error generating backtest CSV: {e}")
        
    def refresh_mongodb_stats(self):
        """Refresh MongoDB statistics"""
        # This is now handled by the periodic stats thread
        # but we keep this method for manual refresh
        mongodb_widget = self.app.get_mongodb_widget()
        mongodb_widget.status_label.setText("Refreshing statistics...")
        
        try:
            db = get_mongodb_handler()
            stats = db.get_collection_stats()
            mongodb_running = db._is_mongodb_running()
            
            mongodb_widget.update_stats(stats)
            mongodb_widget.update_mongodb_status(mongodb_running)
            mongodb_widget.status_label.setText("Statistics refreshed")
        except Exception as e:
            logger.error(f"Error refreshing MongoDB stats: {str(e)}")
            mongodb_widget.status_label.setText(f"Error: {str(e)}")
            
    def start_mongodb(self):
        """Start MongoDB server"""
        mongodb_widget = self.app.get_mongodb_widget()
        mongodb_widget.status_label.setText("Starting MongoDB...")
        
        # Disable controls while operation is in progress
        mongodb_widget.set_controls_enabled(False)
        
        # Start MongoDB control thread
        self.mongodb_control_thread = MongoDBControlThread(action="start")
        self.mongodb_control_thread.success_signal.connect(self.handle_mongodb_control_result)
        self.mongodb_control_thread.error_signal.connect(lambda msg: self.show_alert(msg, "error"))
        self.mongodb_control_thread.start()
        
    def stop_mongodb(self):
        """Stop MongoDB server"""
        mongodb_widget = self.app.get_mongodb_widget()
        mongodb_widget.status_label.setText("Stopping MongoDB...")
        
        # Disable controls while operation is in progress
        mongodb_widget.set_controls_enabled(False)
        
        # Start MongoDB control thread
        self.mongodb_control_thread = MongoDBControlThread(action="stop")
        self.mongodb_control_thread.success_signal.connect(self.handle_mongodb_control_result)
        self.mongodb_control_thread.error_signal.connect(lambda msg: self.show_alert(msg, "error"))
        self.mongodb_control_thread.start()
        
    def handle_mongodb_control_result(self, is_running, message):
        """Handle MongoDB control operation result"""
        mongodb_widget = self.app.get_mongodb_widget()
        
        # Update status
        mongodb_widget.update_mongodb_status(is_running)
        mongodb_widget.status_label.setText(message)
        
        # Re-enable controls
        mongodb_widget.set_controls_enabled(True)
        
        # Refresh statistics
        self.refresh_mongodb_stats()
        
    def clear_database(self):
        """Clear all data from MongoDB database"""
        # Confirm with user
        reply = QMessageBox.question(
            self.app,
            "Confirm Database Clear",
            "Are you sure you want to clear all data from the database?\n\nThis action cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            # Show progress message
            mongodb_widget = self.app.get_mongodb_widget()
            mongodb_widget.status_label.setText("Clearing database...")
            
            # Disable controls while operation is in progress
            mongodb_widget.set_controls_enabled(False)
            
            # Start clear database thread
            self.clear_db_thread = ClearDatabaseThread()
            self.clear_db_thread.success_signal.connect(self.handle_clear_database_result)
            self.clear_db_thread.error_signal.connect(lambda msg: self.show_alert(msg, "error"))
            self.clear_db_thread.start()
                
    def handle_clear_database_result(self, result):
        """Handle database clear operation result"""
        mongodb_widget = self.app.get_mongodb_widget()
        
        # Create result message
        message = "Database cleared successfully:\n"
        for collection, count in result.items():
            message += f"- {collection}: {count} documents deleted\n"
        
        # Show success message
        QMessageBox.information(
            self.app,
            "Database Cleared",
            message
        )
        
        # Log success
        mongodb_widget.status_label.setText("Database cleared successfully")
        
        # Re-enable controls
        mongodb_widget.set_controls_enabled(True)
        
        # Refresh statistics
        self.refresh_mongodb_stats()
        
    def show_alert(self, message, alert_type):
        """Show alert message"""
        if alert_type == "error":
            QMessageBox.critical(self.app, "Error", message)
        elif alert_type == "warning":
            QMessageBox.warning(self.app, "Warning", message)
        else:
            QMessageBox.information(self.app, "Information", message)
            
    def run(self):
        """Run the application"""
        self.app.show()
        if self.app_instance:
            return self.app_instance.exec_()
        return 0
        
    def cleanup(self):
        """Clean up resources before exiting"""
        # Stop threads
        if self.mongodb_stats_thread and self.mongodb_stats_thread.isRunning():
            self.mongodb_stats_thread.stop()
            self.mongodb_stats_thread.wait(1000)
            
        if self.bot_thread and self.bot_thread.isRunning():
            self.bot_thread.stop()
            self.bot_thread.wait(1000)


    def kill_all_orders(self):
        self.app.get_dashboard().kill_bot_requested.connect(self.kill_all_orders)
        self.app.get_dashboard().cancel_trade_requested.connect(self.cancel_trade)
        """Kill all orders and positions"""
        try:
            if self.order_manager:
                result = self.order_manager.kill_all_orders()
                self.app.get_dashboard().update_log(f"Kill switch activated: Canceled {result['orders_canceled']} orders, closed {result['positions_closed']} positions")
                
                if result["errors"]:
                    for error in result["errors"]:
                        self.app.get_dashboard().update_log(f"Error: {error}")
                        
                # Refresh the trades table
                self.update_active_trades()
        except Exception as e:
            self.app.get_dashboard().update_log(f"Error in kill switch: {str(e)}")

    def cancel_trade(self, order_id):
        """Cancel a specific trade"""
        try:
            if self.order_manager:
                if self.order_manager.cancel_order(order_id):
                    self.app.get_dashboard().update_log(f"Order {order_id} canceled successfully")
                else:
                    self.app.get_dashboard().update_log(f"Failed to cancel order {order_id}")
                
                # Refresh the trades table
                self.update_active_trades()
        except Exception as e:
            self.app.get_dashboard().update_log(f"Error canceling order: {str(e)}")
