#!/usr/bin/env python3
"""
Jigsaw Flow Options Trading Bot - Main entry point
This script serves as the main entry point for the Jigsaw Flow trading bot.
It initializes the bot components, handles configuration, and starts the trading logic.
"""

import sys
import os
import argparse
import logging
import signal
import cProfile
import pstats
from datetime import datetime
from PyQt5.QtWidgets import QApplication

# Add the current directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import our modules - UPDATED FOR TRADESTATION
from Code.bot_core.tradestation_api import TradeStationAPI
from Code.bot_core.config_loader import ConfigLoader
from Code.bot_core.instrument_fetcher import InstrumentFetcher
from Code.bot_core.mongodb_handler import get_mongodb_handler

from Code.bot_core.market_data_client import MarketDataClient
from Code.bot_core.order_manager import OrderManager
from Code.bot_core.jigsaw_strategy import JigsawStrategy
from Code.bot_core.position_manager import PositionManager
import time

# Import UI components
from Code.ui.jigsaw_flow_ui import JigsawFlowApp
from Code.ui.ui_controller import UIController


# Setup logging
today = datetime.now().strftime("%Y-%m-%d")
log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"run_bot_{today}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("JigsawFlow")

# Global controller instance for clean shutdown
ui_controller = None

def exception_handler(exctype, value, traceback):
    """
    Global exception handler to prevent application crash
    """
    print(f"[!] Uncaught exception: {exctype.__name__}: {value}")
    logger.critical(f"Uncaught exception: {exctype.__name__}: {value}", 
                  exc_info=(exctype, value, traceback))
    
    # If it's a Qt-related error, don't exit
    if "Qt" in str(exctype) or "QBasicTimer" in str(value):
        return
    
    # For other critical errors, prompt user
    import tkinter as tk
    from tkinter import messagebox
    
    root = tk.Tk()
    root.withdraw()
    
    result = messagebox.askyesno(
        "Error Occurred", 
        f"An error occurred: {value}\n\nDo you want to continue running?"
    )
    root.destroy()
    
    if not result:
        sys.exit(1)


def signal_handler(sig, frame):
    """Handle interruption signals for clean shutdown"""
    logger.info("Shutdown signal received. Cleaning up...")
    print("\n[*] Shutdown signal received. Cleaning up...")
    
    # Clean up controller resources
    if ui_controller:
        ui_controller.cleanup()
        
    sys.exit(0)



def setup_logging(args):
    """
    Setup logging with different levels and handlers
    
    Args:
        args: Command line arguments
    """
    today = datetime.now().strftime("%Y-%m-%d")
    log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))
    os.makedirs(log_folder, exist_ok=True)
    
    # Determine log level
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING
    
    # Create handlers
    log_file = os.path.join(log_folder, f"run_bot_{today}.log")
    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()
    
    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Set formatters
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Create separate error log
    error_log_file = os.path.join(log_folder, f"errors_{today}.log")
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_handler)
    
    # Log startup info
    logger = logging.getLogger("JigsawFlow")
    logger.info(f"Logging initialized at level {log_level}")
    if args.headless:
        logger.info("Running in headless mode")
    if args.test_mode:
        logger.info("Running in test mode with simulated data")
    if args.backtest:
        logger.info("Running in backtest mode")
    
    return logger



def run_headless(args):
    """Run the bot in command-line mode without UI"""
    logger.info("Starting bot in headless mode")
    print("[*] Starting Jigsaw Flow Trading Bot (Headless Mode)")
    
    # Load config
    config_loader = ConfigLoader(args.config)
    config = config_loader.load_config()
    
    # Check if we're in test mode
    test_mode = config["broker"].get("test_mode", False)
    if args.test_mode:
        test_mode = True
    
    if test_mode:
        print("[*] Running in TEST MODE - using simulated data")
        
    # Initialize MongoDB
    print("[*] Initializing MongoDB...")
    db = get_mongodb_handler()
    
    # Initialize API - UPDATED FOR TRADESTATION
    api = TradeStationAPI()

    # Login
    if test_mode or api.login():
        if test_mode:
            print("[✓] Test mode - skipping login")
            print("[✓] Using simulated account balance")
            balances = {
                "cash_balance": 10000.0,
                "available_trading_funds": 10000.0,
                "net_liquidating_value": 10000.0
            }
            account_id = "TEST123"
        else:
            print("[✓] Logged in to TradeStation")
            # Fetch and print balance
            balances = api.fetch_account_balance()
            account_id = None  # Will be auto-detected
            
        cash = balances["cash_balance"]
        available = balances["available_trading_funds"]
        
        print(f"[✓] Cash Balance: ${cash:,.2f}")
        print(f"[✓] Available Trading Funds: ${available:,.2f}")
        
        # Initialize instrument fetcher
        fetcher = InstrumentFetcher(api, test_mode=test_mode)
        
        # ===== ADD RECOVERY CODE HERE =====
        print("[*] Starting trading operations with position recovery...")
                
        # Initialize market data client
        quote_token = api.get_quote_token() if not test_mode else {"token": "test", "dxlink-url": "test"}
        market_data_client = MarketDataClient(
            api_quote_token=quote_token,
            save_to_db=True,
            api=api
        )
        
        # Initialize order manager
        order_manager = OrderManager(api, account_id)
        
        # Initialize strategy with recovery
        strategy = JigsawStrategy(
            instrument_fetcher=fetcher,
            market_data_client=market_data_client,
            order_manager=order_manager,
            config=config
        )
        
        # CRITICAL: Recover positions from database
        print("[*] Recovering positions from database...")
        strategy.recover_positions_on_startup()
        
        # Verify recovery
        active_positions = strategy.position_manager.get_all_positions()
        if active_positions:
            print(f"[✓] Recovered {len(active_positions)} active positions:")
            for symbol, position in active_positions.items():
                print(f"    - {symbol}: {position['type']} @ ${position.get('entry_price', 'N/A')}")
        else:
            print("[*] No active positions to recover")
        
        # Sync with broker
        print("[*] Syncing with broker...")
        strategy.sync_positions_with_broker()
        
        # Initialize strategy
        strategy.initialize()
        
        # Start trading loop with periodic sync
        last_sync_time = time.time()
        sync_interval = 300  # Sync every 5 minutes
        
        print("[*] Starting main trading loop...")
        try:
            while True:
                # Regular trading logic
                strategy.scan_for_trades()
                strategy.manage_active_trades()
                
                # Periodic sync with broker
                current_time = time.time()
                if current_time - last_sync_time > sync_interval:
                    print("[*] Performing periodic position sync...")
                    strategy.sync_positions_with_broker()
                    last_sync_time = current_time
                
                time.sleep(1)  # Main loop delay
                
        except KeyboardInterrupt:
            print("\n[*] Shutting down gracefully...")
            # Save final position state
            strategy.position_manager.export_positions("final_positions_backup.json")
        # ===== END OF RECOVERY CODE =====
        
        print("[*] Trading operation completed")
        
        # Logout if not in test mode
        if not test_mode:
            api.logout()
            
        return 0
    else:
        print("[✗] Failed to login. Check credentials and try again.")
        return 1


def run_with_ui(args):
    """Run the bot with the GUI interface"""
    global ui_controller
    
    logger.info("Starting bot with UI")
    print("[*] Starting Jigsaw Flow Trading Bot (UI Mode)")
    
    # Initialize QApplication
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # Create UI
    ui = JigsawFlowApp()
    
    # Create controller
    ui_controller = UIController(ui)
    
    # Show UI
    ui.show()
    
    # Register signal handlers for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run application
    return app.exec_()


def main():
    """Main entry point for the application"""
    sys.excepthook = exception_handler
    parser = argparse.ArgumentParser(description="Jigsaw Flow Trading Bot")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode (no UI)")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode (simulated data)")
    parser.add_argument("--backtest", action="store_true", help="Run backtest instead of live trading")
    parser.add_argument("--config", type=str, help="Path to alternative config file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--profile", action="store_true", help="Enable performance profiling")
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args)
    
    try:
        # Enable profiling if requested
        if args.profile:
            profiler = cProfile.Profile()
            profiler.enable()
            
        # Run the bot
        if args.headless:
            result = run_headless(args)
        else:
            result = run_with_ui(args)
            
        # Save profiling results if enabled
        if args.profile:
            profiler.disable()
            today = datetime.now().strftime("%Y-%m-%d")
            stats_file = os.path.join('logs', f"profile_{today}.stats")
            profiler.dump_stats(stats_file)
            
            # Print top 20 stats
            stats = pstats.Stats(stats_file)
            stats.sort_stats('cumulative').print_stats(20)
            
        return result
        
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        print(f"[✗] Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())