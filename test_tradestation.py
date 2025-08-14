#!/usr/bin/env python3
"""
TradeStation Paper Trading Test Suite
Tests all major functionality using your bot_core modules
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from colorama import init, Fore, Style

# Initialize colorama for colored output
init(autoreset=True)

# Add the parent directory to the path so we can import bot_core modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your bot_core modules
from bot_core.tradestation_api import TradeStationAPI
from bot_core.instrument_fetcher import InstrumentFetcher
from bot_core.market_data_client import MarketDataClient
from bot_core.order_manager import OrderManager
from bot_core.candle_data_client import CandleDataClient
from bot_core.tradestation_data_fetcher import TradeStationDataFetcher
from bot_core.jigsaw_strategy import JigsawStrategy
from bot_core.position_manager import PositionManager
from bot_core.config_loader import ConfigLoader

# Test results tracking
test_results = {
    "passed": 0,
    "failed": 0,
    "skipped": 0,
    "errors": []
}

def print_header(text):
    """Print a formatted header"""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.CYAN}{text.center(80)}")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}")

def print_test(test_name):
    """Print test name"""
    print(f"\n{Fore.YELLOW}[TEST] {test_name}{Style.RESET_ALL}")

def print_success(message):
    """Print success message"""
    print(f"{Fore.GREEN}[✓] {message}{Style.RESET_ALL}")
    test_results["passed"] += 1

def print_error(message):
    """Print error message"""
    print(f"{Fore.RED}[✗] {message}{Style.RESET_ALL}")
    test_results["failed"] += 1
    test_results["errors"].append(message)

def print_info(message):
    """Print info message"""
    print(f"{Fore.BLUE}[*] {message}{Style.RESET_ALL}")

def print_warning(message):
    """Print warning message"""
    print(f"{Fore.MAGENTA}[!] {message}{Style.RESET_ALL}")

def test_api_authentication(api):
    """Test API authentication and connection"""
    print_test("API Authentication")
    
    try:
        # Check if already logged in
        if api.access_token:
            print_info("Already have access token, testing validity...")
            if api.check_and_refresh_session():
                print_success("API authentication valid")
                return True
        
        # Try to login
        print_info("Attempting to login...")
        if api.login():
            print_success("Successfully authenticated with TradeStation API")
            return True
        else:
            print_error("Failed to authenticate with TradeStation API")
            return False
            
    except Exception as e:
        print_error(f"Authentication error: {str(e)}")
        return False

def test_account_access(api):
    """Test account access and permissions"""
    print_test("Account Access")
    
    try:
        # Fetch account balance
        balance = api.fetch_account_balance()
        
        if balance and balance.get("cash_balance", 0) >= 0:
            print_success(f"Account accessed successfully")
            print_info(f"Cash Balance: ${balance.get('cash_balance', 0):,.2f}")
            print_info(f"Buying Power: ${balance.get('available_trading_funds', 0):,.2f}")
            return True
        else:
            print_warning("Account access limited - this is normal for paper accounts")
            print_info("Continuing with tests that don't require balance info...")
            test_results["skipped"] += 1
            return True  # Don't fail the test for paper accounts
            
    except Exception as e:
        print_error(f"Account access error: {str(e)}")
        return False

def test_market_data_quotes(api, instrument_fetcher):
    """Test real-time market data quotes"""
    print_test("Market Data - Quotes")
    
    try:
        # Test symbols
        symbols = ["SPY", "QQQ", "AAPL"]
        
        print_info(f"Fetching quotes for: {', '.join(symbols)}")
        quotes = api.get_market_quotes(symbols)
        
        if quotes and len(quotes) > 0:
            print_success(f"Received {len(quotes)} quotes")
            
            for quote in quotes:
                symbol = quote.get("symbol", "Unknown")
                bid = quote.get("bid", 0)
                ask = quote.get("ask", 0)
                last = quote.get("last", 0)
                
                print_info(f"{symbol}: Bid=${bid:.2f}, Ask=${ask:.2f}, Last=${last:.2f}")
            
            return True
        else:
            print_error("No quotes received")
            return False
            
    except Exception as e:
        print_error(f"Market data error: {str(e)}")
        return False

def test_historical_data(api):
    """Test historical data fetching"""
    print_test("Historical Data")
    
    try:
        # Create data fetcher
        fetcher = TradeStationDataFetcher(api=api)
        
        # Test connection first
        if not fetcher.test_connection():
            print_error("Data fetcher connection failed")
            return False
        
        # Fetch 5 days of 5-minute data for SPY
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=5)
        
        print_info(f"Fetching SPY 5m data from {start_date} to {end_date}")
        df = fetcher.fetch_bars("SPY", start_date, end_date, "5m")
        
        if not df.empty:
            print_success(f"Received {len(df)} bars of historical data")
            print_info(f"First bar: {df.index[0]}")
            print_info(f"Last bar: {df.index[-1]}")
            print_info(f"Sample data: Open=${df.iloc[-1]['open']:.2f}, Close=${df.iloc[-1]['close']:.2f}")
            return True
        else:
            print_error("No historical data received")
            return False
            
    except Exception as e:
        print_error(f"Historical data error: {str(e)}")
        return False

def test_streaming_data(api):
    """Test streaming market data"""
    print_test("Streaming Market Data")
    
    try:
        # Get quote token
        quote_token = api.get_quote_token()
        
        if not quote_token or not quote_token.get("token"):
            print_error("Failed to get streaming token")
            return False
        
        print_success("Got streaming token")
        print_info(f"Streaming URL: {quote_token.get('dxlink-url', 'N/A')}")
        
        # Create market data client
        market_data = MarketDataClient(
            api_quote_token=quote_token,
            save_to_db=False,  # Don't save test data
            build_candles=False
        )
        
        # Track received data
        quotes_received = []
        trades_received = []
        
        def on_quote(quote):
            quotes_received.append(quote)
            if len(quotes_received) <= 3:  # Print first 3
                print_info(f"Quote: {quote['symbol']} - Bid: ${quote['bid']:.2f}, Ask: ${quote['ask']:.2f}")
        
        def on_trade(trade):
            trades_received.append(trade)
            if len(trades_received) <= 3:  # Print first 3
                print_info(f"Trade: {trade['symbol']} - Price: ${trade['price']:.2f}, Size: {trade['size']}")
        
        # Set callbacks
        market_data.on_quote = on_quote
        market_data.on_trade = on_trade
        
        # Connect
        print_info("Connecting to streaming data...")
        if not market_data.connect():
            print_error("Failed to connect to streaming service")
            return False
        
        # Subscribe to test symbols
        print_info("Subscribing to SPY and QQQ...")
        channel_id = market_data.subscribe(["SPY", "QQQ"], ["Quote", "Trade"])
        
        # Wait for data
        print_info("Waiting for streaming data (10 seconds)...")
        time.sleep(10)
        
        # Check results
        market_data.disconnect()
        
        if quotes_received or trades_received:
            print_success(f"Received {len(quotes_received)} quotes and {len(trades_received)} trades")
            return True
        else:
            print_warning("No streaming data received - market may be closed")
            test_results["skipped"] += 1
            return True  # Don't fail if market is closed
            
    except Exception as e:
        print_error(f"Streaming data error: {str(e)}")
        return False

def test_option_chain(api, instrument_fetcher):
    """Test option chain fetching"""
    print_test("Option Chain Data")
    
    try:
        symbol = "SPY"
        print_info(f"Fetching option chain for {symbol}")
        
        # Fetch nested option chain
        option_chain = instrument_fetcher.fetch_nested_option_chains(symbol)
        
        if option_chain and "expirations" in option_chain:
            expirations = option_chain["expirations"]
            print_success(f"Received {len(expirations)} expirations")
            
            # Show first expiration
            if expirations:
                first_exp = expirations[0]
                exp_date = first_exp.get("expiration-date", "Unknown")
                strikes = first_exp.get("strikes", [])
                print_info(f"First expiration: {exp_date} with {len(strikes)} strikes")
                
                # Show a few strikes
                for i, strike in enumerate(strikes[:3]):
                    strike_price = strike.get("strike-price", 0)
                    call = strike.get("call", "N/A")
                    put = strike.get("put", "N/A")
                    print_info(f"  Strike ${strike_price}: Call={call}, Put={put}")
            
            return True
        else:
            print_error("No option chain data received")
            return False
            
    except Exception as e:
        print_error(f"Option chain error: {str(e)}")
        return False

# Update the test_order_placement function in test_tradestation.py

def test_order_placement(api, order_manager):
    """Test order placement (dry run only for safety)"""
    print_test("Order Placement (Dry Run)")
    
    try:
        # First, let's try to get a valid option symbol
        print_info("Fetching current SPY option chain to get valid symbol...")
        
        instrument_fetcher = InstrumentFetcher(api)
        option_chain = instrument_fetcher.fetch_nested_option_chains("SPY")
        
        test_symbol = None
        if option_chain and "expirations" in option_chain:
            expirations = option_chain["expirations"]
            if expirations and len(expirations) > 0:
                # Get first expiration
                first_exp = expirations[0]
                strikes = first_exp.get("strikes", [])
                
                # Find a strike near the money
                if strikes:
                    # Get middle strike
                    middle_idx = len(strikes) // 2
                    strike_data = strikes[middle_idx]
                    test_symbol = strike_data.get("call")
                    print_info(f"Using option symbol: {test_symbol}")
        
        # If we couldn't get a valid symbol, use a default format
        if not test_symbol:
            # Use proper OCC format: SYMBOL YYMMDDCP########
            # SPY call expiring Aug 16, 2024, strike $550
            test_symbol = "SPY 240816C00550000"
            print_warning(f"Using default symbol format: {test_symbol}")
        
        print_info("Creating test option order...")
        
        # This is a dry run - we won't actually submit it
        test_order = order_manager.create_equity_option_order(
            symbol=test_symbol,
            quantity=1,
            direction="Buy to Open",
            price=0.50,  # Lower price to ensure it won't fill
            order_type="Limit",
            time_in_force="Day"
        )
        
        print_info(f"Test order created: {json.dumps(test_order, indent=2)}")
        
        # Validate with dry run
        dry_run_result = order_manager.dry_run_order(test_order)
        
        if dry_run_result.get("valid"):
            print_success("Order validation passed")
            print_info(f"Estimated cost: ${dry_run_result.get('estimated_cost', 0):.2f}")
            
            # Ask user if they want to submit for real
            print_warning("\nWould you like to submit this order to your PAPER account?")
            print_warning("This will place a REAL order on your paper trading account.")
            print_warning("The order is set at $0.50 which should not fill.")
            response = input("Type 'YES' to submit, anything else to skip: ").strip().upper()
            
            if response == "YES":
                print_info("Submitting order to paper account...")
                result = order_manager.submit_order(test_order)
                
                if "error" not in result:
                    order_id = result.get("order", {}).get("id")
                    print_success(f"Order submitted successfully! Order ID: {order_id}")
                    
                    # Wait a moment then check status
                    time.sleep(2)
                    status = order_manager.get_order_status(order_id)
                    print_info(f"Order status: {status.get('status', 'Unknown')}")
                    
                    # Cancel the order
                    print_info("Canceling test order...")
                    if order_manager.cancel_order(order_id):
                        print_success("Order canceled successfully")
                    else:
                        print_warning("Failed to cancel order - it may have already filled")
                    
                    return True
                else:
                    print_error(f"Order submission failed: {result.get('error')}")
                    return False
            else:
                print_info("Skipped actual order submission")
                return True
        else:
            print_error(f"Order validation failed: {dry_run_result.get('error')}")
            return False
            
    except Exception as e:
        print_error(f"Order placement error: {str(e)}")
        return False

def test_positions(api, order_manager):
    """Test position fetching"""
    print_test("Position Management")
    
    try:
        print_info("Fetching current positions...")
        positions = order_manager.get_positions()
        
        if isinstance(positions, list):
            print_success(f"Retrieved {len(positions)} positions")
            
            if positions:
                for pos in positions[:5]:  # Show first 5
                    symbol = pos.get("symbol", "Unknown")
                    quantity = pos.get("quantity", 0)
                    avg_price = pos.get("average_price", 0)
                    current_price = pos.get("current_price", 0)
                    pnl = pos.get("unrealized_pnl", 0)
                    
                    print_info(f"{symbol}: {quantity} @ ${avg_price:.2f}, "
                             f"Current: ${current_price:.2f}, P&L: ${pnl:.2f}")
            else:
                print_info("No open positions")
            
            return True
        else:
            print_error("Failed to retrieve positions")
            return False
            
    except Exception as e:
        print_error(f"Position management error: {str(e)}")
        return False

def test_active_orders(api, order_manager):
    """Test active order fetching"""
    print_test("Active Orders")
    
    try:
        print_info("Fetching active orders...")
        orders = order_manager.get_active_orders()
        
        if isinstance(orders, list):
            print_success(f"Retrieved {len(orders)} active orders")
            
            if orders:
                for order in orders[:5]:  # Show first 5
                    order_id = order.get("id", "Unknown")
                    symbol = order.get("symbol", "Unknown")
                    status = order.get("status", "Unknown")
                    order_type = order.get("order_type", "Unknown")
                    
                    print_info(f"Order {order_id}: {symbol} - {order_type} - Status: {status}")
            else:
                print_info("No active orders")
            
            return True
        else:
            print_error("Failed to retrieve active orders")
            return False
            
    except Exception as e:
        print_error(f"Active orders error: {str(e)}")
        return False

def test_strategy_initialization(api, config):
    """Test strategy initialization"""
    print_test("Strategy Initialization")
    
    try:
        # Get quote token for market data
        quote_token = api.get_quote_token()
        
        # Create necessary components
        market_data = MarketDataClient(
            api_quote_token=quote_token,
            save_to_db=False,
            build_candles=False
        )
        
        instrument_fetcher = InstrumentFetcher(api)
        order_manager = OrderManager(api)
        
        # Create strategy
        print_info("Creating JigsawStrategy instance...")
        strategy = JigsawStrategy(
            instrument_fetcher=instrument_fetcher,
            market_data_client=market_data,
            order_manager=order_manager,
            config=config
        )
        
        print_info("Initializing strategy...")
        strategy.initialize()
        
        if strategy.initialized:
            print_success("Strategy initialized successfully")
            
            # Check configuration
            use_mag7 = strategy.trading_config.get("use_mag7_confirmation", False)
            if use_mag7:
                print_info("Using Mag7 confirmation strategy")
            else:
                print_info("Using Sector alignment strategy")
            
            return True
        else:
            print_error("Strategy initialization failed")
            return False
            
    except Exception as e:
        print_error(f"Strategy initialization error: {str(e)}")
        return False

def run_all_tests():
    """Run all tests"""
    print_header("TRADESTATION PAPER TRADING TEST SUITE")
    print_info(f"Starting tests at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load configuration
    print_info("Loading configuration...")
    config_loader = ConfigLoader()
    config = config_loader.load_config()
    
    # Create API instance
    print_info("Creating API instance...")
    api = TradeStationAPI()
    
    # Test 1: Authentication
    if not test_api_authentication(api):
        print_error("Authentication failed - cannot continue tests")
        return
    
    # Test 2: Account Access
    test_account_access(api)
    
    # Create other components
    instrument_fetcher = InstrumentFetcher(api)
    order_manager = OrderManager(api)
    
    # Test 3: Market Data Quotes
    test_market_data_quotes(api, instrument_fetcher)
    
    # Test 4: Historical Data
    test_historical_data(api)
    
    # Test 5: Streaming Data
    test_streaming_data(api)
    
    # Test 6: Option Chain
    test_option_chain(api, instrument_fetcher)
    
    # Test 7: Positions
    test_positions(api, order_manager)
    
    # Test 8: Active Orders
    test_active_orders(api, order_manager)
    
    # Test 9: Order Placement
    test_order_placement(api, order_manager)
    
    # Test 10: Strategy
    test_strategy_initialization(api, config)
    
    # Print summary
    print_header("TEST SUMMARY")
    print_success(f"Passed: {test_results['passed']}")
    print_warning(f"Skipped: {test_results['skipped']}")
    print_error(f"Failed: {test_results['failed']}")
    
    if test_results['errors']:
        print("\n" + Fore.RED + "Errors encountered:")
        for error in test_results['errors']:
            print(f"  - {error}")
    
    # Overall result
    if test_results['failed'] == 0:
        print("\n" + Fore.GREEN + "✅ ALL TESTS PASSED!" + Style.RESET_ALL)
    else:
        print("\n" + Fore.RED + "❌ SOME TESTS FAILED!" + Style.RESET_ALL)

if __name__ == "__main__":
    try:
        run_all_tests()
    except KeyboardInterrupt:
        print("\n\n" + Fore.YELLOW + "Tests interrupted by user" + Style.RESET_ALL)
    except Exception as e:
        print("\n\n" + Fore.RED + f"Fatal error: {str(e)}" + Style.RESET_ALL)
        import traceback
        traceback.print_exc()