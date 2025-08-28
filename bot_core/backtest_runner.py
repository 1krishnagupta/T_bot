# File: Code/bot_core/backtest_runner.py
# This is a NEW file - create it in your bot_core directory

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from bot_core.candle_data_client import CandleDataClient
from bot_core.backtest_engine import BacktestEngine
from bot_core.market_data_client import MarketDataClient
from bot_core.mongodb_handler import get_mongodb_handler

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bot_core.backtest_directory_manager import BacktestDirectoryManager


class ProfessionalBacktestRunner:
    """
    Professional backtesting system that works with your existing infrastructure
    """
    
    def __init__(self, config=None, api=None):
        """
        Initialize the backtest runner
        
        Args:
            config: Configuration dictionary
            api: TastyTrade API instance (optional)
        """
        self.config = config or {}
        self.api = api
        self.dir_manager = BacktestDirectoryManager()
        
        # Setup logging with file handler
        self.logger = logging.getLogger("BacktestRunner")
        self.logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        
        # Add file handler if not already present
        if not self.logger.handlers:
            today = datetime.now().strftime("%Y-%m-%d")
            log_file = os.path.join(log_folder, f"backtest_runner_{today}.log")
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
    def run_comprehensive_backtest(self, params: Dict) -> Dict:
        """
        Run a comprehensive backtest with your existing components
        
        Args:
            params: Dictionary with backtest parameters
                - symbols: List of symbols to test
                - timeframes: List of timeframes ('1m', '5m', '15m', 'All')
                - start_date: Start date (YYYY-MM-DD)
                - end_date: End date (YYYY-MM-DD)
                - data_source: 'TastyTrade' or 'YFinance'
                
        Returns:
            Dictionary with comprehensive results
        """

        print("="*80)
        print("STARTING PROFESSIONAL BACKTEST")
        print(f"Parameters: {params}")
        print("="*80)
        
        # Generate run ID
        run_id = self.dir_manager.generate_run_id()
        
        # Create a specific log file for this backtest run
        run_log_path = self.dir_manager.get_log_path(run_id)
        run_logger = logging.getLogger(f"BacktestRun_{run_id}")
        run_logger.setLevel(logging.INFO)
        
        # Add file handler for this specific run
        run_handler = logging.FileHandler(run_log_path)
        run_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        run_handler.setFormatter(run_formatter)
        run_logger.addHandler(run_handler)
        
        # Log initial parameters
        run_logger.info("="*80)
        run_logger.info(f"BACKTEST RUN ID: {run_id}")
        run_logger.info(f"Parameters: {params}")
        run_logger.info(f"Configuration: {self.config.get('trading_config', {})}")
        run_logger.info("="*80)
        
        # Extract parameters
        symbols = params.get('symbols', ['SPY', 'QQQ'])
        timeframes = params.get('timeframes', ['5m'])
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        data_source = params.get('data_source', 'YFinance')
        
        # Handle 'All' timeframes
        if 'All' in timeframes or timeframes == 'All':
            timeframes = ['1m', '5m', '15m']
        elif isinstance(timeframes, str):
            timeframes = [timeframes]
        
        # Create market data client (your existing component)
        # This preserves your MongoDB functionality
        streaming_token = {"token": "backtest_token", "dxlink-url": "mock_url"}
        market_data_client = MarketDataClient(
            api_quote_token=streaming_token,
            save_to_db=True,  # This enables MongoDB saving
            api=self.api
        )
        
        # Create candle data client (your existing component)
        candle_data_client = CandleDataClient(market_data_client)

        # Pass the API instance to candle_data_client if using TradeStation
        if data_source == "TradeStation" and self.api:
            if hasattr(candle_data_client, 'market_data') and candle_data_client.market_data:
                candle_data_client.market_data.api = self.api
        
        # Create backtest engine (your existing component)
        backtest_engine = BacktestEngine(
            candle_data_client=candle_data_client,
            jigsaw_strategy=None,  # Will be set by engine
            config=self.config  # Pass config in constructor
        )
        
        # IMPORTANT: Ensure config is properly set
        if self.config and 'trading_config' in self.config:
            backtest_engine.trading_config = self.config['trading_config']
            use_mag7 = backtest_engine.trading_config.get('use_mag7_confirmation', False)
            if use_mag7:
                print(f"[*] Backtest engine using Mag7 strategy with {backtest_engine.trading_config.get('mag7_threshold', 60)}% threshold")
            else:
                print(f"[*] Backtest engine using Sector strategy with {backtest_engine.trading_config.get('sector_weight_threshold', 43)}% threshold")

        # Set run ID and directory manager
        backtest_engine.run_id = run_id
        backtest_engine.dir_manager = self.dir_manager

        # Create run-specific logger for the backtest engine
        run_log_path = self.dir_manager.get_log_path(run_id)
        backtest_engine.run_logger = logging.getLogger(f"BacktestEngine_{run_id}")
        backtest_engine.run_logger.setLevel(logging.INFO)
        
        if not backtest_engine.run_logger.handlers:
            handler = logging.FileHandler(run_log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            backtest_engine.run_logger.addHandler(handler)
        
        # Results storage
        all_results = {}
        summary_stats = {
            'total_symbols': len(symbols),
            'total_timeframes': len(timeframes),
            'total_combinations': len(symbols) * len(timeframes),
            'successful_tests': 0,
            'failed_tests': 0,
            'total_trades': 0,
            'avg_win_rate': 0,
            'best_performer': None,
            'worst_performer': None
        }
        
        # Calculate date info
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            days_diff = (end_dt - start_dt).days
            
            print(f"\nDate range: {days_diff} days from {start_date} to {end_date}")
            
            # Log data source limitations
            if data_source == 'YFinance':
                print("\n⚠️  YFinance Data Limitations:")
                if '1m' in timeframes:
                    print("  - 1m data: Only last 7 days available")
                if '5m' in timeframes or '15m' in timeframes:
                    print("  - 5m/15m data: Only last 60 days available")
                print("  For full historical data, try other brokers API\n")
            
        except Exception as e:
            return {"error": f"Invalid date format: {e}"}
        
        # Run backtests
        print("\nRUNNING BACKTESTS")
        print("="*80)
        
        best_profit_factor = 0
        worst_drawdown = 0
        
        # Process each combination
        total_combinations = len(symbols) * len(timeframes)
        current_combo = 0
        
        for symbol in symbols:
            for timeframe in timeframes:
                current_combo += 1
                combo_key = f"{symbol}_{timeframe}"
                
                print(f"\n[{current_combo}/{total_combinations}] Testing {symbol} with {timeframe} timeframe...")
                
                try:
                    # Extract period number from timeframe
                    period = int(timeframe.replace('m', '')) if 'm' in timeframe else 60

                    # Run backtest using your existing engine
                    result = backtest_engine.run_backtest_for_ticker(
                        symbol, period, start_date, end_date, data_source=data_source
                    )
                    
                    # Store results
                    all_results[combo_key] = result
                    
                    # ONLY increment successful if there's no error
                    if 'error' not in result and 'Error' not in result:
                        summary_stats['successful_tests'] += 1
                    else:
                        summary_stats['failed_tests'] += 1
                    
                    if data_source == "TastyTrade" and self.api:
                        print(f"[*] Using TastyTrade API for data fetching")
                        candles = candle_data_client.fetch_historical_data_for_backtesting(
                            [symbol], period, start_date, end_date, 
                            data_source=data_source,
                            api=self.api  # Pass the API instance
                        )
                    
                    # Update summary statistics
                    if result.get('Total Trades', 0) > 0:
                        summary_stats['total_trades'] += result.get('Total Trades', 0)
                        
                        # Track best/worst performers
                        profit_factor = result.get('Profit Factor', 0)
                        max_dd = result.get('Max Drawdown', 0)
                        
                        if profit_factor > best_profit_factor:
                            best_profit_factor = profit_factor
                            summary_stats['best_performer'] = {
                                'symbol': combo_key,
                                'profit_factor': profit_factor,
                                'win_rate': result.get('Win Rate', 0)
                            }
                        
                        if max_dd > worst_drawdown:
                            worst_drawdown = max_dd
                            summary_stats['worst_performer'] = {
                                'symbol': combo_key,
                                'max_drawdown': max_dd,
                                'win_rate': result.get('Win Rate', 0)
                            }
                    
                    # Display quick results
                    trades = result.get('Total Trades', 0)
                    if trades == 0:
                        print(f"  ⚠️  No trades found")
                    else:
                        print(f"  ✓ Trades: {trades}, Win Rate: {result.get('Win Rate', 0):.1f}%, PF: {result.get('Profit Factor', 0):.2f}")
                    
                except Exception as e:
                    error_msg = f"  ✗ Error: {str(e)}"
                    print(error_msg)
                    summary_stats['failed_tests'] += 1
                    all_results[combo_key] = {"error": str(e)}
                    
                    # If it's a data source error, stop the entire backtest
                    if "authentication failed" in str(e).lower() or "failed to fetch data" in str(e).lower():
                        print(f"\n[!] Critical error with data source '{data_source}'. Stopping backtest.")
                        print("[!] Please check your credentials or try a different data source.")
                        break
        
        # Calculate average win rate
        valid_results = [r for r in all_results.values() if 'Win Rate' in r]
        if valid_results:
            summary_stats['avg_win_rate'] = sum(r['Win Rate'] for r in valid_results) / len(valid_results)
        
        # Generate report
        print("\n" + "="*80)
        print("GENERATING REPORT")
        print("="*80)
        
        report_path = self._generate_simple_report(run_id, all_results, summary_stats)
        
        # Display summary
        print(f"\n📊 Overall Statistics:")
        print(f"  - Total Combinations: {summary_stats['total_combinations']}")
        print(f"  - Successful: {summary_stats['successful_tests']}")
        print(f"  - Failed: {summary_stats['failed_tests']}")
        print(f"  - Total Trades: {summary_stats['total_trades']:,}")
        print(f"  - Average Win Rate: {summary_stats['avg_win_rate']:.1f}%")
        
        if summary_stats['best_performer']:
            print(f"\n🏆 Best: {summary_stats['best_performer']['symbol']} (PF: {summary_stats['best_performer']['profit_factor']:.2f})")
        
        if summary_stats['worst_performer']:
            print(f"📉 Worst: {summary_stats['worst_performer']['symbol']} (DD: {summary_stats['worst_performer']['max_drawdown']:.1f}%)")
        
        return {
            'run_id': run_id,
            'results': all_results,
            'summary': summary_stats,
            'report_path': report_path,
            'log_path': self.dir_manager.get_log_path(run_id)
        }
    
    def _generate_simple_report(self, run_id: str, results: Dict, summary: Dict) -> str:
        """Generate a simple CSV report"""
        import csv
        
        report_path = os.path.join(self.dir_manager.results_dir, 'Summary', f"{run_id}_summary.csv")
        
        with open(report_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'Symbol/Timeframe', 'Win Rate (%)', 'Profit Factor', 
                'Max Drawdown (%)', 'Total Trades', 'Final Equity'
            ])
            
            # Data rows
            for key, result in results.items():
                if 'error' not in result:
                    writer.writerow([
                        key,
                        result.get('Win Rate', 0),
                        result.get('Profit Factor', 0),
                        result.get('Max Drawdown', 0),
                        result.get('Total Trades', 0),
                        result.get('Final Equity', 10000)
                    ])
        
        print(f"\n📄 Report saved to: {report_path}")
        return report_path