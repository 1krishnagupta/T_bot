# Code/bot_core/backtest_directory_manager.py

import os
from datetime import datetime
from pathlib import Path

class BacktestDirectoryManager:
    """Manages the directory structure for backtest data and results"""
    
    def __init__(self):
        # Base directory for all backtest data
        self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Backtest_Data'))
        
        # Subdirectories
        self.historical_data_dir = os.path.join(self.base_dir, 'Historical_Data')
        self.results_dir = os.path.join(self.base_dir, 'Results')
        self.analysis_dir = os.path.join(self.base_dir, 'Analysis')
        self.logs_dir = os.path.join(self.base_dir, 'Logs')
        
        # Create directory structure
        self._create_directories()
        
    def _create_directories(self):
        """Create the directory structure if it doesn't exist"""
        directories = [
            self.base_dir,
            self.historical_data_dir,
            os.path.join(self.historical_data_dir, 'YFinance'),
            os.path.join(self.historical_data_dir, 'TastyTrade'),  # Changed from 'Alpaca'
            self.results_dir,
            os.path.join(self.results_dir, 'Summary'),
            os.path.join(self.results_dir, 'Trades'),
            self.analysis_dir,
            self.logs_dir
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
        # Create README files to explain directory structure
        self._create_readme_files()
    
    def _create_readme_files(self):
        """Create README files in each directory"""
        readme_content = {
            self.base_dir: """# Backtest Data Directory

This directory contains all backtest-related data and results.

## Structure:
- **Historical_Data/**: Raw market data from different sources
  - YFinance/: Data fetched from Yahoo Finance
  - Alpaca/: Data fetched from Alpaca Markets
- **Results/**: Backtest results and performance metrics
  - Summary/: Summary statistics for each backtest
  - Trades/: Detailed trade-by-trade results
- **Analysis/**: Detailed analysis files and charts
- **Logs/**: Backtest execution logs
""",
            self.historical_data_dir: """# Historical Data

This directory stores raw market data fetched from different sources.

## File Format:
Files are saved as: `{SYMBOL}_{TIMEFRAME}_{START_DATE}_{END_DATE}_{SOURCE}.csv`

Example: `SPY_5m_2024-01-01_2024-01-31_TastyTrade.csv`
""",
            self.results_dir: """# Backtest Results

This directory contains backtest results and performance metrics.

## Subdirectories:
- **Summary/**: High-level statistics and performance metrics
- **Trades/**: Detailed trade-by-trade logs with entry/exit points
"""
        }
        
        for directory, content in readme_content.items():
            readme_path = os.path.join(directory, 'README.md')
            if not os.path.exists(readme_path):
                with open(readme_path, 'w') as f:
                    f.write(content)
    
    def get_historical_data_path(self, symbol, timeframe, start_date, end_date, source):
        """Get the path for historical data file"""
        source_dir = os.path.join(self.historical_data_dir, source)
        
        # Create source directory if it doesn't exist
        os.makedirs(source_dir, exist_ok=True)
        
        # Convert dates to string format if they're date objects
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y-%m-%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y-%m-%d')
        
        filename = f"{symbol}_{timeframe}_{start_date}_{end_date}_{source}.csv"
        return os.path.join(source_dir, filename)
    
    def get_results_path(self, run_id, result_type='summary'):
        """Get the path for results file"""
        if result_type == 'summary':
            directory = os.path.join(self.results_dir, 'Summary')
        else:
            directory = os.path.join(self.results_dir, 'Trades')
        
        filename = f"{run_id}_{result_type}.csv"
        return os.path.join(directory, filename)
    
    def get_analysis_path(self, run_id, analysis_type):
        """Get the path for analysis file"""
        # Ensure analysis_type doesn't already have .csv extension
        if analysis_type.endswith('.csv'):
            filename = f"{run_id}_{analysis_type}"
        else:
            filename = f"{run_id}_{analysis_type}.csv"
        return os.path.join(self.analysis_dir, filename)
    
    def get_log_path(self, run_id):
        """Get the path for log file"""
        filename = f"{run_id}_backtest.log"
        return os.path.join(self.logs_dir, filename)
    
    def generate_run_id(self):
        """Generate a unique run ID for this backtest"""
        return datetime.now().strftime('%Y%m%d_%H%M%S')