# File: Code/bot_core/backtest_engine.py

import os
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import logging
import csv
from pathlib import Path
from Code.bot_core.mag7_strategy import Mag7Strategy

class BacktestEngine:
    """
    Engine for running backtests on historical market data
    """
    
    def __init__(self, candle_data_client=None, jigsaw_strategy=None, config=None):
        """
        Initialize the backtest engine
        
        Args:
            candle_data_client: CandleDataClient for fetching historical candles
            jigsaw_strategy: JigsawStrategy for strategy logic
            config: Configuration dictionary
        """
        self.candle_data_client = candle_data_client
        self.jigsaw_strategy = jigsaw_strategy
        self.config = config or {}
        self.trading_config = self.config.get('trading_config', {})
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"backtest_engine_{today}.log")
        
        self.logger = logging.getLogger("BacktestEngine")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            # Initialize directory manager
            from Code.bot_core.backtest_directory_manager import BacktestDirectoryManager
            self.dir_manager = BacktestDirectoryManager()
            self.run_id = self.dir_manager.generate_run_id()
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def run_backtest(self, tickers, period, start_date, end_date, data_source="YFinance"):
        """
        Run a backtest for the specified tickers and period
        
        Args:
            tickers (list): List of tickers to backtest
            period (int): Timeframe period in minutes
            start_date (str): Start date in ISO format
            end_date (str): End date in ISO format
            data_source (str): Data source to use ('YFinance' or 'TastyTrade')
            
        Returns:
            dict: Backtest results by ticker
        """
        results = {}
        
        for ticker in tickers:
            self.logger.info(f"Running backtest for {ticker} with {period}m candles...")
            print(f"[*] Running backtest for {ticker} with {period}m candles...")
            result = self.run_backtest_for_ticker(ticker, period, start_date, end_date, data_source)
            results[f"{ticker}_{period}m"] = result
            
        return results
    
    

    
    def run_backtest_for_ticker(self, symbol, period, start_date, end_date, data_source="YFinance"):
        """
        Run backtest for a single ticker and period with detailed analysis
        
        Args:
            symbol (str): Instrument symbol
            period (int): Candle period in minutes
            start_date (str): Start date in ISO format
            end_date (str): End date in ISO format
                
        Returns:
            dict: Backtest results
        """
        try:
            print(f"[*] Starting backtest for {symbol} with {period}m candles...")
            print(f"[*] Date range: {start_date} to {end_date}")
            print(f"[*] Data source: {data_source}") 
            
            # Ensure trading_config is set
            if not hasattr(self, 'trading_config') or not self.trading_config:
                self.trading_config = self.config.get('trading_config', {})
                print(f"[*] Using trading config: sector_weight_threshold={self.trading_config.get('sector_weight_threshold', 43)}%")
            
            # IMPORTANT: Update the trading config with latest values from UI
            if self.config and 'trading_config' in self.config:
                self.trading_config = self.config['trading_config']
                
                # Check which strategy is being used - DEFINE strategy_name HERE
                use_mag7 = self.trading_config.get('use_mag7_confirmation', False)
                strategy_name = "Magnificent 7 (Mag7)" if use_mag7 else "Sector Alignment"

                # Print configuration table only once per backtest run
                if not hasattr(self, '_config_table_shown'):
                    self._config_table_shown = True
                    self._print_config_table(strategy_name, use_mag7)
                
            # Get configuration values
            use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
            
            # Get historical candle data with better error handling
            if not self.candle_data_client:
                self.logger.error("No candle data client provided")
                return self._get_empty_result()
            
            # Pass config to candle_data_client
            self.candle_data_client.config = self.config
                
            # Fetch data from the selected source
            candles = self.candle_data_client.get_candles_for_backtesting(
                [symbol], 
                period,
                start_date,
                end_date,
                data_source=data_source
            ).get(symbol, [])
            
            if not candles:
                self.logger.warning(f"No historical data found for {symbol}")
                return self._get_empty_result()
            
            print(f"[✓] Fetched {len(candles)} candles for {symbol}")
            
            # Log date range of fetched data
            if candles:
                first_date = candles[0].get('timestamp', candles[0].get('start_time', 'Unknown'))
                last_date = candles[-1].get('timestamp', candles[-1].get('start_time', 'Unknown'))
                print(f"[*] Data range: {first_date} to {last_date}")
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(candles)
            
            # Standardize column names
            col_map = {
                'Open': 'open', 
                'High': 'high', 
                'Low': 'low', 
                'Close': 'close',
                'Volume': 'volume'
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            
            # Ensure all required columns exist
            required_cols = ['open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return self._get_empty_result()
            
            # Convert data to appropriate types
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            else:
                df['volume'] = 0
                
            # Add timestamp column if not present
            if 'timestamp' not in df.columns:
                if 'start_time' in df.columns:
                    df['timestamp'] = df['start_time']
                else:
                    # Generate timestamps based on period
                    start_dt = pd.to_datetime(start_date)
                    df['timestamp'] = pd.date_range(start=start_dt, periods=len(df), freq=f'{period}min')
            
            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Log DataFrame info
            print(f"[*] DataFrame shape: {df.shape}")
            print(f"[*] DataFrame date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            # Fetch additional data based on strategy
            print(f"[*] Checking strategy configuration...")
            use_mag7 = self.trading_config.get("use_mag7_confirmation", False)

            # Initialize sector_data BEFORE the loop
            sector_data = {}

            if use_mag7:
                # Fetch Mag7 stock data
                print(f"[*] Using Mag7 confirmation strategy")
                mag7_stocks = self.trading_config.get("mag7_stocks", 
                    ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
                print(f"[*] Fetching Mag7 data using {data_source}...")
                
                mag7_result = self.candle_data_client.fetch_historical_data_for_backtesting(
                    mag7_stocks, period, start_date, end_date,
                    data_source=data_source
                )
                
                # Only show processing message once
                if not hasattr(self, '_shown_mag7_processing'):
                    self._shown_mag7_processing = True
                    print(f"[*] Processing Mag7 stock data...")
                
                for stock in mag7_stocks:
                    stock_candles = mag7_result.get(stock, [])
                    
                    if stock_candles:
                        stock_df = pd.DataFrame(stock_candles)
                        
                        # Standardize column names
                        col_map = {
                            'Open': 'open', 
                            'High': 'high', 
                            'Low': 'low', 
                            'Close': 'close',
                            'Volume': 'volume'
                        }
                        stock_df = stock_df.rename(columns={k: v for k, v in col_map.items() if k in stock_df.columns})
                        
                        # Convert to numeric
                        for col in ['open', 'high', 'low', 'close']:
                            if col in stock_df.columns:
                                stock_df[col] = pd.to_numeric(stock_df[col], errors='coerce')
                        
                        # Handle timestamp
                        if 'timestamp' not in stock_df.columns:
                            if 'start_time' in stock_df.columns:
                                stock_df['timestamp'] = stock_df['start_time']
                        
                        if 'timestamp' in stock_df.columns:
                            stock_df['timestamp'] = pd.to_datetime(stock_df['timestamp'])
                            stock_df = stock_df.set_index('timestamp')
                            stock_df = stock_df.reindex(df.set_index('timestamp').index, method='ffill')
                            stock_df = stock_df.reset_index()
                        
                        sector_data[stock] = stock_df
                        print(f"[✓] Got {len(stock_df)} candles for {stock}")
                    else:
                        print(f"[!] No data for {stock}")
            else:
                # Original sector ETF fetching logic
                print(f"[*] Using sector confirmation strategy")
                print(f"[*] Fetching sector ETF data using {data_source}...")
                sectors = self.trading_config.get("sector_etfs", ["XLK", "XLF", "XLV", "XLY"])
                selected_sectors = self.trading_config.get("selected_sectors", sectors)
                
                # Fetch only selected sectors
                print(f"[*] Fetching data for selected sectors: {selected_sectors}")
                sector_result = self.candle_data_client.fetch_historical_data_for_backtesting(
                    selected_sectors, period, start_date, end_date,
                    data_source=data_source
                )
                
                for sector in selected_sectors:
                    print(f"[*] Processing data for sector {sector}...")
                    sector_candles = sector_result.get(sector, [])
                    
                    if sector_candles:
                        sector_df = pd.DataFrame(sector_candles)
                        
                        # Standardize column names
                        col_map = {
                            'Open': 'open', 
                            'High': 'high', 
                            'Low': 'low', 
                            'Close': 'close',
                            'Volume': 'volume'
                        }
                        sector_df = sector_df.rename(columns={k: v for k, v in col_map.items() if k in sector_df.columns})
                        
                        # Convert to numeric
                        for col in ['open', 'high', 'low', 'close']:
                            if col in sector_df.columns:
                                sector_df[col] = pd.to_numeric(sector_df[col], errors='coerce')
                        
                        # Add timestamp if needed
                        if 'timestamp' not in sector_df.columns:
                            if 'start_time' in sector_df.columns:
                                sector_df['timestamp'] = sector_df['start_time']
                        
                        # Ensure timestamp is datetime format
                        if 'timestamp' in sector_df.columns:
                            sector_df['timestamp'] = pd.to_datetime(sector_df['timestamp'])
                            # Align timestamps with main DataFrame
                            sector_df = sector_df.set_index('timestamp')
                            sector_df = sector_df.reindex(df.set_index('timestamp').index, method='ffill')
                            sector_df = sector_df.reset_index()
                        
                        sector_data[sector] = sector_df
                        print(f"[✓] Got {len(sector_df)} candles for sector {sector}")
                    else:
                        print(f"[!] No data for sector {sector}")

            # Ensure data is properly aligned
            print(f"\n[*] Data alignment check:")
            print(f"  - Main ticker ({symbol}): {len(df)} candles")
            for name, data_df in sector_data.items():
                print(f"  - {name}: {len(data_df)} candles")
                if len(data_df) != len(df):
                    print(f"    [!] WARNING: Data length mismatch!")

            # Initialize sector_weights HERE - BEFORE the main loop
            # Get sector weights from config (used for both strategies)
            sector_weights = self.trading_config.get("sector_weights", {
                "XLK": 32,
                "XLF": 14,
                "XLV": 11,
                "XLY": 11
            })

            # Calculate technical indicators first
            print(f"[*] Calculating technical indicators...")

            # 1. Calculate EMAs
            df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
            df['ema15'] = df['close'].ewm(span=15, adjust=False).mean()

            # 2. Calculate VWAP
            df['vwap'] = self._calculate_vwap(df)

            # 3. Calculate Bollinger Bands
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            df['bb_std'] = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
            df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']

            # 4. Calculate Stochastic
            df['stoch_k'], df['stoch_d'] = self._calculate_stochastic_full(df)

            # 5. Calculate ATR
            df['atr'] = self._calculate_atr_series(df)

            # Calculate Heiken Ashi
            ha_df = self._calculate_heiken_ashi(df)

            # Initialize tracking
            trades = []
            analysis_data = []

            # Initial equity
            initial_equity = 10
            current_equity = initial_equity

            # Initialize best_method and all_method_stats
            best_method = None
            all_method_stats = {}

            # Test different trailing methods
            trailing_methods = [
                "Heiken Ashi Candle Trail (1-3 candle lookback)",
                "EMA Trail (e.g., EMA(9) trailing stop)",
                "% Price Trail (e.g., 1.5% below current price)",
                "ATR-Based Trail (1.5x ATR)",
                "Fixed Tick/Point Trail (custom value)"
            ]

            method_results = {}
            for method in trailing_methods:
                method_results[method] = {
                    "trades": [],
                    "win_count": 0,
                    "loss_count": 0,
                    "total_profit": 0,
                    "total_loss": 0,
                    "max_drawdown": 0,
                    "equity_curve": [initial_equity]
                }

            # Process ALL candles (start from 0 for complete analysis)
            print(f"[*] Analyzing {len(df)} candles for trade signals...")

            # Process candles from the beginning with minimal warmup
            warmup = min(30, len(df) // 10)  # Use 10% of data or 30, whichever is smaller

            # Track active trades
            active_trades = {}

            # Add some debugging counters
            alignment_count = 0  # Generic counter for any alignment strategy
            compression_count = 0
            momentum_aligned_count = 0
            trend_aligned_count = 0
            entry_signal_count = 0
            trade_count = 0

            # NOW START THE MAIN LOOP - sector_data is already defined
            # Analyze ALL candles including warmup period
            for i in range(len(df)):
                current_time = df.iloc[i]['timestamp'] if 'timestamp' in df.columns else i
                
                # Create analysis record for EVERY candle
                analysis_record = {
                    'candle_idx': i,
                    'timestamp': str(current_time),
                    'open': float(df.iloc[i]['open']),
                    'high': float(df.iloc[i]['high']),
                    'low': float(df.iloc[i]['low']),
                    'close': float(df.iloc[i]['close']),
                    'volume': float(df.iloc[i]['volume']) if 'volume' in df.columns else 0,
                    'ema9': float(df.iloc[i]['ema9']) if pd.notna(df.iloc[i]['ema9']) else None,
                    'ema15': float(df.iloc[i]['ema15']) if pd.notna(df.iloc[i]['ema15']) else None,
                    'vwap': float(df.iloc[i]['vwap']) if pd.notna(df.iloc[i]['vwap']) else None,
                    'bb_width': float(df.iloc[i]['bb_width']) if pd.notna(df.iloc[i]['bb_width']) else None,
                    'stoch_k': float(df.iloc[i]['stoch_k']) if pd.notna(df.iloc[i]['stoch_k']) else None,
                    'stoch_d': float(df.iloc[i]['stoch_d']) if pd.notna(df.iloc[i]['stoch_d']) else None,
                    'atr': float(df.iloc[i]['atr']) if pd.notna(df.iloc[i]['atr']) else None,
                    'sector_aligned': False,
                    'sector_direction': 'neutral',
                    'compression_detected': False,
                    'entry_signal': None,
                    'trade_entered': False,
                    'equity': current_equity
                }
                
                # Only check for trading signals after warmup period
                if i >= warmup and i < len(df) - 1:
                    # 1. Check alignment based on strategy type
                    use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
                    
                    # Initialize alignment variables
                    aligned = False
                    direction = "neutral"
                    alignment_value = 0
                    
                    if use_mag7:
                        # Check Mag7 alignment
                        aligned, direction, alignment_value = self._check_mag7_alignment(sector_data, i)
                        analysis_record['sector_aligned'] = aligned  # Keep field name for compatibility
                        analysis_record['sector_direction'] = direction
                        analysis_record['sector_weight'] = alignment_value if aligned else 0
                        
                        if aligned:
                            alignment_count += 1
                            
                        
                        if not aligned:
                            analysis_record['skip_reason'] = f'No Mag7 alignment (aligned={alignment_value:.1f}%, threshold={self.trading_config.get("mag7_threshold", 60)}%)'
                    else:
                        # Check Sector alignment (original ETF-based logic)
                        aligned, direction, alignment_value = self._check_sector_alignment(sector_data, i, sector_weights)
                        analysis_record['sector_aligned'] = aligned
                        analysis_record['sector_direction'] = direction
                        analysis_record['sector_weight'] = alignment_value if aligned else 0
                        
                        if aligned:
                            alignment_count += 1
                            print(f"  [✓] Candle {i}: Sector aligned ({direction}, weight={alignment_value}%)")
                        else:
                            if i % 100 == 0:  # Log every 100th candle to avoid spam
                                print(f"  [✗] Candle {i}: No sector alignment")
                        
                        if not aligned:
                            analysis_record['skip_reason'] = f'No sector alignment (weight={alignment_value}%, threshold={self.trading_config.get("sector_weight_threshold", 43)}%)'
                    
                    # Continue with common logic for both strategies
                    if aligned:
                        # 2. Check for Compression
                        compression_detected, comp_direction = self._detect_compression(df, i)
                        analysis_record['compression_detected'] = compression_detected
                        analysis_record['compression_direction'] = comp_direction if compression_detected else 'neutral'
                        
                        if compression_detected:
                            compression_count += 1
                        
                        if not compression_detected or comp_direction != direction:
                            analysis_record['skip_reason'] = f'No compression or direction mismatch (compression={comp_direction}, {strategy_name.lower()}={direction})'
                        else:
                            # 3. Check Momentum
                            stoch_aligned = False
                            if direction == "bullish" and df.iloc[i]['stoch_k'] > 20:
                                stoch_aligned = True
                            elif direction == "bearish" and df.iloc[i]['stoch_k'] < 80:
                                stoch_aligned = True
                            
                            analysis_record['momentum_aligned'] = stoch_aligned
                            
                            if stoch_aligned:
                                momentum_aligned_count += 1
                            
                            if not stoch_aligned:
                                if direction == "bullish":
                                    analysis_record['skip_reason'] = f'Momentum not aligned (Stoch K={df.iloc[i]["stoch_k"]:.1f} <= 20)'
                                else:
                                    analysis_record['skip_reason'] = f'Momentum not aligned (Stoch K={df.iloc[i]["stoch_k"]:.1f} >= 80)'
                            else:
                                # 3b. Price relative to VWAP and EMA15
                                trend_aligned = False
                                if pd.notna(df.iloc[i]['vwap']) and pd.notna(df.iloc[i]['ema15']):
                                    if direction == "bullish" and df.iloc[i]['close'] > df.iloc[i]['vwap'] and df.iloc[i]['close'] > df.iloc[i]['ema15']:
                                        trend_aligned = True
                                    elif direction == "bearish" and df.iloc[i]['close'] < df.iloc[i]['vwap'] and df.iloc[i]['close'] < df.iloc[i]['ema15']:
                                        trend_aligned = True
                                
                                analysis_record['trend_aligned'] = trend_aligned
                                
                                if trend_aligned:
                                    trend_aligned_count += 1
                                
                                if not trend_aligned:
                                    if direction == "bullish":
                                        analysis_record['skip_reason'] = f'Trend not aligned (Price={df.iloc[i]["close"]:.2f} must be > VWAP={df.iloc[i]["vwap"]:.2f} and EMA15={df.iloc[i]["ema15"]:.2f})'
                                    else:
                                        analysis_record['skip_reason'] = f'Trend not aligned (Price={df.iloc[i]["close"]:.2f} must be < VWAP={df.iloc[i]["vwap"]:.2f} and EMA15={df.iloc[i]["ema15"]:.2f})'
                                else:
                                    # 4. Check Entry Trigger
                                    if i < len(ha_df):
                                        entry_signal = self._check_entry_signal(df.iloc[i-1], df.iloc[i], ha_df.iloc[i])
                                        
                                        # Debug: Log why entry signal might be None
                                        if entry_signal is None and i % 100 == 0:  # Log every 100th candle
                                            ha_candle = ha_df.iloc[i]
                                            print(f"  [DEBUG] No HA signal at candle {i}: HA_open={ha_candle['open']:.4f}, HA_close={ha_candle['close']:.4f}, HA_low={ha_candle['low']:.4f}, HA_high={ha_candle['high']:.4f}")
                                        
                                        analysis_record['entry_signal'] = entry_signal
                                        
                                        if entry_signal:
                                            entry_signal_count += 1
                                        
                                        if not entry_signal or entry_signal != direction:
                                            analysis_record['skip_reason'] = f'No entry signal (HA signal={entry_signal}, expected={direction})'
                                        else:
                                            # Valid trade signal found!
                                            analysis_record['trade_entered'] = True
                                            analysis_record['trade_direction'] = entry_signal
                                            trade_count += 1
                                            
                                            # Log the trade entry to logger only, not console
                                            self.logger.info(f"TRADE ENTERED at candle {i}: {direction.upper()} @ ${df.iloc[i]['close']:.2f}")
                                            
                                            # Simulate trades for each trailing method
                                            for method in trailing_methods:
                                                # Simulate trade
                                                exit_idx, exit_price, exit_reason = self._simulate_trade_with_method(
                                                    df, ha_df, i, direction, method
                                                )
                                                
                                                # Calculate P&L
                                                entry_price = float(df.iloc[i]['close'])
                                                if direction == "bullish":
                                                    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                                                else:
                                                    pnl_pct = ((entry_price - exit_price) / entry_price) * 100
                                                
                                                pnl_dollars = (pnl_pct / 100) * current_equity
                                                
                                                # Create trade record
                                                trade = {
                                                    "symbol": symbol,
                                                    "method": method,
                                                    "direction": direction,
                                                    "entry_idx": i,
                                                    "entry_time": str(df.iloc[i]['timestamp']),
                                                    "entry_price": entry_price,
                                                    "exit_idx": exit_idx,
                                                    "exit_time": str(df.iloc[exit_idx]['timestamp']) if exit_idx < len(df) else str(df.iloc[-1]['timestamp']),
                                                    "exit_price": exit_price,
                                                    "exit_reason": exit_reason,
                                                    "pnl_pct": round(pnl_pct, 2),
                                                    "pnl_dollars": round(pnl_dollars, 2),
                                                    "contract_price": entry_price * 0.006  # Approximate 0.60 delta option price
                                                }
                                                
                                                # Update method results
                                                method_results[method]["trades"].append(trade)
                                                if pnl_pct > 0:
                                                    method_results[method]["win_count"] += 1
                                                    method_results[method]["total_profit"] += pnl_dollars
                                                else:
                                                    method_results[method]["loss_count"] += 1
                                                    method_results[method]["total_loss"] += abs(pnl_dollars)
                                                
                                                # Update equity curve
                                                new_equity = method_results[method]["equity_curve"][-1] + pnl_dollars
                                                method_results[method]["equity_curve"].append(new_equity)
                                            
                                            # Store the best trade for overall tracking
                                            best_pnl = -float('inf')
                                            best_trade = None
                                            for method in trailing_methods:
                                                if method_results[method]["trades"]:
                                                    last_trade = method_results[method]["trades"][-1]
                                                    if last_trade["pnl_dollars"] > best_pnl:
                                                        best_pnl = last_trade["pnl_dollars"]
                                                        best_trade = last_trade
                                            
                                            if best_trade:
                                                trades.append(best_trade)
                                                current_equity += best_trade["pnl_dollars"]
                else:
                    analysis_record['skip_reason'] = 'Warmup period' if i < warmup else 'End of data'
                                
                # Add record to analysis data
                analysis_data.append(analysis_record)
            
            print(f"[✓] Analysis complete. Processed {len(analysis_data)} candles.")
            
            # Print statistics with correct labels
            strategy_type = "Mag7" if use_mag7 else "Sector"
            print(f"\n[*] Signal Statistics:")
            print(f"  - {strategy_type} Aligned: {alignment_count} times")
            print(f"  - Compression Detected: {compression_count} times")
            print(f"  - Momentum Aligned: {momentum_aligned_count} times")
            print(f"  - Trend Aligned: {trend_aligned_count} times")
            print(f"  - Entry Signals: {entry_signal_count} times")
            print(f"  - Trades Entered: {trade_count} times")
            
            # Add debug info about why trades weren't entered
            if trade_count == 0:
                print(f"\n[!] No trades found. Debugging info:")
                # Use the correct data variable based on strategy
                data_count = len(sector_data) if sector_data else 0
                print(f"  - {strategy_type} data available: {data_count} symbols")
                
                if use_mag7:
                    print(f"  - Mag7 threshold: {self.trading_config.get('mag7_threshold', 60)}%")
                    print(f"  - Mag7 stocks configured: {self.trading_config.get('mag7_stocks', ['AAPL', 'MSFT', 'AMZN', 'NVDA', 'GOOG', 'TSLA', 'META'])}")
                else:
                    print(f"  - Sector weight threshold: {self.trading_config.get('sector_weight_threshold', 43)}%")
                    print(f"  - Sectors configured: {self.trading_config.get('selected_sectors', ['XLK', 'XLF', 'XLV', 'XLY'])}")
                
                print(f"  - Compression threshold: {self.trading_config.get('bb_width_threshold', 0.05)}")
                print(f"  - Donchian threshold: {self.trading_config.get('donchian_contraction_threshold', 0.6)}")
                print(f"  - Volume squeeze threshold: {self.trading_config.get('volume_squeeze_threshold', 0.3)}")
                
                # Sample some data to see what's happening
                if len(analysis_data) > 100:
                    sample_idx = len(analysis_data) // 2
                    sample = analysis_data[sample_idx]
                    print(f"\n  Sample candle analysis (idx {sample_idx}):")
                    print(f"    - Close: {sample.get('close', 'N/A')}")
                    print(f"    - BB Width: {sample.get('bb_width', 'N/A')}")
                    print(f"    - Alignment detected: {sample.get('alignment_detected', False)}")
                    print(f"    - Compression: {sample.get('compression_detected', False)}")
                    print(f"    - Skip reason: {sample.get('skip_reason', 'N/A')}")
                
                # # Check if alignment data is valid
                # for symbol_name, data_df in sector_data.items():
                #     if isinstance(data_df, pd.DataFrame) and not data_df.empty:
                #         print(f"  - {symbol_name}: {len(data_df)} candles, date range: {data_df.iloc[0]['timestamp']} to {data_df.iloc[-1]['timestamp']}")
                #     else:
                #         print(f"  - {symbol_name}: No data!")
            
            # Determine best trailing method
            best_profit_factor = 0
            for method, results in method_results.items():
                if results["win_count"] > 0 or results["loss_count"] > 0:
                    win_rate = (results["win_count"] / (results["win_count"] + results["loss_count"])) * 100
                    profit_factor = results["total_profit"] / max(results["total_loss"], 1)
                    max_dd = self._calculate_max_drawdown(results["equity_curve"])
                    final_equity = results["equity_curve"][-1]
                    
                    all_method_stats[method] = {
                        "win_rate": win_rate,
                        "profit_factor": profit_factor,
                        "max_drawdown": max_dd,
                        "total_trades": results["win_count"] + results["loss_count"],
                        "winning_trades": results["win_count"],
                        "losing_trades": results["loss_count"],
                        "total_profit": results["total_profit"],
                        "total_loss": results["total_loss"],
                        "final_equity": final_equity
                    }
                    
                    if profit_factor > best_profit_factor:
                        best_profit_factor = profit_factor
                        best_method = method
                
            # Create run-specific logger if not exists
            if not hasattr(self, 'run_id'):
                self.run_id = self.dir_manager.generate_run_id()
            
            if not hasattr(self, 'run_logger'):
                run_log_path = self.dir_manager.get_log_path(self.run_id)
                self.run_logger = logging.getLogger(f"BacktestEngine_{self.run_id}")
                self.run_logger.setLevel(logging.INFO)
                
                # Add handler if not present
                if not self.run_logger.handlers:
                    handler = logging.FileHandler(run_log_path)
                    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                    handler.setFormatter(formatter)
                    self.run_logger.addHandler(handler)
            
            # Log the analysis summary
            self.run_logger.info(f"\nBacktest Summary for {symbol}:")
            self.run_logger.info(f"  Period: {period}m")
            self.run_logger.info(f"  Date Range: {start_date} to {end_date}")
            self.run_logger.info(f"  Total Candles: {len(df)}")
            self.run_logger.info(f"  Strategy: {strategy_name}")
            self.run_logger.info(f"  Alignment Count: {alignment_count}")
            self.run_logger.info(f"  Compression Count: {compression_count}")
            self.run_logger.info(f"  Entry Signals: {entry_signal_count}")
            self.run_logger.info(f"  Trades Entered: {trade_count}")
            
            # Save analysis data
            try:
                # Ensure we have the directory manager and run_id
                if not hasattr(self, 'dir_manager'):
                    from Code.bot_core.backtest_directory_manager import BacktestDirectoryManager
                    self.dir_manager = BacktestDirectoryManager()
                
                if not hasattr(self, 'run_id'):
                    self.run_id = self.dir_manager.generate_run_id()
                
                # Get strategy type for filename
                use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
                strategy_suffix = "mag7" if use_mag7 else "sector"
                
                # Generate analysis path with strategy name
                analysis_filename = f"{symbol}_{period}min_{strategy_suffix}_analysis"
                analysis_path = self.dir_manager.get_analysis_path(self.run_id, analysis_filename)
                
                # Make sure the directory exists
                os.makedirs(os.path.dirname(analysis_path), exist_ok=True)
                
                # Save analysis data
                self._save_analysis_to_csv(analysis_data, analysis_path)
                
                print(f"[✓] Saved {strategy_suffix} strategy analysis ({len(analysis_data)} records) to {analysis_path}")
                self.logger.info(f"Saved {strategy_suffix} analysis to {analysis_path}")
                
                # Also save to run logger if available
                if hasattr(self, 'run_logger'):
                    self.run_logger.info(f"{strategy_suffix.capitalize()} analysis saved: {analysis_path} ({len(analysis_data)} records)")
                
            except Exception as e:
                print(f"[✗] Error saving analysis data: {str(e)}")
                self.logger.error(f"Error saving analysis data: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # Save trades data
            try:
                # Ensure we have run_id
                if not hasattr(self, 'run_id'):
                    self.run_id = self.dir_manager.generate_run_id()
                    
                # Create a unique filename for each symbol
                trades_filename = f"trades_{symbol}_{period}min"
                trades_path = self.dir_manager.get_results_path(self.run_id, trades_filename)
                os.makedirs(os.path.dirname(trades_path), exist_ok=True)
                self._save_trades_to_csv(trades, trades_path)
                print(f"[✓] Saved trades to {trades_path}")
                self.logger.info(f"Saved trades to {trades_path}")
                self.run_logger.info(f"Trades saved: {trades_path} ({len(trades)} trades)")
            except Exception as e:
                print(f"[✗] Error saving trades data: {str(e)}")
                self.logger.error(f"Error saving trades data: {str(e)}")

            # Return results - handle case where no trades were found
            if best_method and best_method in all_method_stats:
                stats = all_method_stats[best_method]
                result = {
                    "Win Rate": round(stats['win_rate'], 2),
                    "Profit Factor": round(stats['profit_factor'], 2),
                    "Max Drawdown": round(stats['max_drawdown'], 2),
                    "Total Trades": stats['total_trades'],
                    "Winning Trades": stats['winning_trades'],
                    "Losing Trades": stats['losing_trades'],
                    "Gross Profit": round(stats['total_profit'], 2),
                    "Gross Loss": round(stats['total_loss'], 2),
                    "Final Equity": round(stats['final_equity'], 2),
                    "Optimal Trailing Method": best_method,
                    "All Methods": all_method_stats,
                    "Trades": trades,
                    "Strategy": strategy_suffix  # Add strategy to results
                }
                
                return result
            else:
                # No trades found - return empty result with debugging info
                empty_result = self._get_empty_result()
                empty_result["Debug Info"] = {
                    "Total Candles": len(df),
                    "Strategy": strategy_suffix,
                    f"{strategy_suffix.capitalize()} Aligned": alignment_count,
                    "Compression Detected": compression_count,
                    "Momentum Aligned": momentum_aligned_count,
                    "Trend Aligned": trend_aligned_count,
                    "Entry Signals": entry_signal_count,
                    "Trades": trade_count
                }
                print(f"\n[!] No trades found using {strategy_suffix} strategy. Check the debug info above.")
                return empty_result
                
        except Exception as e:
            self.logger.error(f"Error in backtest for {symbol}: {str(e)}")
            print(f"[✗] Backtest error for {symbol}: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._get_empty_result(error=str(e))
        

    def _print_config_table(self, strategy_name, use_mag7):
        """Print configuration parameters in a formatted table"""
        all_params = self._get_all_config_params()
        
        print(f"\n[*] Active Strategy: {strategy_name}")
        print("=" * 60)
        print("| Parameter                      | Value               |")
        print("=" * 60)
        
        # Define the order of parameters to display
        if use_mag7:
            # Mag7 strategy parameters first
            priority_keys = ['use_mag7_confirmation', 'mag7_threshold', 'mag7_price_change_threshold', 
                            'mag7_min_aligned', 'mag7_stocks', 'tickers']
        else:
            # Sector strategy parameters first
            priority_keys = ['use_mag7_confirmation', 'sector_weight_threshold', 'sector_etfs', 'tickers']
        
        # Add common parameters
        common_keys = ['bb_width_threshold', 'donchian_contraction_threshold', 'volume_squeeze_threshold',
                    'ema_value', 'stochastic_k_period', 'stochastic_d_period', 'stochastic_smooth',
                    'stop_loss_method', 'contracts_per_trade', 'auto_trading_enabled']
        
        # Display priority parameters first
        for key in priority_keys:
            if key in self.trading_config and key in all_params:
                display_name, formatter = all_params[key]
                value = self.trading_config[key]
                try:
                    formatted_value = formatter(value)
                except:
                    formatted_value = str(value)
                print(f"| {display_name:<30} | {formatted_value:<19} |")
        
        # Display common parameters
        for key in common_keys:
            if key in self.trading_config and key in all_params:
                display_name, formatter = all_params[key]
                value = self.trading_config[key]
                try:
                    formatted_value = formatter(value)
                except:
                    formatted_value = str(value)
                print(f"| {display_name:<30} | {formatted_value:<19} |")
        
        # Display any remaining parameters not in priority or common lists
        displayed_keys = set(priority_keys + common_keys)
        for key, (display_name, formatter) in all_params.items():
            if key in self.trading_config and key not in displayed_keys:
                # Skip strategy-specific parameters that don't apply
                if not use_mag7 and key in ['mag7_threshold', 'mag7_price_change_threshold', 'mag7_min_aligned', 'mag7_stocks']:
                    continue
                if use_mag7 and key in ['sector_weight_threshold', 'sector_etfs']:
                    continue
                    
                value = self.trading_config[key]
                try:
                    formatted_value = formatter(value)
                except:
                    formatted_value = str(value)
                print(f"| {display_name:<30} | {formatted_value:<19} |")
        
        print("=" * 60)

    
    def _get_all_config_params(self):
        """Get all configuration parameters dynamically"""
        # Define all possible parameters with their display names and formatters
        all_params = {
            # Common parameters
            "bb_width_threshold": ("BB Width Threshold", lambda x: f"{x:.3f}"),
            "donchian_contraction_threshold": ("Donchian Contraction", lambda x: f"{x:.1f}"),
            "volume_squeeze_threshold": ("Volume Squeeze Threshold", lambda x: f"{x:.1f}"),
            "ema_value": ("EMA Period", lambda x: f"{x}"),
            "adx_filter": ("ADX Filter Enabled", lambda x: "Yes" if x else "No"),
            "adx_minimum": ("ADX Minimum", lambda x: f"{x}"),
            "stochastic_k_period": ("Stochastic K Period", lambda x: f"{x}"),
            "stochastic_d_period": ("Stochastic D Period", lambda x: f"{x}"),
            "stochastic_smooth": ("Stochastic Smooth", lambda x: f"{x}"),
            "stop_loss_method": ("Stop Loss Method", lambda x: x),
            "atr_multiple": ("ATR Multiple", lambda x: f"{x:.1f}"),
            "fixed_stop_percentage": ("Fixed Stop %", lambda x: f"{x:.1f}%"),
            "contracts_per_trade": ("Contracts per Trade", lambda x: f"{x}"),
            "auto_trading_enabled": ("Auto Trading", lambda x: "Yes" if x else "No"),
            "no_trade_window_minutes": ("No Trade Window", lambda x: f"{x} min"),
            "auto_close_minutes": ("Auto Close Time", lambda x: f"{x} min"),
            "cutoff_time": ("Cutoff Time", lambda x: x),
            "failsafe_minutes": ("Failsafe Minutes", lambda x: f"{x} min"),
            "news_filter": ("News Filter", lambda x: "Yes" if x else "No"),
            "volume_spike_threshold": ("Volume Spike", lambda x: f"{x:.1f}x"),
            "liquidity_min_volume": ("Min Volume", lambda x: f"{x:,}"),
            "liquidity_min_oi": ("Min Open Interest", lambda x: f"{x:,}"),
            "liquidity_max_spread": ("Max Spread", lambda x: f"${x:.2f}"),
            "trailing_stop_method": ("Trailing Stop", lambda x: x.split("(")[0].strip()),
            
            # Strategy-specific parameters
            "use_mag7_confirmation": ("Strategy Type", lambda x: "Mag7" if x else "Sector"),
            "sector_weight_threshold": ("Sector Weight Threshold", lambda x: f"{x}%"),
            "sector_etfs": ("Sector ETFs", lambda x: ", ".join(x) if isinstance(x, list) else x),
            "mag7_threshold": ("Mag7 Alignment Threshold", lambda x: f"{x}%"),
            "mag7_price_change_threshold": ("Price Change Threshold", lambda x: f"{x}%"),
            "mag7_min_aligned": ("Min Aligned Stocks", lambda x: f"{x}"),
            "mag7_stocks": ("Mag7 Stocks", lambda x: ", ".join(x) if isinstance(x, list) else x),
            "tickers": ("Trading Tickers", lambda x: ", ".join(x) if isinstance(x, list) else x),
        }
        
        return all_params


            
    def _save_analysis_to_csv(self, analysis_data, filename):
        """
        Save detailed analysis data to CSV with configuration parameters
        
        Args:
            analysis_data (list): List of analysis records
            filename (str): Output filename
        """
        print(f"[DEBUG] _save_analysis_to_csv called with filename: {filename}")
        print(f"[DEBUG] Analysis data length: {len(analysis_data) if analysis_data else 0}")
        
        if not analysis_data:
            print("[DEBUG] No analysis data to save!")
            return
        
        try:
            # Determine which strategy is being used
            use_mag7 = self.trading_config.get("use_mag7_confirmation", False)
            strategy_name = "Mag7" if use_mag7 else "Sector"
            print(f"[DEBUG] Strategy detected: {strategy_name}")
            
            # Get all configuration parameters
            config_params = {
                'strategy_type': strategy_name,
                'bb_width_threshold': self.trading_config.get("bb_width_threshold", 0.05),
                'donchian_threshold': self.trading_config.get("donchian_contraction_threshold", 0.6),
                'volume_squeeze_threshold': self.trading_config.get("volume_squeeze_threshold", 0.3),
                'stochastic_k_period': self.trading_config.get("stochastic_k_period", 5),
                'stochastic_d_period': self.trading_config.get("stochastic_d_period", 3),
                'stochastic_smooth': self.trading_config.get("stochastic_smooth", 2),
                'ema_value': self.trading_config.get("ema_value", 15),
                'adx_filter': self.trading_config.get("adx_filter", True),
                'adx_minimum': self.trading_config.get("adx_minimum", 20),
                'stop_loss_method': self.trading_config.get("stop_loss_method", "ATR Multiple"),
                'atr_multiple': self.trading_config.get("atr_multiple", 1.5),
                'fixed_stop_percentage': self.trading_config.get("fixed_stop_percentage", 1.0),
                'trailing_stop_method': self.trading_config.get("trailing_stop_method", "Heiken Ashi Candle Trail"),
                'contracts_per_trade': self.trading_config.get("contracts_per_trade", 1),
                'no_trade_window_minutes': self.trading_config.get("no_trade_window_minutes", 3),
                'auto_close_minutes': self.trading_config.get("auto_close_minutes", 15),
                'cutoff_time': self.trading_config.get("cutoff_time", "15:15"),
                'failsafe_minutes': self.trading_config.get("failsafe_minutes", 20),
                'volume_spike_threshold': self.trading_config.get("volume_spike_threshold", 1.5),
            }
            
            # Add strategy-specific parameters
            if use_mag7:
                config_params.update({
                    'mag7_threshold': self.trading_config.get("mag7_threshold", 60),
                    'mag7_price_change_threshold': self.trading_config.get("mag7_price_change_threshold", 0.2),
                    'mag7_min_aligned': self.trading_config.get("mag7_min_aligned", 4),
                    'mag7_stocks': ', '.join(self.trading_config.get("mag7_stocks", 
                        ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"]))
                })
            else:
                config_params.update({
                    'sector_weight_threshold': self.trading_config.get("sector_weight_threshold", 43),
                    'sector_etfs': ', '.join(self.trading_config.get("selected_sectors", 
                        self.trading_config.get("sector_etfs", ["XLK", "XLF", "XLV", "XLY"]))),
                    'xlk_weight': self.trading_config.get("sector_weights", {}).get("XLK", 32),
                    'xlf_weight': self.trading_config.get("sector_weights", {}).get("XLF", 14),
                    'xlv_weight': self.trading_config.get("sector_weights", {}).get("XLV", 11),
                    'xly_weight': self.trading_config.get("sector_weights", {}).get("XLY", 11),
                })
            
            # Create enhanced analysis records with config parameters
            enhanced_records = []
            for i, record in enumerate(analysis_data):
                if i == 0:  # Debug first record
                    print(f"[DEBUG] First record keys: {list(record.keys())[:10]}...")
                    
                enhanced_record = {}
                
                # First add all config parameters
                for key, value in config_params.items():
                    enhanced_record[f'config_{key}'] = value
                
                # Then add comparison values for key metrics
                if 'bb_width' in record and record['bb_width'] is not None:
                    enhanced_record['bb_width_vs_threshold'] = f"{record['bb_width']:.4f} vs {config_params['bb_width_threshold']:.4f}"
                    enhanced_record['bb_compression'] = 'YES' if record['bb_width'] < config_params['bb_width_threshold'] else 'NO'
                
                if 'stoch_k' in record and record['stoch_k'] is not None:
                    if record.get('sector_direction') == 'bullish' or record.get('compression_direction') == 'bullish':
                        enhanced_record['stoch_momentum_aligned'] = 'YES' if record['stoch_k'] > 20 else 'NO'
                        enhanced_record['stoch_k_vs_threshold'] = f"{record['stoch_k']:.1f} vs >20"
                    elif record.get('sector_direction') == 'bearish' or record.get('compression_direction') == 'bearish':
                        enhanced_record['stoch_momentum_aligned'] = 'YES' if record['stoch_k'] < 80 else 'NO'
                        enhanced_record['stoch_k_vs_threshold'] = f"{record['stoch_k']:.1f} vs <80"
                
                if 'atr' in record and record['atr'] is not None:
                    enhanced_record['stop_loss_distance'] = f"{record['atr'] * config_params['atr_multiple']:.2f}" if config_params['stop_loss_method'] == 'ATR Multiple' else f"{config_params['fixed_stop_percentage']}%"
                
                # Add alignment specific info
                if use_mag7:
                    if 'sector_weight' in record:  # Actually Mag7 percentage
                        enhanced_record['mag7_alignment'] = f"{record['sector_weight']:.1f}% vs {config_params['mag7_threshold']}%"
                        enhanced_record['mag7_aligned'] = 'YES' if record.get('sector_aligned', False) else 'NO'
                else:
                    if 'sector_weight' in record:
                        enhanced_record['sector_alignment'] = f"{record['sector_weight']}% vs {config_params['sector_weight_threshold']}%"
                        enhanced_record['sector_aligned'] = 'YES' if record.get('sector_aligned', False) else 'NO'
                
                # Add all original record fields
                for key, value in record.items():
                    # Rename sector fields for Mag7 strategy
                    if use_mag7 and key == 'sector_aligned':
                        enhanced_record['mag7_aligned'] = value
                    elif use_mag7 and key == 'sector_direction':
                        enhanced_record['mag7_direction'] = value
                    elif use_mag7 and key == 'sector_weight':
                        enhanced_record['mag7_percentage'] = value
                    else:
                        enhanced_record[key] = value
                
                enhanced_records.append(enhanced_record)
            
            print(f"[DEBUG] Created {len(enhanced_records)} enhanced records")
            
            # Identify all keys in enhanced records
            all_keys = set()
            for record in enhanced_records:
                all_keys.update(record.keys())
            
            print(f"[DEBUG] Total unique keys: {len(all_keys)}")
            
            # Define column order - config parameters first, then comparisons, then data
            config_keys = [k for k in sorted(all_keys) if k.startswith('config_')]
            comparison_keys = ['bb_width_vs_threshold', 'bb_compression', 'stoch_k_vs_threshold', 
                            'stoch_momentum_aligned', 'stop_loss_distance', 'mag7_alignment', 
                            'mag7_aligned', 'sector_alignment', 'sector_aligned']
            comparison_keys = [k for k in comparison_keys if k in all_keys]
            
            # Core data columns in logical order
            core_columns = ['candle_idx', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'ema9', 'ema15', 'vwap', 'bb_width', 'stoch_k', 'stoch_d', 'atr']
            
            # Strategy-specific columns
            if use_mag7:
                strategy_columns = ['mag7_aligned', 'mag7_direction', 'mag7_percentage']
            else:
                strategy_columns = ['sector_aligned', 'sector_direction', 'sector_weight']
            
            # Signal columns
            signal_columns = ['compression_detected', 'compression_direction', 'momentum_aligned',
                            'trend_aligned', 'entry_signal', 'trade_entered', 'trade_direction',
                            'skip_reason', 'equity']
            
            # Combine all columns in order
            ordered_keys = config_keys + comparison_keys
            for col_list in [core_columns, strategy_columns, signal_columns]:
                for col in col_list:
                    if col in all_keys and col not in ordered_keys:
                        ordered_keys.append(col)
            
            # Add any remaining keys
            for key in sorted(all_keys):
                if key not in ordered_keys:
                    ordered_keys.append(key)
            
            print(f"[DEBUG] Writing CSV with {len(ordered_keys)} columns")
            
            # Write to CSV
            import csv
            with open(filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_keys)
                writer.writeheader()
                writer.writerows(enhanced_records)
                
            print(f"[DEBUG] Successfully wrote CSV to: {filename}")
            
            # Verify file was created
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                print(f"[DEBUG] File created successfully, size: {file_size} bytes")
            else:
                print(f"[DEBUG] ERROR: File was not created!")
                
        except Exception as e:
            print(f"[DEBUG] ERROR in _save_analysis_to_csv: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

            
    def _save_trades_to_csv(self, trades, filename):
        """
        Save trade data to CSV
        
        Args:
            trades (list): List of trade records
            filename (str): Output filename
        """
        if not trades:
            return
            
        # Identify all keys in trades
        all_keys = set()
        for trade in trades:
            all_keys.update(trade.keys())
        
        # Write to CSV
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(list(all_keys)))
            writer.writeheader()
            writer.writerows(trades)
    
    def _get_empty_result(self, error=None):
        """Return empty result with optional error"""
        result = {
            "Win Rate": 0,
            "Profit Factor": 0,
            "Max Drawdown": 0,
            "Total Trades": 0,
            "Winning Trades": 0,
            "Losing Trades": 0,
            "Gross Profit": 0,
            "Gross Loss": 0,
            "Final Equity": 10000,
            "Optimal Trailing Method": "Unknown",
            "Trades": []
        }
        if error:
            result["Error"] = error
        return result
    
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
    


    def _check_sector_alignment(self, sector_data, idx, sector_weights):
        """
        Check for sector alignment OR Mag7 alignment based on configuration
        
        Args:
            sector_data (dict): Dictionary of sector DataFrames (or Mag7 DataFrames)
            idx (int): Current index
            sector_weights (dict): Sector weights dictionary
            
        Returns:
            tuple: (aligned, direction, combined_weight)
        """
        # Check if we should use Mag7 instead
        if self.trading_config.get("use_mag7_confirmation", False):
            # For Mag7, pass the Mag7 data directly
            return self._check_mag7_alignment(sector_data, idx)
        
        # Original sector alignment logic
        if not sector_data:
            self.logger.warning("No sector data available for alignment check")
            return False, "neutral", 0
        
        # Get threshold from config (user can set to 0% for testing)
        threshold = self.trading_config.get("sector_weight_threshold", 43)
        
        # Get current status for each sector
        sector_status = {}
        for sector, df in sector_data.items():
            if len(df) <= idx or idx < 5:
                continue
                
            # Get current and 5-period average
            current_price = df.iloc[idx]['close']
            avg_5 = df.iloc[idx-5:idx]['close'].mean()
            
            # Get price change threshold from config
            price_change_threshold = self.trading_config.get("sector_price_change_threshold", 0.2) / 100
            
            # Determine sector status based on current vs average
            if current_price > avg_5 * (1 + price_change_threshold):
                sector_status[sector] = "bullish"
            elif current_price < avg_5 * (1 - price_change_threshold):
                sector_status[sector] = "bearish"
        
        # Check XLK alignment with other sectors
        xlk_status = sector_status.get("XLK", "neutral")
        
        if xlk_status == "neutral":
            return False, "neutral", 0
        
        # Count aligned sectors
        aligned_weight = sector_weights.get("XLK", 32)  # XLK weight
        
        for sector, status in sector_status.items():
            if sector != "XLK" and status == xlk_status:
                aligned_weight += sector_weights.get(sector, 0)
        
        # Check if weight exceeds threshold
        if aligned_weight >= threshold:
            return True, xlk_status, aligned_weight
        
        return False, "neutral", aligned_weight



    def _check_mag7_alignment(self, market_data, idx):
        """
        Check for Magnificent 7 alignment using historical data
        
        Args:
            market_data (dict): Dictionary of DataFrames (includes Mag7 stocks)
            idx (int): Current index
            
        Returns:
            tuple: (aligned, direction, percentage)
        """
        # Get Mag7 stocks from config
        mag7_stocks = self.trading_config.get("mag7_stocks", 
            ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"])
        
        # Get threshold from config
        threshold = float(self.trading_config.get("mag7_threshold", 60))
        
        # Extract Mag7 data from market_data
        mag7_data = {}
        for symbol in mag7_stocks:
            if symbol in market_data:
                mag7_data[symbol] = market_data[symbol]
        
        if not mag7_data or idx < 5:
            return False, "neutral", 0
        
        # Analyze each stock
        stock_statuses = {}
        
        for symbol, df in mag7_data.items():
            if len(df) <= idx:
                continue
            
            # Get current and average price
            current_price = df.iloc[idx]['close']
            avg_5 = df.iloc[idx-5:idx]['close'].mean()
            
            # Determine status with price change threshold
            price_change_threshold = self.trading_config.get("mag7_price_change_threshold", 0.2) / 100  # Convert percentage to decimal
            
            if current_price > avg_5 * (1 + price_change_threshold):  # Above average by threshold
                stock_statuses[symbol] = "bullish"
            elif current_price < avg_5 * (1 - price_change_threshold):  # Below average by threshold
                stock_statuses[symbol] = "bearish"
            else:
                stock_statuses[symbol] = "neutral"
        
        # Count statuses
        bullish_count = sum(1 for status in stock_statuses.values() if status == "bullish")
        bearish_count = sum(1 for status in stock_statuses.values() if status == "bearish")
        
        # Calculate percentages
        total_stocks = len(mag7_stocks)
        bullish_pct = (bullish_count / total_stocks) * 100
        bearish_pct = (bearish_count / total_stocks) * 100
        
        # Only log every 100th candle to reduce spam
        if idx % 100 == 0:
            self.logger.info(f"Mag7 alignment at candle {idx}: {bullish_count} bullish ({bullish_pct:.1f}%), "
                            f"{bearish_count} bearish ({bearish_pct:.1f}%)")
        
        # Check alignment
        if bullish_pct >= threshold:
            return True, "bullish", bullish_pct
        elif bearish_pct >= threshold:
            return True, "bearish", bearish_pct
        else:
            return False, "neutral", max(bullish_pct, bearish_pct)


    def _detect_compression(self, df, idx):
        """
        Detect price compression
        
        Args:
            df (DataFrame): Price data with technical indicators
            idx (int): Current index
            
        Returns:
            tuple: (compression_detected, direction)
        """
        # Need at least 20 candles of data
        if idx < 20 or idx >= len(df):
            return False, "neutral"
            
        # 1. Bollinger Band Width check
        if 'bb_width' in df.columns:
            bb_width = df.iloc[idx]['bb_width']
        else:
            bb_width = self._calculate_bollinger_band_width(df.iloc[idx-20:idx+1])
        
        # Get threshold from config
        bb_width_threshold = float(self.trading_config.get("bb_width_threshold", 0.05))
        bb_compression = pd.notna(bb_width) and bb_width < bb_width_threshold and bb_width > 0
        
        # 2. Calculate Donchian Channel contraction
        high_max = df['high'].iloc[idx-20:idx+1].max()
        low_min = df['low'].iloc[idx-20:idx+1].min()
        dc_range = high_max - low_min
        
        # Calculate average true range for comparison
        if idx >= 40:
            past_high_max = df['high'].iloc[idx-40:idx-20].max()
            past_low_min = df['low'].iloc[idx-40:idx-20].min()
            past_range = past_high_max - past_low_min
            # Get threshold from config
            dc_threshold = float(self.trading_config.get("donchian_contraction_threshold", 0.6))
            dc_compression = dc_range < (past_range * dc_threshold)
        else:
            avg_range = (df['high'].iloc[:idx+1].mean() - df['low'].iloc[:idx+1].mean())
            dc_threshold = float(self.trading_config.get("donchian_contraction_threshold", 0.6))
            dc_compression = dc_range < (avg_range * dc_threshold)
        
        # Calculate average true range for comparison
        if idx >= 40:
            past_high_max = df['high'].iloc[idx-40:idx-20].max()
            past_low_min = df['low'].iloc[idx-40:idx-20].min()
            past_range = past_high_max - past_low_min
            # Get threshold from config
            dc_threshold = float(self.trading_config.get("donchian_contraction_threshold", 0.6))
            dc_compression = dc_range < (past_range * dc_threshold)
        else:
            avg_range = (df['high'].iloc[:idx+1].mean() - df['low'].iloc[:idx+1].mean())
            dc_threshold = float(self.trading_config.get("donchian_contraction_threshold", 0.6))
            dc_compression = dc_range < (avg_range * dc_threshold)
        
        # 3. Calculate Volume Squeeze
        if 'volume' in df.columns and df['volume'].iloc[idx-20:idx+1].sum() > 0:
            recent_vol = df['volume'].iloc[idx-5:idx+1].mean()
            past_vol = df['volume'].iloc[idx-20:idx-5].mean() if idx >= 25 else df['volume'].iloc[:idx].mean()
            # Get threshold from config
            vol_threshold = float(self.trading_config.get("volume_squeeze_threshold", 0.3))
            volume_squeeze = recent_vol < (past_vol * vol_threshold) if past_vol > 0 else False
        else:
            volume_squeeze = False
        
        # Get compression threshold count from config
        required_compression_count = self.trading_config.get("compression_threshold_count", 2)
        compression_count = sum([bb_compression, dc_compression, volume_squeeze])
        compression_detected = compression_count >= required_compression_count
        
        if not compression_detected:
            return False, "neutral"
            
        # Determine direction based on VWAP
        if 'vwap' in df.columns and pd.notna(df.iloc[idx]['vwap']):
            vwap = df.iloc[idx]['vwap']
            if df.iloc[idx]['close'] > vwap:
                return True, "bullish"
            else:
                return True, "bearish"
        else:
            # No VWAP, determine direction based on EMA
            if 'ema15' in df.columns and pd.notna(df.iloc[idx]['ema15']):
                ema = df.iloc[idx]['ema15']
                if df.iloc[idx]['close'] > ema:
                    return True, "bullish"
                else:
                    return True, "bearish"
            else:
                # Use price action to determine direction
                if df.iloc[idx]['close'] > df.iloc[idx]['open']:
                    return True, "bullish"
                else:
                    return True, "bearish"
                

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
        try:
            # Calculate middle band (simple moving average)
            if len(data) < window:
                return 0.05  # Default value if not enough data
                
            # Make sure we're working with float values
            if 'close' in data.columns:
                close_prices = data['close'].astype(float)
            elif 'Close' in data.columns:
                close_prices = data['Close'].astype(float)
            else:
                # No close prices available
                return 0.05
            
            # Calculate middle band (SMA)
            middle_band = close_prices.rolling(window=window).mean().iloc[-1]
            
            # Calculate standard deviation
            std = close_prices.rolling(window=window).std().iloc[-1]
            
            # Calculate upper and lower bands
            upper_band = middle_band + (std * num_std)
            lower_band = middle_band - (std * num_std)
            
            # Calculate width as percentage of middle band
            if middle_band == 0 or pd.isna(middle_band):
                return 0.05  # Prevent division by zero
                
            width = (upper_band - lower_band) / middle_band
            
            return width if pd.notna(width) else 0.05
        except Exception as e:
            self.logger.error(f"Error calculating Bollinger Band width: {e}")
            return 0.05  # Default value on error
    
    def _calculate_stochastic_full(self, data, k_period=5, d_period=3, smooth=2):
        """
        Calculate Stochastic Oscillator (Barry Burns' method) for full DataFrame
        
        Args:
            data (DataFrame): Price data
            k_period (int): K period
            d_period (int): D period
            smooth (int): Smoothing factor
            
        Returns:
            tuple: (Series K, Series D)
        """
        # Calculate highest high and lowest low over k_period
        high_high = data['high'].rolling(window=k_period).max()
        low_low = data['low'].rolling(window=k_period).min()
        
        # Calculate raw K
        raw_k = 100 * (data['close'] - low_low) / (high_high - low_low)
        raw_k = raw_k.replace([np.inf, -np.inf], np.nan).fillna(50)
        
        # Apply smoothing to K
        k = raw_k.rolling(window=smooth).mean()
        
        # Calculate D
        d = k.rolling(window=d_period).mean()
        
        return k, d
    
    def _calculate_stochastic(self, data, k_period=5, d_period=3, smooth=2):
        """
        Calculate Stochastic Oscillator for a single position
        
        Args:
            data (DataFrame): Price data
            k_period (int): K period
            d_period (int): D period
            smooth (int): Smoothing factor
            
        Returns:
            tuple: (K value, D value)
        """
        # Need enough data for calculation
        if len(data) < k_period + d_period:
            return 50, 50
            
        # Calculate highest high and lowest low over look back period
        high_high = data['high'].rolling(window=k_period).max().iloc[-1]
        low_low = data['low'].rolling(window=k_period).min().iloc[-1]
        
        # Calculate raw K
        close = data['close'].iloc[-1]
        if high_high == low_low:
            raw_k = 50
        else:
            raw_k = 100 * (close - low_low) / (high_high - low_low)
            
        # Simple smoothing for K
        k_values = []
        for i in range(min(smooth, len(data))):
            if i == 0:
                k_values.append(raw_k)
            else:
                prev_close = data['close'].iloc[-(i+1)]
                prev_high = data['high'].rolling(window=k_period).max().iloc[-(i+1)]
                prev_low = data['low'].rolling(window=k_period).min().iloc[-(i+1)]
                
                if prev_high == prev_low:
                    k_values.append(50)
                else:
                    k_values.append(100 * (prev_close - prev_low) / (prev_high - prev_low))
        
        k = sum(k_values) / len(k_values)
        
        # For D, we'd need to calculate K for past d_period points
        # For simplicity, just return k as both values
        return k, k
    
    def _calculate_atr_series(self, data, period=14):
        """
        Calculate ATR for entire DataFrame
        
        Args:
            data (DataFrame): Price data
            period (int): ATR period
            
        Returns:
            Series: ATR values
        """
        # Calculate True Range components
        tr1 = data['high'] - data['low']
        tr2 = abs(data['high'] - data['close'].shift(1))
        tr3 = abs(data['low'] - data['close'].shift(1))
        
        # True Range is the maximum of the three
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Calculate ATR as average of True Range
        atr = tr.rolling(window=period).mean()
        
        return atr
    
    def _calculate_atr(self, data, idx=None, period=14):
        """
        Calculate ATR for a specific index
        
        Args:
            data (DataFrame): Price data
            idx (int, optional): Index to calculate for
            period (int): ATR period
            
        Returns:
            float: ATR value
        """
        try:
            # Calculate full ATR series
            atr_series = self._calculate_atr_series(data, period)
            
            # Return ATR for specific index or last value
            if idx is not None and idx < len(atr_series):
                return atr_series.iloc[idx] if not pd.isna(atr_series.iloc[idx]) else 1.0
            else:
                return atr_series.iloc[-1] if not pd.isna(atr_series.iloc[-1]) else 1.0
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {e}")
            return 1.0  # Default value
    
    def _calculate_vwap(self, data):
        """
        Calculate Volume Weighted Average Price
        
        Args:
            data (DataFrame): Price data with OHLCV columns
            
        Returns:
            Series: VWAP values
        """
        # Calculate typical price
        typical_price = (data['high'] + data['low'] + data['close']) / 3
        
        # If volume column exists
        if 'volume' in data.columns and data['volume'].sum() > 0:
            # Calculate VWAP
            vwap = (typical_price * data['volume']).cumsum() / data['volume'].cumsum()
        else:
            # No volume data, use simple moving average instead
            vwap = typical_price.rolling(window=20).mean()
        
        return vwap
    
    def _check_entry_signal(self, prev_candle, current_candle, ha_candle):
        """
        Check for entry signal based on candle data and Heiken Ashi
        
        Args:
            prev_candle (Series): Previous candle data
            current_candle (Series): Current candle data
            ha_candle (Series): Current Heiken Ashi candle
            
        Returns:
            str: "bullish", "bearish", or None for no signal
        """
        try:
            # Check for Heiken Ashi pattern
            ha_open = float(ha_candle["open"])
            ha_close = float(ha_candle["close"])
            ha_high = float(ha_candle["high"])
            ha_low = float(ha_candle["low"])
            
            # Get wick tolerance from config
            wick_tolerance_pct = self.trading_config.get("ha_wick_tolerance", 0.1)
            wick_tolerance = (ha_high - ha_low) * wick_tolerance_pct
            
            # Bullish signal - Small or no lower wick = strong bullish candle
            if abs(ha_open - ha_low) < wick_tolerance and ha_close > ha_open:
                return "bullish"
                
            # Bearish signal - Small or no upper wick = strong bearish candle  
            if abs(ha_open - ha_high) < wick_tolerance and ha_close < ha_open:
                return "bearish"
        except Exception as e:
            self.logger.error(f"Error in entry signal check: {e}")
        
        return None
    
    def _check_volume_spike(self, data, idx, lookback=5, threshold=1.5):
        """
        Check for volume spike
        
        Args:
            data (DataFrame): Price data
            idx (int): Current index
            lookback (int): Number of periods to look back
            threshold (float): Volume spike threshold
            
        Returns:
            bool: True if volume spike detected
        """
        if 'volume' not in data.columns or idx < lookback:
            return False
            
        current_volume = data['volume'].iloc[idx]
        avg_volume = data['volume'].iloc[idx-lookback:idx].mean()
        
        return current_volume > (avg_volume * threshold)
    
    def _simulate_trade_with_method(self, data, ha_data, start_idx, direction, method):
        """
        Simulate a trade with a given trailing stop method
        
        Args:
            data (DataFrame): Price data
            ha_data (DataFrame): Heiken Ashi data
            start_idx (int): Starting index for the trade
            direction (str): "bullish" or "bearish"
            method (str): Trailing stop method
            
        Returns:
            tuple: (exit_index, exit_price, reason)
        """
        try:
            max_bars = min(30, len(data) - start_idx)  # Maximum 30 bars or until end of data
            stop_price = None
            reason = "Max bars reached"
            
            # Set initial stop price based on method
            if "Heiken Ashi" in method:
                # Use prior candle low/high as initial stop
                if direction == "bullish":
                    stop_price = float(data.iloc[start_idx-1]['low'])
                else:
                    stop_price = float(data.iloc[start_idx-1]['high'])
            
            elif "EMA" in method:
                # Use EMA as stop
                if 'ema9' in data.columns:
                    if direction == "bullish":
                        stop_price = float(data.iloc[start_idx]['ema9'])
                    else:
                        stop_price = float(data.iloc[start_idx]['ema9'])
                else:
                    # Calculate EMA9 on the fly
                    ema = data['close'].iloc[:start_idx+1].ewm(span=9, adjust=False).mean()
                    stop_price = float(ema.iloc[-1])
                    
            elif "ATR" in method:
                # Use ATR-based stop
                if 'atr' in data.columns:
                    atr = float(data.iloc[start_idx]['atr'])
                else:
                    atr = self._calculate_atr(data, start_idx)
                    
                if direction == "bullish":
                    stop_price = float(data.iloc[start_idx]['close']) - (atr * 1.5)
                else:
                    stop_price = float(data.iloc[start_idx]['close']) + (atr * 1.5)
                    
            elif "% Price" in method:
                # Use percentage of price
                if direction == "bullish":
                    stop_price = float(data.iloc[start_idx]['close']) * 0.985  # 1.5% below
                else:
                    stop_price = float(data.iloc[start_idx]['close']) * 1.015  # 1.5% above
                    
            else:  # Fixed Tick
                # Use fixed amount
                if direction == "bullish":
                    stop_price = float(data.iloc[start_idx]['close']) - 1.0  # $1 below
                else:
                    stop_price = float(data.iloc[start_idx]['close']) + 1.0  # $1 above
                    
            # Simulate the trade
            entry_price = float(data.iloc[start_idx]['close'])
            max_profit = 0
            min_holding_bars = 3  # Minimum 3 bars (15 minutes for 5m candles)
            
            for i in range(start_idx + 1, min(start_idx + max_bars, len(data))):
                # Calculate bars held
                bars_held = i - start_idx
                # Skip exit checks if minimum holding period not met
                if bars_held < min_holding_bars:
                    continue
                if i >= len(data):
                    break
                    
                # Current price
                current_price = float(data.iloc[i]['close'])
                current_low = float(data.iloc[i]['low'])
                current_high = float(data.iloc[i]['high'])
                
                # Check for stop hit
                if direction == "bullish" and current_low <= stop_price:
                    return i, stop_price, "Stop loss hit"
                elif direction == "bearish" and current_high >= stop_price:
                    return i, stop_price, "Stop loss hit"
                    
                # Calculate current profit
                if direction == "bullish":
                    current_profit = current_price - entry_price
                else:
                    current_profit = entry_price - current_price
                    
                # Update max profit
                if current_profit > max_profit:
                    max_profit = current_profit
                    
                    # Trail the stop if using trailing stop
                    if "Heiken Ashi" in method:
                        # Update stop to prior candle low/high
                        if direction == "bullish":
                            new_stop = float(data.iloc[i-1]['low'])
                            if new_stop > stop_price:
                                stop_price = new_stop
                        else:
                            new_stop = float(data.iloc[i-1]['high'])
                            if new_stop < stop_price:
                                stop_price = new_stop
                                
                    elif "EMA" in method:
                        # Update stop to current EMA
                        if 'ema9' in data.columns:
                            ema_value = float(data.iloc[i]['ema9'])
                        else:
                            ema = data['close'].iloc[:i+1].ewm(span=9, adjust=False).mean()
                            ema_value = float(ema.iloc[-1])
                            
                        if direction == "bullish":
                            if ema_value > stop_price:
                                stop_price = ema_value
                        else:
                            if ema_value < stop_price:
                                stop_price = ema_value
                                
                    elif "ATR" in method:
                        # Update stop based on ATR
                        if 'atr' in data.columns:
                            atr = float(data.iloc[i]['atr'])
                        else:
                            atr = self._calculate_atr(data, i)
                            
                        if direction == "bullish":
                            new_stop = current_price - (atr * 1.5)
                            if new_stop > stop_price:
                                stop_price = new_stop
                        else:
                            new_stop = current_price + (atr * 1.5)
                            if new_stop < stop_price:
                                stop_price = new_stop
                                
                    elif "% Price" in method:
                        # Update stop based on percentage
                        if direction == "bullish":
                            new_stop = current_price * 0.985  # 1.5% below
                            if new_stop > stop_price:
                                stop_price = new_stop
                        else:
                            new_stop = current_price * 1.015  # 1.5% above
                            if new_stop < stop_price:
                                stop_price = new_stop
                                
                    else:  # Fixed Tick
                        # Update stop based on fixed amount
                        if direction == "bullish":
                            new_stop = current_price - 1.0
                            if new_stop > stop_price:
                                stop_price = new_stop
                        else:
                            new_stop = current_price + 1.0
                            if new_stop < stop_price:
                                stop_price = new_stop
                
                # Check for Heiken Ashi reversal signal
                if i < len(ha_data):
                    ha_open = float(ha_data.iloc[i]["open"])
                    ha_close = float(ha_data.iloc[i]["close"])
                    
                    # Calculate current profit percentage
                    if direction == "bullish":
                        current_profit_pct = ((current_price - entry_price) / entry_price) * 100
                    else:
                        current_profit_pct = ((entry_price - current_price) / entry_price) * 100
                    
                    # Get minimum profit threshold from config
                    min_profit_before_exit = self.trading_config.get("ha_exit_min_profit", 0.5)
                    
                    if (direction == "bullish" and ha_open > ha_close) or (direction == "bearish" and ha_open < ha_close):
                        # Only exit if we have decent profit or are losing money
                        if current_profit_pct >= min_profit_before_exit or current_profit_pct < -0.1:
                            return i, current_price, "Heiken Ashi reversal"
                        
                # Check for opposing Stochastic crossover
                if 'stoch_k' in data.columns and 'stoch_d' in data.columns:
                    k = data.iloc[i]['stoch_k']
                    d = data.iloc[i]['stoch_d']
                    prev_k = data.iloc[i-1]['stoch_k'] if i > start_idx + 1 else k
                    
                    # Only exit if stochastic is strongly overbought/oversold and crossing
                    if direction == "bullish" and k > 85 and prev_k > d and k < d:
                        return i, current_price, "Stochastic overbought and crossing down"
                    elif direction == "bearish" and k < 15 and prev_k < d and k > d:
                        return i, current_price, "Stochastic oversold and crossing up"
                        
                # Check for VWAP or EMA crossover against trade
                if 'vwap' in data.columns and 'ema15' in data.columns:
                    vwap = data.iloc[i]['vwap']
                    ema = data.iloc[i]['ema15']
                    
                    if pd.notna(vwap) and pd.notna(ema):
                        if direction == "bullish" and current_price < min(vwap, ema):
                            return i, current_price, "Price crossed below VWAP and EMA"
                        elif direction == "bearish" and current_price > max(vwap, ema):
                            return i, current_price, "Price crossed above VWAP and EMA"
            
            # If we reach this point, we've hit max bars
            if start_idx + max_bars < len(data):
                return start_idx + max_bars - 1, float(data.iloc[start_idx + max_bars - 1]['close']), reason
            else:
                return len(data) - 1, float(data.iloc[-1]['close']), reason
                
        except Exception as e:
            self.logger.error(f"Error simulating trade: {e}")
            return start_idx + 1, float(data.iloc[start_idx]['close']), f"Error: {str(e)}"
            
    def _calculate_max_drawdown(self, equity_curve):
        """
        Calculate maximum drawdown from equity curve
        
        Args:
            equity_curve (list): Equity curve values
            
        Returns:
            float: Maximum drawdown as percentage
        """
        max_dd = 0
        peak = equity_curve[0]
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            else:
                dd = (peak - equity) / peak * 100
                max_dd = max(max_dd, dd)
                
        return max_dd

    def generate_summary_output(self, results, output_file):
        """
        Generate summary output file with backtest results
        
        Args:
            results (dict): Backtest results by ticker
            output_file (str): Output file path
        """
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Header row
            writer.writerow([
                'Symbol_Period', 
                'Win Rate', 
                'Profit Factor', 
                'Max Drawdown', 
                'Total Trades',
                'Winning Trades',
                'Losing Trades',
                'Gross Profit',
                'Gross Loss',
                'Final Equity',
                'Optimal Trailing Method'
            ])
            
            # Write results for each ticker/period
            for symbol_period, result in results.items():
                writer.writerow([
                    symbol_period,
                    result.get('Win Rate', 0),
                    result.get('Profit Factor', 0),
                    result.get('Max Drawdown', 0),
                    result.get('Total Trades', 0),
                    result.get('Winning Trades', 0),
                    result.get('Losing Trades', 0),
                    result.get('Gross Profit', 0),
                    result.get('Gross Loss', 0),
                    result.get('Final Equity', 10000),
                    result.get('Optimal Trailing Method', 'Unknown')
                ])
                
        print(f"[✓] Summary results saved to {output_file}")
        return output_file