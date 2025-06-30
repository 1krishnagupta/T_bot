# File: Code/bot_core/backtest_engine.py

import os
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import logging
import csv
from pathlib import Path

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
                print(f"[*] Updated trading config from UI:")
                print(f"    - Sector threshold: {self.trading_config.get('sector_weight_threshold', 43)}%")
                print(f"    - BB width threshold: {self.trading_config.get('bb_width_threshold', 0.05)}")
                print(f"    - Compression threshold: {self.trading_config.get('donchian_contraction_threshold', 0.6)}")
            
            # Get historical candle data with better error handling
            if not self.candle_data_client:
                self.logger.error("No candle data client provided")
                return self._get_empty_result()
                
            # Fetch data from the selected source
            candles = self.candle_data_client.get_candles_for_backtesting(
                [symbol], 
                period,
                start_date,
                end_date,
                data_source=data_source  # ADD THIS PARAMETER
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
            
            # Fetch sector ETF data - IMPORTANT: Use the same data source that was selected
            print(f"[*] Fetching sector ETF data using {data_source}...")
            sectors = ["XLK", "XLF", "XLV", "XLY"]
            sector_data = {}
            
            # Get sector weights from config or use defaults
            sector_weights = self.trading_config.get("sector_weights", {
                "XLK": 32,
                "XLF": 14,
                "XLV": 11,
                "XLY": 11
            })
            
            # Ensure candle_data_client fetches sector data
            print(f"[*] Fetching sector data separately...")
            sector_result = self.candle_data_client.fetch_historical_data_for_backtesting(
                sectors, period, start_date, end_date,
                data_source=data_source
            )
            
            for sector in sectors:
                print(f"[*] Processing data for sector {sector}...")
                sector_candles = sector_result.get(sector, [])
                
                if sector_candles:
                    sector_df = pd.DataFrame(sector_candles)
                    
                    # Standardize column names
                    sector_df = sector_df.rename(columns={k: v for k, v in col_map.items() if k in sector_df.columns})
                    
                    # Convert to numeric
                    for col in ['open', 'high', 'low', 'close']:
                        if col in sector_df.columns:
                            sector_df[col] = pd.to_numeric(sector_df[col], errors='coerce')
                    
                    # Add timestamp if needed
                    if 'timestamp' not in sector_df.columns:
                        if 'start_time' in sector_df.columns:
                            sector_df['timestamp'] = sector_df['start_time']
                    
                    # Ensure timestamp is string format for consistency
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
            sector_aligned_count = 0
            compression_count = 0
            momentum_aligned_count = 0
            trend_aligned_count = 0
            entry_signal_count = 0
            trade_count = 0
            
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
                    # 1. Check Sector Alignment
                    sector_aligned, direction, combined_weight = self._check_sector_alignment(sector_data, i, sector_weights)
                    analysis_record['sector_aligned'] = sector_aligned
                    analysis_record['sector_direction'] = direction
                    analysis_record['sector_weight'] = combined_weight if sector_aligned else 0
                    
                    if sector_aligned:
                        sector_aligned_count += 1
                    
                    if not sector_aligned:
                        analysis_record['skip_reason'] = f'No sector alignment (weight={combined_weight}%, threshold={self.trading_config.get("sector_weight_threshold", 43)}%)'
                    else:
                        # 2. Check for Compression
                        compression_detected, comp_direction = self._detect_compression(df, i)
                        analysis_record['compression_detected'] = compression_detected
                        analysis_record['compression_direction'] = comp_direction if compression_detected else 'neutral'
                        
                        if compression_detected:
                            compression_count += 1
                        
                        if not compression_detected or comp_direction != direction:
                            analysis_record['skip_reason'] = 'No compression or direction mismatch'
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
                                analysis_record['skip_reason'] = 'Momentum not aligned'
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
                                    analysis_record['skip_reason'] = 'Trend not aligned'
                                else:
                                    # 4. Check Entry Trigger
                                    if i < len(ha_df):
                                        entry_signal = self._check_entry_signal(df.iloc[i-1], df.iloc[i], ha_df.iloc[i])
                                        analysis_record['entry_signal'] = entry_signal
                                        
                                        if entry_signal:
                                            entry_signal_count += 1
                                        
                                        if not entry_signal or entry_signal != direction:
                                            analysis_record['skip_reason'] = 'No entry signal'
                                        else:
                                            # Valid trade signal found!
                                            analysis_record['trade_entered'] = True
                                            analysis_record['trade_direction'] = entry_signal
                                            trade_count += 1
                                            
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
            print(f"\n[*] Signal Statistics:")
            print(f"  - Sector Aligned: {sector_aligned_count} times")
            print(f"  - Compression Detected: {compression_count} times")
            print(f"  - Momentum Aligned: {momentum_aligned_count} times")
            print(f"  - Trend Aligned: {trend_aligned_count} times")
            print(f"  - Entry Signals: {entry_signal_count} times")
            print(f"  - Trades Entered: {trade_count} times")
            
            # Add debug info about why trades weren't entered
            if trade_count == 0:
                print(f"\n[!] No trades found. Debugging info:")
                print(f"  - Sector data available: {len(sector_data)} sectors")
                print(f"  - Sector weight threshold: {self.trading_config.get('sector_weight_threshold', 43)}%")
                print(f"  - Compression threshold: {self.trading_config.get('bb_width_threshold', 0.05)}")
                print(f"  - Stochastic settings: K={self.trading_config.get('stochastic_k_period', 5)}, D={self.trading_config.get('stochastic_d_period', 3)}")
                
                # Check if sector data is valid
                for sector, df in sector_data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        print(f"  - {sector}: {len(df)} candles, date range: {df.iloc[0]['timestamp']} to {df.iloc[-1]['timestamp']}")
                    else:
                        print(f"  - {sector}: No data!")
            
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

            # Save complete analysis
            if not hasattr(self, 'dir_manager') or not self.dir_manager:
                from Code.bot_core.backtest_directory_manager import BacktestDirectoryManager
                self.dir_manager = BacktestDirectoryManager()
                
            if not hasattr(self, 'run_id'):
                self.run_id = self.dir_manager.generate_run_id()
            
            # Create run-specific logger if not exists
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
            self.run_logger.info(f"  Sector Aligned Count: {sector_aligned_count}")
            self.run_logger.info(f"  Compression Count: {compression_count}")
            self.run_logger.info(f"  Entry Signals: {entry_signal_count}")
            self.run_logger.info(f"  Trades Entered: {trade_count}")
            
            # Save analysis data
            analysis_path = self.dir_manager.get_analysis_path(self.run_id, f"{symbol}_{period}min_analysis")
            self._save_analysis_to_csv(analysis_data, analysis_path)
            print(f"[✓] Saved complete analysis ({len(analysis_data)} records) to {analysis_path}")
            self.logger.info(f"Saved analysis to {analysis_path}")
            self.run_logger.info(f"Analysis saved: {analysis_path} ({len(analysis_data)} records)")
            
            # Save trades data
            trades_path = self.dir_manager.get_results_path(self.run_id, 'trades')
            self._save_trades_to_csv(trades, trades_path)
            print(f"[✓] Saved trades to {trades_path}")
            self.logger.info(f"Saved trades to {trades_path}")
            self.run_logger.info(f"Trades saved: {trades_path} ({len(trades)} trades)")

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
                    "Trades": trades
                }
                
                return result
            else:
                # No trades found - return empty result with debugging info
                empty_result = self._get_empty_result()
                empty_result["Debug Info"] = {
                    "Total Candles": len(df),
                    "Sector Aligned": sector_aligned_count,
                    "Compression Detected": compression_count,
                    "Momentum Aligned": momentum_aligned_count,
                    "Trend Aligned": trend_aligned_count,
                    "Entry Signals": entry_signal_count,
                    "Trades": trade_count
                }
                print("\n[!] No trades found. Check the debug info above.")
                return empty_result
                
        except Exception as e:
            self.logger.error(f"Error in backtest for {symbol}: {str(e)}")
            print(f"[✗] Backtest error for {symbol}: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._get_empty_result(error=str(e))
            
    def _save_analysis_to_csv(self, analysis_data, filename):
        """
        Save detailed analysis data to CSV
        
        Args:
            analysis_data (list): List of analysis records
            filename (str): Output filename
        """
        if not analysis_data:
            return
            
        # Identify all keys in analysis data
        all_keys = set()
        for record in analysis_data:
            all_keys.update(record.keys())
        
        # Write to CSV
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(list(all_keys)))
            writer.writeheader()
            writer.writerows(analysis_data)
            
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
        Check for sector alignment using historical sector data
        
        Args:
            sector_data (dict): Dictionary of sector DataFrames
            idx (int): Current index
            sector_weights (dict): Sector weights dictionary
            
        Returns:
            tuple: (aligned, direction, combined_weight)
        """
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
            
            # Determine sector status based on current vs average
            if current_price > avg_5 * 1.002:  # 0.2% above average
                sector_status[sector] = "bullish"
            elif current_price < avg_5 * 0.998:  # 0.2% below average
                sector_status[sector] = "bearish"
            else:
                sector_status[sector] = "neutral"
        
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
        
        # Need 2 out of 3 for compression
        compression_count = sum([bb_compression, dc_compression, volume_squeeze])
        # BUT if threshold is very low (like 0%), be more lenient
        threshold = self.trading_config.get("sector_weight_threshold", 43)
        if threshold < 10:  # Very low threshold, be more lenient
            compression_detected = compression_count >= 1
        else:
            compression_detected = compression_count >= 2
        
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
            # Check for Heiken Ashi pattern - HA bottom with no lower wick
            ha_open = float(ha_candle["open"])
            ha_close = float(ha_candle["close"])
            ha_high = float(ha_candle["high"])
            ha_low = float(ha_candle["low"])
            
            # Bullish signal - Flat-bottom, no lower wick = strong bullish candle
            if abs(ha_open - ha_low) < 0.0001 and ha_close > ha_open:
                return "bullish"
                
            # Bearish signal - Flat-top, no upper wick = strong bearish candle  
            if abs(ha_open - ha_high) < 0.0001 and ha_close < ha_open:
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
            
            for i in range(start_idx + 1, min(start_idx + max_bars, len(data))):
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
                    
                    if (direction == "bullish" and ha_open > ha_close) or (direction == "bearish" and ha_open < ha_close):
                        return i, current_price, "Heiken Ashi reversal"
                        
                # Check for opposing Stochastic crossover
                if 'stoch_k' in data.columns and 'stoch_d' in data.columns:
                    k = data.iloc[i]['stoch_k']
                    d = data.iloc[i]['stoch_d']
                    
                    if direction == "bullish" and k > 80 and k < d:
                        return i, current_price, "Stochastic overbought and crossing down"
                    elif direction == "bearish" and k < 20 and k > d:
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