import yaml
import os
import json
import logging
from datetime import datetime
from pathlib import Path

class ConfigLoader:
    """
    Configuration loader for the trading bot.
    Handles loading configuration from YAML or JSON files and provides
    fallback default values for missing settings.
    """
    
    def __init__(self, config_path=None):
        """
        Initialize the configuration loader
        
        Args:
            config_path (str, optional): Path to the configuration file
        """
        self.config_path = config_path
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"config_loader_{today}.log")
        
        self.logger = logging.getLogger("ConfigLoader")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
    def load_config(self, path=None):
        """
        Load configuration from a file
        
        Args:
            path (str, optional): Path to the configuration file, overrides the path
                provided in the constructor
                
        Returns:
            dict: Configuration dictionary
        """
        # Use path parameter if provided, otherwise use the one from constructor
        config_path = path if path else self.config_path
        
        # If still no path, use default config file
        if config_path is None:
            config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'settings.yaml'))
        
        # Check if file exists
        if not os.path.exists(config_path):
            self.logger.warning(f"Configuration file not found: {config_path}")
            print(f"[!] Configuration file not found: {config_path}")
            print("[!] Using default configuration")
            return self.get_default_config()
        
        try:
            # Load configuration based on file extension
            file_extension = os.path.splitext(config_path)[1].lower()
            
            if file_extension in ['.yaml', '.yml']:
                with open(config_path, 'r') as file:
                    config = yaml.safe_load(file)
            elif file_extension == '.json':
                with open(config_path, 'r') as file:
                    config = json.load(file)
            elif file_extension == '.txt':
                # Handle TXT files as YAML
                with open(config_path, 'r') as file:
                    config = yaml.safe_load(file)
            else:
                self.logger.error(f"Unsupported configuration file format: {file_extension}")
                print(f"[✗] Unsupported configuration file format: {file_extension}")
                return self.get_default_config()
            
            # Merge with default config to fill in missing values
            merged_config = self.merge_with_defaults(config)
            
            self.logger.info(f"Configuration loaded from {config_path}")
            print(f"[✓] Configuration loaded from {config_path}")
            
            return merged_config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            print(f"[✗] Error loading configuration: {str(e)}")
            return self.get_default_config()
    
    def get_default_config(self):
        """
        Get default configuration
        
        Returns:
            dict: Default configuration dictionary
        """
        return {
            "broker": {
                "username": "",
                "password": "",
                "account_id": "",
                "auto_trading_enabled": True,  
            },
            "database": {
                "type": "mongodb",
                "host": "localhost",
                "port": 27017,
                "db_name": "trading_bot"
            },
            "trading_config": {
                "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "TSLA"],
                "contracts_per_trade": 1,
                "trailing_stop_method": "Heiken Ashi Candle Trail (1-3 candle lookback)",
                "no_trade_window_minutes": 3,
                "auto_close_minutes": 15,
                "cutoff_time": "15:15",
                "ema_value": 15,
                "failsafe_minutes": 20,
                "adx_filter": True,
                "adx_minimum": 20,
                "news_filter": False,
                "bb_width_threshold": 0.05,
                "donchian_contraction_threshold": 0.6,
                "volume_squeeze_threshold": 0.3,
                "liquidity_min_volume": 1000000,
                "liquidity_min_oi": 500,
                "liquidity_max_spread": 0.10,
                "stochastic_k_period": 5,
                "stochastic_d_period": 3,
                "stochastic_smooth": 2,

                # === HEIKEN ASHI CONFIGURATION ===
                "ha_wick_tolerance": 0.1,  # Heiken Ashi wick tolerance (0.1 = 10% of candle range)
                                           # Used to identify strong bullish/bearish candles
                                           # Lower values = stricter signal, fewer trades
                                           # Higher values = more lenient, more trades
                                           # Range: 0.01 (1%) to 0.3 (30%), Default: 0.1 (10%)
                
                # === PRICE CHANGE THRESHOLDS ===
                "sector_price_change_threshold": 0.2,  # Minimum % change to consider sector bullish/bearish
                                                      # Used in sector alignment detection
                                                      # Lower = more sensitive, Higher = less sensitive
                                                      # Range: 0.05% to 1.0%, Default: 0.2%
                
                "mag7_lookback_periods": 5,  # Number of periods to look back for Mag7 average
                                            # Used to calculate moving average for trend detection
                                            # Range: 3 to 20, Default: 5
                
                # === COMPRESSION DETECTION PARAMETERS ===
                "compression_lookback": 20,  # Number of candles to look back for compression
                                           # Used for Bollinger Bands and Donchian Channels
                                           # Range: 10 to 50, Default: 20
                
                "compression_threshold_count": 2,  # How many compression indicators needed (out of 3)
                                                 # 1 = Any single indicator, 2 = At least 2, 3 = All 3
                                                 # Range: 1 to 3, Default: 2
                
                # === ENTRY SIGNAL PARAMETERS ===
                "min_bars_held": 3,  # Minimum bars to hold position before allowing exit
                                    # Prevents premature exits on noise
                                    # Range: 1 to 10, Default: 3
                
                "ha_exit_min_profit": 0.5,  # Minimum profit % before allowing HA reversal exit
                                           # Prevents exits on small reversals when profitable
                                           # Range: 0.1% to 2.0%, Default: 0.5%
                
                # === TREND ALIGNMENT PARAMETERS ===
                "trend_alignment_threshold": 0.0,  # Price distance from VWAP/EMA as % for trend alignment
                                                 # 0 = Price must be exactly above/below
                                                 # 0.1 = Price can be within 0.1% for alignment
                                                 # Range: 0.0% to 0.5%, Default: 0.0%
                
                # === STOCHASTIC PARAMETERS ===
                "stoch_bullish_threshold": 20,  # Stochastic must be above this for bullish trades
                                               # Higher = more selective entries
                                               # Range: 10 to 40, Default: 20
                
                "stoch_bearish_threshold": 80,  # Stochastic must be below this for bearish trades
                                              # Lower = more selective entries
                                              # Range: 60 to 90, Default: 80
                
                "stoch_exit_overbought": 80,  # Exit long positions when stoch crosses down from here
                                             # Range: 70 to 95, Default: 80
                
                "stoch_exit_oversold": 20,  # Exit short positions when stoch crosses up from here
                                          # Range: 5 to 30, Default: 20
                
                # === VOLUME ANALYSIS ===
                "volume_lookback": 10,  # Bars to look back for average volume calculation
                                      # Used for volume spike detection
                                      # Range: 5 to 20, Default: 10
                
                # === TRAILING STOP PARAMETERS ===
                "ha_lookback_candles": 3,  # Number of HA candles to look back for trailing stop
                                         # Range: 1 to 5, Default: 3
                
                "ema_trail_period": 9,  # EMA period for EMA trailing stop
                                      # Range: 5 to 20, Default: 9
                
                "percent_trail_value": 1.5,  # Percentage for % price trailing stop
                                           # Range: 0.5% to 5.0%, Default: 1.5%
                
                "fixed_trail_points": 5.0,  # Fixed points for tick/point trailing stop
                                          # Range: 1.0 to 20.0, Default: 5.0
                
                
                # Sector Configuration
                "sector_etfs": ["XLK", "XLF", "XLV", "XLY"],
                "sector_weight_threshold": 43,
                "sector_weights": {
                    "XLK": 32,  # Tech
                    "XLF": 14,  # Financials
                    "XLV": 11,  # Health Care
                    "XLY": 11   # Consumer Discretionary
                },
                
                # Mag7 Configuration
                "use_mag7_confirmation": False,  # Toggle between sector and Mag7
                "mag7_threshold": 60,  # Default 60% threshold
                "mag7_stocks": ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"],
                
                # Selectable Sector Options
                "selected_sectors": ["XLK", "XLF", "XLV", "XLY"],  # User can select which sectors to use
                "min_sectors_aligned": 2,  # Minimum number of selected sectors that must be aligned
            },
            "ui_config": {
                "theme": "dark",
                "log_level": "info",
                "show_debug_info": False,
                "chart_timeframes": ["1m", "5m", "15m"]
            },
            "logging": {
                "level": "INFO",
                "file_enabled": True,
                "console_enabled": True
            }
        }
    
    def merge_with_defaults(self, config):
        """
        Merge configuration with defaults
        
        Args:
            config (dict): Configuration dictionary
            
        Returns:
            dict: Merged configuration dictionary
        """
        default_config = self.get_default_config()
        
        # Function to recursively merge dictionaries
        def merge_dicts(default_dict, override_dict):
            result = default_dict.copy()
            
            for key, value in override_dict.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = value
                    
            return result
        
        return merge_dicts(default_config, config)
    
    def save_config(self, config, path=None):
        """
        Save configuration to a file
        
        Args:
            config (dict): Configuration dictionary
            path (str, optional): Path to save configuration to
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Use path parameter if provided, otherwise use the one from constructor
        config_path = path if path else self.config_path
        
        # If still no path, use default config file
        if config_path is None:
            config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'settings.yaml'))
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            
            # Save configuration based on file extension
            file_extension = os.path.splitext(config_path)[1].lower()
            
            if file_extension in ['.yaml', '.yml']:
                with open(config_path, 'w') as file:
                    yaml.dump(config, file, default_flow_style=False)
            elif file_extension == '.json':
                with open(config_path, 'w') as file:
                    json.dump(config, file, indent=2)
            elif file_extension == '.txt':
                # Handle TXT files as YAML
                with open(config_path, 'w') as file:
                    yaml.dump(config, file, default_flow_style=False)
            else:
                self.logger.error(f"Unsupported configuration file format: {file_extension}")
                print(f"[✗] Unsupported configuration file format: {file_extension}")
                return False
            
            self.logger.info(f"Configuration saved to {config_path}")
            print(f"[✓] Configuration saved to {config_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
            print(f"[✗] Error saving configuration: {str(e)}")
            return False
    
    def get_credentials(self, path=None):
        """
        Get broker credentials from configuration
        
        Args:
            path (str, optional): Path to credentials file
            
        Returns:
            tuple: (username, password, account_id)
        """
        # Use path parameter if provided, otherwise use default credentials file
        if path is None:
            credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'credentials.txt'))
        else:
            credentials_path = path
        
        # Check if file exists
        if not os.path.exists(credentials_path):
            self.logger.warning(f"Credentials file not found: {credentials_path}")
            print(f"[!] Credentials file not found: {credentials_path}")
            return "", "", ""
        
        try:
            # Load credentials from file
            with open(credentials_path, 'r') as file:
                credentials = yaml.safe_load(file)
            
            if not credentials or not isinstance(credentials, dict):
                self.logger.error("Invalid credentials format")
                print("[✗] Invalid credentials format")
                return "", "", ""
            
            # Extract credentials
            broker_info = credentials.get("broker", {})
            username = broker_info.get("username", "")
            password = broker_info.get("password", "")
            account_id = broker_info.get("account_id", "")
            
            self.logger.info(f"Credentials loaded for user: {username}")
            print(f"[✓] Credentials loaded for user: {username}")
            
            return username, password, account_id
            
        except Exception as e:
            self.logger.error(f"Error loading credentials: {str(e)}")
            print(f"[✗] Error loading credentials: {str(e)}")
            return "", "", ""
    
    def load_trading_config(self, path=None):
        """
        Load trading configuration
        
        Args:
            path (str, optional): Path to trading configuration file
            
        Returns:
            dict: Trading configuration
        """
        # Load full configuration
        config = self.load_config(path)
        
        # Extract trading configuration
        return config.get("trading_config", {})
    
    def save_trading_config(self, trading_config, path=None):
        """
        Save trading configuration
        
        Args:
            trading_config (dict): Trading configuration
            path (str, optional): Path to save configuration to
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Load full configuration
        config = self.load_config(path)
        
        # Update trading configuration
        config["trading_config"] = trading_config
        
        # Save full configuration
        return self.save_config(config, path)

    # Simple function to load config for backward compatibility
    def load_config(self, path=None):
        """
        Load configuration from a file
        
        Args:
            path (str, optional): Path to the configuration file
                
        Returns:
            dict: Configuration dictionary
        """
        # Use path parameter if provided, otherwise use the one from constructor
        config_path = path if path else self.config_path
        
        # Initialize with default config
        config = self.get_default_config()
        
        if config_path and os.path.exists(config_path):
            try:
                # Load configuration based on file extension
                file_extension = os.path.splitext(config_path)[1].lower()
                
                with open(config_path, 'r') as file:
                    if file_extension in ['.yaml', '.yml', '.txt']:
                        loaded_data = yaml.safe_load(file) or {}
                    elif file_extension == '.json':
                        loaded_data = json.load(file)
                    else:
                        self.logger.error(f"Unsupported configuration file format: {file_extension}")
                        return self.get_default_config()
                
                # Merge with defaults
                config = self.merge_with_defaults(loaded_data)
                
                self.logger.info(f"Configuration loaded from {config_path}")
                print(f"[✓] Configuration loaded from {config_path}")
                
                # Debug: Print what was loaded
                if "trading_config" in config:
                    use_mag7 = config["trading_config"].get("use_mag7_confirmation", False)
                    threshold = config["trading_config"].get("sector_weight_threshold", 43) if not use_mag7 else config["trading_config"].get("mag7_threshold", 60)
                    print(f"[DEBUG] Loaded config: Strategy={'Mag7' if use_mag7 else 'Sector'}, Threshold={threshold}%")
                
                return config
                
            except Exception as e:
                self.logger.error(f"Error loading configuration: {str(e)}")
                print(f"[✗] Error loading configuration: {str(e)}")
        
        return config