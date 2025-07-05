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
    
    # In bot_core/config_loader.py
# Find the get_default_config method and update the trading_config section:

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
            path (str, optional): Path to the configuration file, overrides the path
                provided in the constructor
                
        Returns:
            dict: Configuration dictionary
        """
        # Use path parameter if provided, otherwise use the one from constructor
        config_path = path if path else self.config_path
        
        # Initialize config with defaults
        config = self.get_default_config()
        
        # If this is credentials.txt, load broker info only
        if config_path and 'credentials' in config_path:
            if os.path.exists(config_path):
                try:
                    with open(config_path, 'r') as file:
                        cred_data = yaml.safe_load(file)
                        if cred_data and "broker" in cred_data:
                            config["broker"] = cred_data["broker"]
                except Exception as e:
                    self.logger.error(f"Error loading credentials: {e}")
            
            # Also try to load settings.yaml for trading config
            settings_path = os.path.join(os.path.dirname(config_path), 'settings.yaml')
            if os.path.exists(settings_path):
                try:
                    with open(settings_path, 'r') as file:
                        settings_data = yaml.safe_load(file)
                        if settings_data and "trading_config" in settings_data:
                            config["trading_config"] = settings_data["trading_config"]
                            self.logger.info(f"Loaded trading config from settings.yaml")
                except Exception as e:
                    self.logger.error(f"Error loading settings: {e}")
                    
            return config
        
        # For other files, load normally
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
                    loaded_config = yaml.safe_load(file) or {}
            elif file_extension == '.json':
                with open(config_path, 'r') as file:
                    loaded_config = json.load(file)
            elif file_extension == '.txt':
                # Handle TXT files as YAML
                with open(config_path, 'r') as file:
                    loaded_config = yaml.safe_load(file) or {}
            else:
                self.logger.error(f"Unsupported configuration file format: {file_extension}")
                print(f"[✗] Unsupported configuration file format: {file_extension}")
                return self.get_default_config()
            
            # Merge with default config to fill in missing values
            merged_config = self.merge_with_defaults(loaded_config)
            
            self.logger.info(f"Configuration loaded from {config_path}")
            print(f"[✓] Configuration loaded from {config_path}")
            
            return merged_config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            print(f"[✗] Error loading configuration: {str(e)}")
            return self.get_default_config()