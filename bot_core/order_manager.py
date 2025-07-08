# Code/bot_core/order_manager.py

import logging
import json
import time
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Any, Union
from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class OrderManager:
    """
    Manages order creation, submission, and tracking for the trading bot.
    Implements TastyTrade's order format and handling logic.
    """
    
    def __init__(self, api, account_id=None):
        """
        Initialize the order manager
        
        Args:
            api: TastyTrade API client
            account_id (str): TastyTrade account ID, will be loaded from API if None
        """
        self.api = api
        self.account_id = account_id
        
        # Setup logging FIRST before any other operations
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"order_manager_{today}.log")
        
        self.logger = logging.getLogger("OrderManager")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Ensure we have an account ID
        if not self.account_id:
            self.account_id = self._get_account_id()
            
        # Order tracking containers
        self.active_orders = {}  # Currently active orders
        self.order_history = {}  # All past orders
        
        # Initialize MongoDB for persistent order tracking
        self.db = get_mongodb_handler()
        
        # Create orders collection if it doesn't exist
        self.db.create_collection(COLLECTIONS['ORDERS'])
        self.db.create_index(COLLECTIONS['ORDERS'], [("order_id", 1)])
        self.db.create_index(COLLECTIONS['ORDERS'], [("status", 1), ("created_at", -1)])
        
        # Load active orders from database on startup
        self._load_active_orders_from_db()
            
    def _get_account_id(self) -> str:
        """
        Get the account ID from the API
        
        Returns:
            str: Account ID or empty string if not found
        """
        try:
            accounts = self.api.safe_request("GET", "/accounts")
            if accounts.status_code == 200:
                account_data = accounts.json().get("data", {}).get("items", [])
                if account_data:
                    return account_data[0].get("account", {}).get("account-number", "")
        except Exception as e:
            self.logger.error(f"Error getting account ID: {e}")
        
        return ""
    

    def _load_active_orders_from_db(self):
        """Load active orders from database on startup"""
        try:
            # Query for active orders
            active_orders = self.db.find_many(
                COLLECTIONS['ORDERS'],
                {"status": {"$in": ["Open", "Pending", "Working", "Partially Filled"]}}
            )
            
            # Load into memory
            for order in active_orders:
                order_id = order.get("order_id")
                if order_id:
                    self.active_orders[order_id] = order
                    
            self.logger.info(f"Loaded {len(self.active_orders)} active orders from database")
            
        except Exception as e:
            self.logger.error(f"Error loading orders from database: {e}")


    def create_equity_option_order(self, symbol, quantity, direction, price=None, order_type="Limit", time_in_force="Day") -> Dict:
        """
        Create an equity option order
        
        Args:
            symbol (str): Option symbol in OCC format (e.g., "SPY 230731C00393000")
            quantity (int): Number of contracts to trade
            direction (str): "Buy to Open", "Sell to Open", "Buy to Close", or "Sell to Close"
            price (float): Order price (required for Limit orders)
            order_type (str): "Limit" or "Market"
            time_in_force (str): "Day", "GTC", or "GTD"
            
        Returns:
            dict: Order JSON
        """
        # Validate input
        if order_type == "Limit" and price is None:
            raise ValueError("Price is required for Limit orders")
            
        # Create order
        order = {
            "time-in-force": time_in_force,
            "order-type": order_type,
            "legs": [
                {
                    "instrument-type": "Equity Option",
                    "symbol": symbol,
                    "quantity": quantity,
                    "action": direction
                }
            ]
        }
        
        # Add price if provided
        if price is not None:
            order["price"] = str(price)
            # For price-effect, determine if it's a debit or credit based on action
            if direction.startswith("Buy"):
                order["price-effect"] = "Debit"
            else:
                order["price-effect"] = "Credit"
                
        return order
    
    def create_multi_leg_option_order(self, legs, price=None, order_type="Limit", time_in_force="Day") -> Dict:
        """
        Create a multi-leg option order (spreads)
        
        Args:
            legs (list): List of leg dictionaries, each containing:
                - symbol (str): Option symbol
                - quantity (int): Number of contracts
                - direction (str): "Buy to Open", "Sell to Open", etc.
            price (float): Order price (required for Limit orders)
            order_type (str): "Limit" or "Market"
            time_in_force (str): "Day", "GTC", or "GTD"
            
        Returns:
            dict: Order JSON
        """
        # Validate input
        if order_type == "Limit" and price is None:
            raise ValueError("Price is required for Limit orders")
            
        if not legs or len(legs) == 0:
            raise ValueError("At least one leg is required")
            
        if len(legs) > 4:
            raise ValueError("Maximum of 4 legs allowed for option orders")
            
        # Create order legs
        order_legs = []
        for leg in legs:
            order_legs.append({
                "instrument-type": "Equity Option",
                "symbol": leg["symbol"],
                "quantity": leg["quantity"],
                "action": leg["direction"]
            })
            
        # Create order
        order = {
            "time-in-force": time_in_force,
            "order-type": order_type,
            "legs": order_legs
        }
        
        # Add price if provided
        if price is not None:
            order["price"] = str(price)
            
            # For price-effect, determine based on overall strategy intent
            # If first leg is a buy, assume it's a debit spread
            if legs[0]["direction"].startswith("Buy"):
                order["price-effect"] = "Debit"
            else:
                order["price-effect"] = "Credit"
                
        return order
    
    def create_otoco_order(self, entry_order, profit_order, stop_order) -> Dict:
        """
        Create an OTOCO (One Triggers OCO) complex order
        
        Args:
            entry_order (dict): Entry order JSON
            profit_order (dict): Profit target order JSON
            stop_order (dict): Stop loss order JSON
            
        Returns:
            dict: OTOCO order JSON
        """
        return {
            "type": "OTOCO",
            "trigger-order": entry_order,
            "orders": [
                profit_order,
                stop_order
            ]
        }
    
    def dry_run_order(self, order) -> Dict:
        """
        Submit an order for dry run (validation without execution)
        
        Args:
            order (dict): Order JSON
            
        Returns:
            dict: Dry run response
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/orders/dry-run"
        response = self.api.safe_request("POST", url, json=order)
        
        if response.status_code == 200:
            return response.json().get("data", {})
        else:
            self.logger.error(f"Order dry run failed: {response.status_code} {response.text}")
            return {"error": response.text}
    
    def submit_order(self, order) -> Dict:
        """
        Submit an order for execution
        
        Args:
            order (dict): Order JSON
            
        Returns:
            dict: Order response
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/orders"
        
        # Log the order being submitted
        self.logger.info(f"Submitting LIVE order to TastyTrade: {json.dumps(order)}")
        
        response = self.api.safe_request("POST", url, json=order)
        
        if response.status_code == 201:
            data = response.json().get("data", {})
            order_data = data.get("order", {})
            order_id = order_data.get("id")
            
            if order_id:
                self.active_orders[order_id] = order_data
                self.order_history[order_id] = order_data
                
                # Save to database
                order_doc = {
                    "order_id": order_id,
                    "order_data": order_data,
                    "status": "Open",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                self.db.insert_one(COLLECTIONS['ORDERS'], order_doc)
                
            self.logger.info(f"LIVE order submitted successfully. Order ID: {order_id}")
            return data
        else:
            self.logger.error(f"Order submission failed: {response.status_code} {response.text}")
            return {"error": response.text}
            
    
    def submit_complex_order(self, complex_order) -> Dict:
        """
        Submit a complex order (OTOCO, OCO, etc.)
        
        Args:
            complex_order (dict): Complex order JSON
            
        Returns:
            dict: Complex order response
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/complex-orders"
        response = self.api.safe_request("POST", url, json=complex_order)
        
        if response.status_code == 201:
            return response.json().get("data", {})
        else:
            self.logger.error(f"Complex order submission failed: {response.status_code} {response.text}")
            return {"error": response.text}
    
    def cancel_order(self, order_id) -> bool:
        """
        Cancel an active order
        
        Args:
            order_id (str): Order ID to cancel
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/orders/{order_id}"
        response = self.api.safe_request("DELETE", url)
        
        if response.status_code == 204:
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            return True
        else:
            self.logger.error(f"Order cancellation failed: {response.status_code} {response.text}")
            return False
    
    def get_order_status(self, order_id) -> Dict:
        """
        Get the current status of an order
        
        Args:
            order_id (str): Order ID to check
            
        Returns:
            dict: Order status data
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/orders/{order_id}"
        response = self.api.safe_request("GET", url)
        
        if response.status_code == 200:
            data = response.json().get("data", {})
            
            # Update our local tracking
            if data.get("id"):
                self.order_history[data["id"]] = data
                
                # If order is in a terminal state, remove from active orders
                status = data.get("status")
                if status in ["Filled", "Canceled", "Rejected", "Expired"]:
                    if data["id"] in self.active_orders:
                        del self.active_orders[data["id"]]
                else:
                    # Otherwise, update active orders
                    self.active_orders[data["id"]] = data
                    
            return data
        else:
            self.logger.error(f"Get order status failed: {response.status_code} {response.text}")
            return {"error": response.text}
    
    def get_active_orders(self) -> List[Dict]:
        """
        Get all active orders
        
        Returns:
            list: List of active order objects
        """
        if not self.account_id:
            raise ValueError("No account ID available")
            
        url = f"/accounts/{self.account_id}/orders/live"
        response = self.api.safe_request("GET", url)
        
        if response.status_code == 200:
            data = response.json().get("data", {}).get("items", [])
            
            # Update local tracking
            self.active_orders = {}
            for order in data:
                if order.get("id"):
                    self.active_orders[order["id"]] = order
                    self.order_history[order["id"]] = order
                    
            return list(self.active_orders.values())
        else:
            self.logger.error(f"Get active orders failed: {response.status_code} {response.text}")
            return []
            
    def calculate_option_order_cost(self, order_data):
        """
        Calculate the cost of an option order
        
        Args:
            order_data (dict): Order data from dry run
            
        Returns:
            float: Order cost
        """
        try:
            # Get buying power effect from order data
            bp_effect = order_data.get("buying-power-effect", {})
            
            # Get impact and effect
            impact = float(bp_effect.get("impact", 0))
            effect = bp_effect.get("effect", "")
            
            # Calculate cost based on effect (Debit = negative, Credit = positive)
            if effect == "Debit":
                return -impact
            elif effect == "Credit":
                return impact
            else:
                return 0
        except Exception as e:
            self.logger.error(f"Error calculating order cost: {e}")
            return 0

    
    # In order_manager.py, add a kill_all_orders method
    def kill_all_orders(self):
        """
        Cancel all active orders and close all positions
        
        Returns:
            dict: Results of the operation
        """
        if not self.account_id:
            raise ValueError("No account ID available")
        
        result = {
            "orders_canceled": 0,
            "positions_closed": 0,
            "errors": []
        }
    
        # First, get all active orders
        active_orders = self.get_active_orders()
        
        # Cancel each active order
        for order in active_orders:
            order_id = order.get("id")
            if order_id:
                if self.cancel_order(order_id):
                    result["orders_canceled"] += 1
                else:
                    result["errors"].append(f"Failed to cancel order {order_id}")
        
        # Then, get all open positions and close them
        positions = self.get_positions()
        
        for position in positions:
            symbol = position.get("symbol")
            quantity = position.get("quantity", 0)
            
            if symbol and quantity != 0:
                # Determine if it's a long or short position
                is_long = quantity > 0
                
                # Create a closing order
                if is_long:
                    # Create a sell order to close long position
                    order = self.create_market_order(
                        symbol=symbol,
                        quantity=abs(quantity),
                        direction="Sell to Close"
                    )
                else:
                    # Create a buy order to close short position
                    order = self.create_market_order(
                        symbol=symbol,
                        quantity=abs(quantity),
                        direction="Buy to Close"
                    )
                
                # Submit the order
                order_result = self.submit_order(order)
                
                if "error" not in order_result:
                    result["positions_closed"] += 1
                else:
                    result["errors"].append(f"Failed to close position for {symbol}: {order_result['error']}")
        
        return result


    def create_market_order(self, symbol, quantity, direction):
        """
        Create a market order for any instrument
        
        Args:
            symbol (str): Instrument symbol
            quantity (float): Quantity to trade
            direction (str): "Buy to Open", "Sell to Open", "Buy to Close", or "Sell to Close"
            
        Returns:
            dict: Order JSON
        """
        # Determine instrument type based on symbol
        if " " in symbol and len(symbol) > 15:  # Option symbol
            instrument_type = "Equity Option"
        elif "/" in symbol:  # Cryptocurrency
            instrument_type = "Cryptocurrency"
        elif symbol.startswith("/"):  # Future
            instrument_type = "Future"
        else:  # Equity
            instrument_type = "Equity"
        
        # Create order
        order = {
            "time-in-force": "Day",
            "order-type": "Market",
            "legs": [
                {
                    "instrument-type": instrument_type,
                    "symbol": symbol,
                    "quantity": quantity,
                    "action": direction
                }
            ]
        }
        
        return order