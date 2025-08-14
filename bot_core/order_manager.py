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
    Implements TradeStation's order format and handling logic.
    """
    
    def __init__(self, api, account_id=None):
        """
        Initialize the order manager
        
        Args:
            api: TradeStation API client
            account_id (str): TradeStation account ID, will be loaded from API if None
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
            response = self.api.safe_request("GET", "/v3/brokerage/accounts")
            if response.status_code == 200:
                accounts = response.json().get("Accounts", [])
                if accounts:
                    return accounts[0].get("AccountID", "")
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
        
        # Convert symbol to TradeStation format if needed
        symbol = self._convert_option_symbol(symbol)
        
        # Convert direction to TradeStation format
        action_map = {
            "Buy to Open": "BuyToOpen",
            "Sell to Open": "SellToOpen",
            "Buy to Close": "BuyToClose",
            "Sell to Close": "SellToClose"
        }
        
        action = action_map.get(direction, "BuyToOpen")
        
        # Create order
        order = {
            "AccountID": self.account_id,
            "Symbol": symbol,
            "Quantity": str(quantity),
            "OrderType": order_type,
            "TimeInForce": {"Duration": time_in_force},
            "Route": "Intelligent",
            "AssetType": "OP"  # Specify this is an option order
        }
        
        # Add price if provided
        if price is not None:
            if order_type == "Limit":
                order["LimitPrice"] = str(price)
            elif order_type == "StopMarket":
                order["StopPrice"] = str(price)
        
        # Add action
        order["TradeAction"] = action
        
        return order
    
    def create_multi_leg_option_order(self, legs, price=None, order_type="Limit", time_in_force="Day") -> Dict:
        """
        Create a multi-leg option order (spreads)
        
        Args:
            legs (list): List of leg dictionaries
            price (float): Order price (required for Limit orders)
            order_type (str): "Limit" or "Market"
            time_in_force (str): "Day", "GTC", or "GTD"
            
        Returns:
            dict: Order JSON
        """
        # TradeStation uses a different format for multi-leg orders
        # For now, create as separate orders
        # In production, would use TradeStation's spread order format
        
        if not legs or len(legs) == 0:
            raise ValueError("At least one leg is required")
        
        # For simplicity, return the first leg as a single order
        # Real implementation would create a proper spread order
        first_leg = legs[0]
        return self.create_equity_option_order(
            symbol=first_leg["symbol"],
            quantity=first_leg["quantity"],
            direction=first_leg["direction"],
            price=price,
            order_type=order_type,
            time_in_force=time_in_force
        )
    
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
        # TradeStation uses OSO (Order Sends Order) for this
        # Simplified implementation
        return {
            "Type": "OSO",
            "Orders": [entry_order, profit_order, stop_order]
        }
    
    def dry_run_order(self, order) -> Dict:
        """
        Submit an order for dry run (validation without execution)
        
        Args:
            order (dict): Order JSON
            
        Returns:
            dict: Dry run response
        """
        # TradeStation doesn't have a specific dry run endpoint
        # We can validate locally
        try:
            # Basic validation
            required_fields = ["AccountID", "Symbol", "Quantity", "OrderType", "TradeAction"]
            for field in required_fields:
                if field not in order:
                    return {"error": f"Missing required field: {field}"}
            
            # Validate order type
            valid_order_types = ["Market", "Limit", "StopMarket", "StopLimit"]
            if order["OrderType"] not in valid_order_types:
                return {"error": f"Invalid order type: {order['OrderType']}"}
            
            # Validate trade action
            valid_actions = ["Buy", "Sell", "BuyToOpen", "SellToOpen", "BuyToClose", "SellToClose"]
            if order["TradeAction"] not in valid_actions:
                return {"error": f"Invalid trade action: {order['TradeAction']}"}
            
            # If validation passes
            return {
                "valid": True,
                "order": order,
                "estimated_cost": float(order.get("Quantity", 0)) * 100  # Rough estimate
            }
            
        except Exception as e:
            self.logger.error(f"Error in dry run validation: {e}")
            return {"error": str(e)}
    
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
        
        # Ensure account ID is in the order
        order["AccountID"] = self.account_id
        
        # Log the order being submitted
        self.logger.info(f"Submitting LIVE order to TradeStation: {json.dumps(order)}")
        
        # Submit to TradeStation
        response = self.api.safe_request("POST", "/v3/brokerage/orders", json=order)
        
        if response.status_code in [200, 201]:
            data = response.json()
            order_id = data.get("OrderID")
            
            if order_id:
                # Store order data
                order_data = {
                    "id": order_id,
                    "status": data.get("Status", "Sent"),
                    "symbol": order.get("Symbol"),
                    "quantity": order.get("Quantity"),
                    "order_type": order.get("OrderType"),
                    "action": order.get("TradeAction"),
                    "submitted_at": datetime.now().isoformat()
                }
                
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
                return {"order": order_data}
            else:
                return {"error": "No order ID returned"}
        else:
            error_msg = f"Order submission failed: {response.status_code}"
            try:
                error_detail = response.json()
                error_msg += f" - {error_detail}"
            except:
                error_msg += f" - {response.text}"
            
            self.logger.error(error_msg)
            return {"error": error_msg}
    
    def submit_complex_order(self, complex_order) -> Dict:
        """
        Submit a complex order (OTOCO, OCO, etc.)
        
        Args:
            complex_order (dict): Complex order JSON
            
        Returns:
            dict: Complex order response
        """
        # TradeStation handles complex orders differently
        # For now, submit as individual orders
        if "Orders" in complex_order:
            results = []
            for order in complex_order["Orders"]:
                result = self.submit_order(order)
                results.append(result)
            return {"results": results}
        else:
            return self.submit_order(complex_order)
    
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
        
        url = f"/v3/brokerage/orders/{order_id}"
        response = self.api.safe_request("DELETE", url)
        
        if response.status_code in [200, 204]:
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            
            # Update database
            self.db.update_one(
                COLLECTIONS['ORDERS'],
                {"order_id": order_id},
                {"status": "Cancelled", "updated_at": datetime.now().isoformat()}
            )
            
            return True
        else:
            self.logger.error(f"Order cancellation failed: {response.status_code}")
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
        
        url = f"/v3/brokerage/orders/{order_id}"
        response = self.api.safe_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Update our local tracking
            order_data = {
                "id": data.get("OrderID"),
                "status": data.get("Status"),
                "filled_quantity": data.get("FilledQuantity", 0),
                "remaining_quantity": data.get("RemainingQuantity", 0),
                "average_fill_price": data.get("AverageFilledPrice", 0),
                "updated_at": datetime.now().isoformat()
            }
            
            # Update tracking based on status
            status = data.get("Status", "")
            if status in ["FLL", "CAN", "REJ", "EXP"]:  # Filled, Cancelled, Rejected, Expired
                if order_id in self.active_orders:
                    del self.active_orders[order_id]
            else:
                self.active_orders[order_id] = order_data
            
            self.order_history[order_id] = order_data
            
            return order_data
        else:
            self.logger.error(f"Get order status failed: {response.status_code}")
            return {"error": f"Failed to get order status: {response.status_code}"}
    
    def get_active_orders(self) -> List[Dict]:
        """
        Get all active orders
        
        Returns:
            list: List of active order objects
        """
        if not self.account_id:
            raise ValueError("No account ID available")
        
        url = f"/v3/brokerage/accounts/{self.account_id}/orders"
        response = self.api.safe_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            orders = data.get("Orders", [])
            
            # Update local tracking
            self.active_orders = {}
            for order in orders:
                order_id = order.get("OrderID")
                if order_id and order.get("Status") not in ["FLL", "CAN", "REJ", "EXP"]:
                    order_data = {
                        "id": order_id,
                        "status": order.get("Status"),
                        "symbol": order.get("Symbol"),
                        "quantity": order.get("Quantity"),
                        "order_type": order.get("OrderType"),
                        "action": order.get("TradeAction")
                    }
                    self.active_orders[order_id] = order_data
                    self.order_history[order_id] = order_data
            
            return list(self.active_orders.values())
        else:
            self.logger.error(f"Get active orders failed: {response.status_code}")
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
            # Basic calculation
            quantity = float(order_data.get("Quantity", 0))
            price = float(order_data.get("LimitPrice", 0))
            
            # Options are traded in contracts of 100
            cost = quantity * price * 100
            
            # For debit trades (buying), cost is negative
            if order_data.get("TradeAction", "").startswith("Buy"):
                return -cost
            else:
                return cost
                
        except Exception as e:
            self.logger.error(f"Error calculating order cost: {e}")
            return 0
    
    def get_positions(self):
        """
        Get current positions from the broker
        
        Returns:
            list: List of position objects
        """
        if not self.account_id:
            raise ValueError("No account ID available")
        
        try:
            url = f"/v3/brokerage/accounts/{self.account_id}/positions"
            response = self.api.safe_request("GET", url)
            
            if response.status_code == 200:
                data = response.json()
                positions = data.get("Positions", [])
                
                # Convert to common format
                formatted_positions = []
                for pos in positions:
                    formatted_pos = {
                        "symbol": pos.get("Symbol"),
                        "quantity": float(pos.get("Quantity", 0)),
                        "average_price": float(pos.get("AveragePrice", 0)),
                        "current_price": float(pos.get("Last", 0)),
                        "market_value": float(pos.get("MarketValue", 0)),
                        "unrealized_pnl": float(pos.get("UnrealizedProfitLoss", 0)),
                        "position_type": pos.get("PositionType", "Long")
                    }
                    formatted_positions.append(formatted_pos)
                
                return formatted_positions
            else:
                self.logger.error(f"Failed to get positions: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            return []
    
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
        # Determine if it's an option based on symbol format
        if " " in symbol and len(symbol) > 15:  # Option symbol
            return self.create_equity_option_order(
                symbol=symbol,
                quantity=quantity,
                direction=direction,
                order_type="Market"
            )
        else:  # Equity
            # Convert direction for equities
            action_map = {
                "Buy to Open": "Buy",
                "Sell to Open": "Sell",
                "Buy to Close": "Sell",
                "Sell to Close": "Buy"
            }
            
            action = action_map.get(direction, "Buy")
            
            return {
                "AccountID": self.account_id,
                "Symbol": symbol,
                "Quantity": str(quantity),
                "OrderType": "Market",
                "TimeInForce": {"Duration": "Day"},
                "TradeAction": action,
                "Route": "Intelligent"
            }
        

    
    def _convert_option_symbol(self, symbol):
        """
        Convert option symbol to TradeStation format if needed
        
        Args:
            symbol (str): Option symbol (various formats)
            
        Returns:
            str: TradeStation formatted option symbol
        """
        # Check if it's already in TradeStation format
        if symbol and " " in symbol and len(symbol.split()) == 2:
            # Looks like TradeStation format already
            return symbol
        
        # If it's in OCC format (SPY 240816C00550000), it should work
        # But let's validate and potentially reformat
        
        try:
            parts = symbol.split()
            if len(parts) == 2:
                underlying = parts[0]
                option_part = parts[1]
                
                # Validate the format
                if len(option_part) >= 15:  # Should be YYMMDDCP########
                    return symbol  # Already in correct format
            
            # If we get here, try to parse and rebuild
            # This is a fallback - ideally symbols should come in correct format
            self.logger.warning(f"Option symbol {symbol} may not be in correct format")
            return symbol
            
        except Exception as e:
            self.logger.error(f"Error converting option symbol {symbol}: {e}")
            return symbol

    