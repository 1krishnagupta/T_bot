# Code/bot_core/position_manager.py

import logging
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from Code.bot_core.mongodb_handler import get_mongodb_handler, COLLECTIONS

class PositionManager:
    """
    Manages persistent position tracking with MongoDB backup
    """
    
    def __init__(self):
        """Initialize the position manager with MongoDB connection"""
        self.db = get_mongodb_handler()
        
        # In-memory position cache
        self.active_positions = {}
        self.position_history = []
        
        # Setup logging
        today = datetime.now().strftime("%Y-%m-%d")
        log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, f"position_manager_{today}.log")
        
        self.logger = logging.getLogger("PositionManager")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Create collections if they don't exist
        self._initialize_collections()
        
        # Load existing positions on startup
        self._load_positions_from_db()
    
    def _initialize_collections(self):
        """Create necessary collections with proper indexes"""
        try:
            # Create collections
            self.db.create_collection(COLLECTIONS['POSITIONS'])
            self.db.create_collection(COLLECTIONS['POSITION_HISTORY'])
            
            # Create indexes for better query performance
            self.db.create_index(COLLECTIONS['POSITIONS'], [("symbol", 1), ("status", 1)])
            self.db.create_index(COLLECTIONS['POSITIONS'], [("entry_time", -1)])
            self.db.create_index(COLLECTIONS['POSITION_HISTORY'], [("symbol", 1), ("exit_time", -1)])
            
            self.logger.info("Position collections initialized")
        except Exception as e:
            self.logger.error(f"Error initializing collections: {e}")
    
    def _load_positions_from_db(self):
        """Load active positions from database on startup"""
        try:
            # Query for all active positions
            active_positions = self.db.find_many(
                COLLECTIONS['POSITIONS'], 
                {"status": {"$in": ["Open", "Pending"]}}
            )
            
            # Load into memory
            for position in active_positions:
                symbol = position.get("symbol")
                if symbol:
                    self.active_positions[symbol] = position
                    self.logger.info(f"Loaded active position for {symbol} from database")
            
            self.logger.info(f"Loaded {len(self.active_positions)} active positions from database")
            
            # Also load recent position history
            recent_history = self.db.find_many(
                COLLECTIONS['POSITION_HISTORY'],
                {},
                limit=100
            )
            self.position_history = recent_history
            
        except Exception as e:
            self.logger.error(f"Error loading positions from database: {e}")
    
    def add_position(self, symbol: str, position_data: Dict) -> bool:
        """
        Add a new position and save to database
        
        Args:
            symbol: Trading symbol
            position_data: Position details
            
        Returns:
            bool: Success status
        """
        try:
            # Ensure required fields
            position_data["symbol"] = symbol
            position_data["last_update"] = datetime.now().isoformat()
            position_data["status"] = position_data.get("status", "Open")
            
            # Add to memory
            self.active_positions[symbol] = position_data
            
            # Save to database
            # Remove any existing position for this symbol first
            self.db.delete_one(COLLECTIONS['POSITIONS'], {"symbol": symbol})
            
            # Insert new position
            position_id = self.db.insert_one(COLLECTIONS['POSITIONS'], position_data)
            
            if position_id:
                self.logger.info(f"Added position for {symbol} to database with ID: {position_id}")
                return True
            else:
                self.logger.error(f"Failed to save position for {symbol} to database")
                return False
                
        except Exception as e:
            self.logger.error(f"Error adding position for {symbol}: {e}")
            return False
    
    def update_position(self, symbol: str, updates: Dict) -> bool:
        """
        Update an existing position
        
        Args:
            symbol: Trading symbol
            updates: Fields to update
            
        Returns:
            bool: Success status
        """
        try:
            if symbol not in self.active_positions:
                self.logger.warning(f"Position for {symbol} not found in memory")
                return False
            
            # Update in memory
            self.active_positions[symbol].update(updates)
            self.active_positions[symbol]["last_update"] = datetime.now().isoformat()
            
            # Update in database
            updates["last_update"] = datetime.now().isoformat()
            success = self.db.update_one(
                COLLECTIONS['POSITIONS'],
                {"symbol": symbol},
                updates
            )
            
            if success:
                self.logger.info(f"Updated position for {symbol}")
            else:
                self.logger.error(f"Failed to update position for {symbol} in database")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating position for {symbol}: {e}")
            return False
    
    def close_position(self, symbol: str, exit_data: Dict) -> bool:
        """
        Close a position and move to history
        
        Args:
            symbol: Trading symbol
            exit_data: Exit details (price, time, reason, etc.)
            
        Returns:
            bool: Success status
        """
        try:
            if symbol not in self.active_positions:
                self.logger.warning(f"Position for {symbol} not found")
                return False
            
            # Get position data
            position = self.active_positions[symbol].copy()
            
            # Update with exit data
            position.update(exit_data)
            position["status"] = "Closed"
            position["exit_time"] = position.get("exit_time", datetime.now().isoformat())
            position["last_update"] = datetime.now().isoformat()
            
            # Calculate P&L if not provided
            if "pnl" not in position and "entry_price" in position and "exit_price" in exit_data:
                entry_price = float(position["entry_price"])
                exit_price = float(exit_data["exit_price"])
                quantity = float(position.get("quantity", 1))
                
                if position.get("type") in ["Long", "Long Call", "Long Put"]:
                    pnl = (exit_price - entry_price) * quantity * 100  # Options multiplier
                else:
                    pnl = (entry_price - exit_price) * quantity * 100
                    
                position["pnl"] = pnl
                position["pnl_percent"] = (pnl / (entry_price * quantity * 100)) * 100
            
            # Remove from active positions in memory
            del self.active_positions[symbol]
            
            # Remove from active positions in database
            self.db.delete_one(COLLECTIONS['POSITIONS'], {"symbol": symbol})
            
            # Add to position history
            history_id = self.db.insert_one(COLLECTIONS['POSITION_HISTORY'], position)
            
            # Add to memory history
            self.position_history.append(position)
            
            # Keep only recent history in memory
            if len(self.position_history) > 100:
                self.position_history = self.position_history[-100:]
            
            self.logger.info(f"Closed position for {symbol} with P&L: ${position.get('pnl', 0):.2f}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error closing position for {symbol}: {e}")
            return False
    
    def get_position(self, symbol: str) -> Optional[Dict]:
        """Get a specific position"""
        return self.active_positions.get(symbol)
    
    def get_all_positions(self) -> Dict[str, Dict]:
        """Get all active positions"""
        return self.active_positions.copy()
    
    def get_position_count(self) -> int:
        """Get count of active positions"""
        return len(self.active_positions)
    
    def get_total_pnl(self) -> float:
        """Calculate total P&L for all active positions"""
        total_pnl = 0.0
        for position in self.active_positions.values():
            pnl = position.get("unrealized_pnl", 0.0)
            if isinstance(pnl, (int, float)):
                total_pnl += pnl
        return total_pnl
    
    def sync_with_broker(self, broker_positions: List[Dict]):
        """
        Sync positions with broker data
        
        Args:
            broker_positions: List of positions from broker
        """
        try:
            broker_symbols = set()
            
            # Update or add positions from broker
            for broker_pos in broker_positions:
                symbol = broker_pos.get("symbol")
                if not symbol:
                    continue
                    
                broker_symbols.add(symbol)
                
                if symbol in self.active_positions:
                    # Update existing position
                    self.update_position(symbol, {
                        "quantity": broker_pos.get("quantity"),
                        "current_price": broker_pos.get("current_price"),
                        "unrealized_pnl": broker_pos.get("unrealized_pnl"),
                        "broker_sync": datetime.now().isoformat()
                    })
                else:
                    # Add new position found at broker
                    self.add_position(symbol, {
                        "type": broker_pos.get("type", "Unknown"),
                        "quantity": broker_pos.get("quantity"),
                        "entry_price": broker_pos.get("average_price"),
                        "current_price": broker_pos.get("current_price"),
                        "unrealized_pnl": broker_pos.get("unrealized_pnl"),
                        "entry_time": broker_pos.get("opened_at", datetime.now().isoformat()),
                        "broker_sync": datetime.now().isoformat(),
                        "source": "broker_sync"
                    })
            
            # Mark positions not at broker as potentially closed
            for symbol in list(self.active_positions.keys()):
                if symbol not in broker_symbols:
                    self.logger.warning(f"Position {symbol} not found at broker, marking as closed")
                    self.close_position(symbol, {
                        "exit_time": datetime.now().isoformat(),
                        "exit_reason": "Not found at broker during sync"
                    })
                    
        except Exception as e:
            self.logger.error(f"Error syncing with broker: {e}")
    
    def cleanup_stale_positions(self, hours: int = 24):
        """
        Clean up positions that haven't been updated in specified hours
        
        Args:
            hours: Number of hours before considering position stale
        """
        try:
            current_time = datetime.now()
            stale_positions = []
            
            for symbol, position in self.active_positions.items():
                last_update = position.get("last_update")
                if last_update:
                    try:
                        update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                        if (current_time - update_time).total_seconds() > hours * 3600:
                            stale_positions.append(symbol)
                    except:
                        pass
            
            for symbol in stale_positions:
                self.logger.warning(f"Closing stale position for {symbol}")
                self.close_position(symbol, {
                    "exit_reason": f"Stale position - no update for {hours} hours"
                })
                
        except Exception as e:
            self.logger.error(f"Error cleaning up stale positions: {e}")
    
    def export_positions(self, filepath: str):
        """Export all positions to a JSON file for backup"""
        try:
            export_data = {
                "export_time": datetime.now().isoformat(),
                "active_positions": self.active_positions,
                "recent_history": self.position_history[-50:]  # Last 50 closed positions
            }
            
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2)
                
            self.logger.info(f"Exported positions to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error exporting positions: {e}")
    
    def get_position_summary(self) -> Dict:
        """Get summary statistics of positions"""
        summary = {
            "total_positions": len(self.active_positions),
            "long_positions": 0,
            "short_positions": 0,
            "total_unrealized_pnl": 0.0,
            "winning_positions": 0,
            "losing_positions": 0,
            "positions_by_symbol": {}
        }
        
        for symbol, position in self.active_positions.items():
            # Count position types
            if position.get("type") in ["Long", "Long Call", "Long Put"]:
                summary["long_positions"] += 1
            else:
                summary["short_positions"] += 1
            
            # Calculate P&L
            pnl = position.get("unrealized_pnl", 0.0)
            if isinstance(pnl, (int, float)):
                summary["total_unrealized_pnl"] += pnl
                if pnl > 0:
                    summary["winning_positions"] += 1
                elif pnl < 0:
                    summary["losing_positions"] += 1
            
            # Group by symbol
            summary["positions_by_symbol"][symbol] = {
                "type": position.get("type"),
                "quantity": position.get("quantity"),
                "unrealized_pnl": pnl
            }
        
        return summary