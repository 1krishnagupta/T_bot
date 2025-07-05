#!/usr/bin/env python3
"""
Jigsaw Flow Trading Bot - Complete System Test with ALL Features
Tests every feature including stop loss, position management, kill switch, etc.
"""

import sys
import os
import time
import threading
import random
import json
from datetime import datetime, timedelta
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# Add project to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import all necessary components
from Code.bot_core.tastytrade_api import TastyTradeAPI
from Code.bot_core.config_loader import ConfigLoader
from Code.bot_core.instrument_fetcher import InstrumentFetcher
from Code.bot_core.market_data_client import MarketDataClient
from Code.bot_core.order_manager import OrderManager
from Code.bot_core.jigsaw_strategy import JigsawStrategy
from Code.bot_core.position_manager import PositionManager
from Code.bot_core.mongodb_handler import get_mongodb_handler

class MockOrder:
    """Mock order for testing"""
    def __init__(self, order_id, symbol, order_type, quantity, direction, stop_price=None):
        self.id = order_id
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction
        self.stop_price = stop_price
        self.status = "Open"
        self.filled_price = None
        self.stop_order_id = None

class MockOrderManager(OrderManager):
    """Mock order manager that simulates real order management"""
    def __init__(self, api, account_id=None):
        super().__init__(api, account_id)
        self.mock_orders = {}
        self.next_order_id = 1000
        self.position_stops = {}  # Track stop orders for positions
        
    def submit_order(self, order):
        """Simulate order submission"""
        order_id = f"TEST-{self.next_order_id}"
        self.next_order_id += 1
        
        # Extract order details
        legs = order.get("legs", [])
        if legs:
            leg = legs[0]
            symbol = leg.get("symbol")
            quantity = leg.get("quantity")
            action = leg.get("action")
            
            # Create mock order
            mock_order = MockOrder(
                order_id=order_id,
                symbol=symbol,
                order_type=order.get("order-type", "Market"),
                quantity=quantity,
                direction=action,
                stop_price=order.get("stop-trigger-price")
            )
            
            self.mock_orders[order_id] = mock_order
            
            # Log order
            print(f"[ORDER] Submitted: {order_id} - {action} {quantity} {symbol}")
            
            # Simulate immediate fill for market orders
            if mock_order.order_type == "Market":
                mock_order.status = "Filled"
                mock_order.filled_price = 100.0  # Mock price
                
            return {
                "order": {
                    "id": order_id,
                    "status": mock_order.status
                }
            }
        
        return {"error": "Invalid order format"}
    
    def cancel_order(self, order_id):
        """Simulate order cancellation"""
        if order_id in self.mock_orders:
            self.mock_orders[order_id].status = "Canceled"
            print(f"[ORDER] Canceled: {order_id}")
            return True
        return False
    
    def _place_initial_stop_order(self, symbol, stop_price, direction, contract, quantity):
        """Override to track stop orders"""
        stop_order_id = f"STOP-{self.next_order_id}"
        self.next_order_id += 1
        
        # Create stop order
        stop_order = MockOrder(
            order_id=stop_order_id,
            symbol=contract,
            order_type="Stop",
            quantity=quantity,
            direction="Sell to Close" if direction == "bullish" else "Buy to Close",
            stop_price=stop_price
        )
        
        self.mock_orders[stop_order_id] = stop_order
        self.position_stops[symbol] = stop_order_id
        
        print(f"[STOP] Placed stop order {stop_order_id} at ${stop_price:.2f}")
        return stop_order_id
    
    def update_stop_order(self, symbol, new_stop_price):
        """Simulate stop order update"""
        if symbol in self.position_stops:
            old_stop_id = self.position_stops[symbol]
            
            # Cancel old stop
            self.cancel_order(old_stop_id)
            
            # Create new stop
            new_stop_id = f"STOP-{self.next_order_id}"
            self.next_order_id += 1
            
            stop_order = MockOrder(
                order_id=new_stop_id,
                symbol=symbol,
                order_type="Stop",
                quantity=1,
                direction="Sell to Close",
                stop_price=new_stop_price
            )
            
            self.mock_orders[new_stop_id] = stop_order
            self.position_stops[symbol] = new_stop_id
            
            print(f"[STOP] Updated stop for {symbol} to ${new_stop_price:.2f}")
            return new_stop_id
        
        return None
    
    def kill_all_orders(self):
        """Simulate kill switch"""
        canceled = 0
        closed = 0
        
        # Cancel all open orders
        for order_id, order in list(self.mock_orders.items()):
            if order.status == "Open":
                order.status = "Canceled"
                canceled += 1
        
        # Simulate closing all positions
        closed = len([o for o in self.mock_orders.values() 
                     if o.direction in ["Buy to Open", "Sell to Open"]])
        
        print(f"[KILL SWITCH] Canceled {canceled} orders, closed {closed} positions")
        
        return {
            "orders_canceled": canceled,
            "positions_closed": closed,
            "errors": []
        }

class EnhancedMarketDataGenerator(QThread):
    """Enhanced market data generator with stop loss triggers"""
    data_signal = pyqtSignal(str, dict)
    stop_trigger_signal = pyqtSignal(str, float)  # symbol, trigger_price
    
    def __init__(self):
        super().__init__()
        self.running = True
        self.base_prices = {
            # Sectors
            "XLK": 180.0,
            "XLF": 40.0,
            "XLV": 140.0,
            "XLY": 180.0,
            # Mag7
            "AAPL": 195.0,
            "MSFT": 430.0,
            "AMZN": 185.0,
            "NVDA": 140.0,
            "GOOG": 175.0,
            "TSLA": 250.0,
            "META": 520.0,
            # Trading symbols
            "SPY": 500.0,
            "QQQ": 480.0
        }
        self.trends = {}
        self.volatility = {}  # Track volatility for each symbol
        self.stop_levels = {}  # Track stop levels to trigger
        
    def set_stop_level(self, symbol, stop_price):
        """Set a stop level to monitor"""
        self.stop_levels[symbol] = stop_price
        
    def run(self):
        """Generate enhanced market data"""
        tick_count = 0
        
        while self.running:
            tick_count += 1
            
            # Periodically change market conditions
            if tick_count % 20 == 0:  # Every 10 seconds
                self.update_market_conditions()
            
            # Generate data for all symbols
            for symbol, base_price in self.base_prices.items():
                # Apply trend and volatility
                trend = self.trends.get(symbol, 0)
                volatility = self.volatility.get(symbol, 0.001)
                
                # Calculate new price with trend and noise
                new_price = base_price * (1 + trend)
                noise = random.uniform(-volatility, volatility)
                new_price = new_price * (1 + noise)
                
                # Occasionally create sharp moves (5% chance)
                if random.random() < 0.05:
                    spike = random.uniform(-0.02, 0.02)  # 2% spike
                    new_price = new_price * (1 + spike)
                    print(f"[MARKET] Price spike on {symbol}: {spike*100:.1f}%")
                
                # Check stop triggers
                if symbol in self.stop_levels:
                    stop_price = self.stop_levels[symbol]
                    
                    # For long positions, trigger if price falls below stop
                    if new_price <= stop_price and base_price > stop_price:
                        self.stop_trigger_signal.emit(symbol, new_price)
                        print(f"[STOP TRIGGER] {symbol} hit stop at ${new_price:.2f}")
                        del self.stop_levels[symbol]  # Remove after triggering
                
                # Update base price
                self.base_prices[symbol] = new_price
                
                # Create realistic quote
                spread = new_price * 0.0002
                quote = {
                    "symbol": symbol,
                    "bid": new_price - spread/2,
                    "ask": new_price + spread/2,
                    "bid_size": random.randint(100, 1000),
                    "ask_size": random.randint(100, 1000),
                    "timestamp": datetime.now().isoformat()
                }
                
                self.data_signal.emit("quote", quote)
                
                # Generate trades
                if random.random() < 0.3:
                    trade = {
                        "symbol": symbol,
                        "price": new_price,
                        "size": random.randint(100, 5000),
                        "volume": random.randint(100000, 1000000),
                        "timestamp": datetime.now().isoformat()
                    }
                    self.data_signal.emit("trade", trade)
                
                # Generate candles for compression detection
                if tick_count % 10 == 0:  # Every 5 seconds
                    self.generate_candle(symbol, new_price)
            
            # Check for compression setups
            if random.random() < 0.1:  # 10% chance
                self.generate_compression_setup()
            
            time.sleep(0.5)
    
    def update_market_conditions(self):
        """Update market conditions periodically"""
        # Decide overall market direction
        market_trend = random.choice(["bullish", "bearish", "neutral", "volatile"])
        
        print(f"[MARKET] Condition changed to: {market_trend.upper()}")
        
        for symbol in self.base_prices.keys():
            if market_trend == "bullish":
                self.trends[symbol] = random.uniform(0.0001, 0.0004)
                self.volatility[symbol] = random.uniform(0.0005, 0.001)
            elif market_trend == "bearish":
                self.trends[symbol] = random.uniform(-0.0004, -0.0001)
                self.volatility[symbol] = random.uniform(0.0005, 0.001)
            elif market_trend == "volatile":
                self.trends[symbol] = random.uniform(-0.0002, 0.0002)
                self.volatility[symbol] = random.uniform(0.001, 0.003)  # Higher volatility
            else:  # neutral
                self.trends[symbol] = random.uniform(-0.0001, 0.0001)
                self.volatility[symbol] = random.uniform(0.0002, 0.0005)
    
    def generate_candle(self, symbol, current_price):
        """Generate candle data"""
        # Simple OHLC generation
        open_price = current_price * (1 + random.uniform(-0.001, 0.001))
        high = max(open_price, current_price) * (1 + random.uniform(0, 0.002))
        low = min(open_price, current_price) * (1 - random.uniform(0, 0.002))
        
        candle = {
            "symbol": symbol,
            "period": "5m",
            "open": open_price,
            "high": high,
            "low": low,
            "close": current_price,
            "volume": random.randint(10000, 100000),
            "timestamp": datetime.now().isoformat()
        }
        
        self.data_signal.emit("candle", candle)
    
    def generate_compression_setup(self):
        """Generate compression setup for testing"""
        # Pick a trading symbol
        symbol = random.choice(["SPY", "QQQ"])
        base_price = self.base_prices[symbol]
        
        print(f"[COMPRESSION] Generating setup for {symbol}")
        
        # Generate tight range candles
        for i in range(5):
            # Very tight range to simulate compression
            open_price = base_price * (1 + random.uniform(-0.0002, 0.0002))
            high = open_price * 1.0003
            low = open_price * 0.9997
            close = base_price * (1 + random.uniform(-0.0002, 0.0002))
            
            candle = {
                "symbol": symbol,
                "period": "1m",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": random.randint(5000, 10000),  # Lower volume
                "timestamp": datetime.now().isoformat()
            }
            
            self.data_signal.emit("candle", candle)
            time.sleep(0.1)

class EnhancedTestDashboard(QWidget):
    """Enhanced dashboard with all features"""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.trade_count = 0
        self.active_positions = {}
        self.position_widgets = {}  # Store position row widgets
        
    def init_ui(self):
        layout = QVBoxLayout()
        
        # Title
        title = QLabel("Jigsaw Flow Complete System Test")
        title.setStyleSheet("font-size: 20px; font-weight: bold; padding: 10px;")
        layout.addWidget(title)
        
        # Create tab widget
        self.tabs = QTabWidget()
        
        # Main Control Tab
        self.control_tab = self.create_control_tab()
        self.tabs.addTab(self.control_tab, "Control Panel")
        
        # Positions Tab
        self.positions_tab = self.create_positions_tab()
        self.tabs.addTab(self.positions_tab, "Positions")
        
        # Orders Tab
        self.orders_tab = self.create_orders_tab()
        self.tabs.addTab(self.orders_tab, "Orders")
        
        # Market Data Tab
        self.market_tab = self.create_market_tab()
        self.tabs.addTab(self.market_tab, "Market Data")
        
        # Logs Tab
        self.logs_tab = self.create_logs_tab()
        self.tabs.addTab(self.logs_tab, "Activity Log")
        
        layout.addWidget(self.tabs)
        
        # Status Bar
        self.status_bar = QStatusBar()
        self.status_bar.showMessage("System Ready")
        layout.addWidget(self.status_bar)
        
        self.setLayout(layout)
        self.setWindowTitle("Jigsaw Flow Complete System Test")
        self.resize(1000, 800)
        
        # Apply dark theme
        self.apply_dark_theme()
    
    def create_control_tab(self):
        """Create main control panel tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Strategy Configuration
        strategy_group = QGroupBox("Strategy Configuration")
        strategy_layout = QGridLayout()
        
        # Strategy Selection
        self.sector_radio = QRadioButton("Sector Confirmation")
        self.mag7_radio = QRadioButton("Mag7 Confirmation")
        self.sector_radio.setChecked(True)
        
        strategy_layout.addWidget(self.sector_radio, 0, 0)
        strategy_layout.addWidget(self.mag7_radio, 0, 1)
        
        # Sector selection checkboxes
        self.sector_checks = {}
        sector_label = QLabel("Select Sectors:")
        strategy_layout.addWidget(sector_label, 1, 0)
        
        sectors = ["XLK (Tech)", "XLF (Finance)", "XLV (Health)", "XLY (Consumer)"]
        for i, sector in enumerate(sectors):
            check = QCheckBox(sector)
            check.setChecked(True)
            self.sector_checks[sector.split()[0]] = check
            strategy_layout.addWidget(check, 1, i+1)
        
        # Threshold
        strategy_layout.addWidget(QLabel("Threshold:"), 2, 0)
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(0, 100)
        self.threshold_spin.setValue(43)
        self.threshold_spin.setSuffix("%")
        strategy_layout.addWidget(self.threshold_spin, 2, 1)
        
        # Trading Parameters
        strategy_layout.addWidget(QLabel("Stop Loss Method:"), 3, 0)
        self.stop_method_combo = QComboBox()
        self.stop_method_combo.addItems([
            "ATR Multiple",
            "Fixed Percentage",
            "Structure-based"
        ])
        strategy_layout.addWidget(self.stop_method_combo, 3, 1)
        
        # Trailing Stop Method
        strategy_layout.addWidget(QLabel("Trailing Stop:"), 3, 2)
        self.trail_method_combo = QComboBox()
        self.trail_method_combo.addItems([
            "Heiken Ashi Candle Trail",
            "EMA Trail",
            "% Price Trail",
            "ATR-Based Trail",
            "Fixed Tick Trail"
        ])
        strategy_layout.addWidget(self.trail_method_combo, 3, 3)
        
        strategy_group.setLayout(strategy_layout)
        layout.addWidget(strategy_group)
        
        # Control Buttons
        controls_group = QGroupBox("System Controls")
        controls_layout = QGridLayout()
        
        # Main controls
        self.start_btn = QPushButton("Start System")
        self.stop_btn = QPushButton("Stop System")
        self.pause_btn = QPushButton("Pause")
        self.resume_btn = QPushButton("Resume")
        
        self.start_btn.setStyleSheet("background-color: #27ae60;")
        self.stop_btn.setStyleSheet("background-color: #e74c3c;")
        self.stop_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.resume_btn.setEnabled(False)
        
        controls_layout.addWidget(self.start_btn, 0, 0)
        controls_layout.addWidget(self.stop_btn, 0, 1)
        controls_layout.addWidget(self.pause_btn, 0, 2)
        controls_layout.addWidget(self.resume_btn, 0, 3)
        
        # Test controls
        self.test_trade_btn = QPushButton("Force Test Trade")
        self.test_compression_btn = QPushButton("Test Compression")
        self.test_stop_btn = QPushButton("Test Stop Hit")
        self.kill_switch_btn = QPushButton("KILL SWITCH")
        
        self.kill_switch_btn.setStyleSheet("background-color: #c0392b; font-weight: bold;")
        
        controls_layout.addWidget(self.test_trade_btn, 1, 0)
        controls_layout.addWidget(self.test_compression_btn, 1, 1)
        controls_layout.addWidget(self.test_stop_btn, 1, 2)
        controls_layout.addWidget(self.kill_switch_btn, 1, 3)
        
        # Database controls
        self.clear_db_btn = QPushButton("Clear Database")
        self.export_btn = QPushButton("Export Positions")
        self.recover_btn = QPushButton("Recover Positions")
        
        controls_layout.addWidget(self.clear_db_btn, 2, 0)
        controls_layout.addWidget(self.export_btn, 2, 1)
        controls_layout.addWidget(self.recover_btn, 2, 2)
        
        controls_group.setLayout(controls_layout)
        layout.addWidget(controls_group)
        
        # Statistics
        stats_group = QGroupBox("Statistics")
        stats_layout = QGridLayout()
        
        self.stats_labels = {
            "total_trades": QLabel("Total Trades: 0"),
            "open_positions": QLabel("Open Positions: 0"),
            "wins": QLabel("Wins: 0"),
            "losses": QLabel("Losses: 0"),
            "win_rate": QLabel("Win Rate: 0%"),
            "total_pnl": QLabel("Total P&L: $0.00"),
            "db_status": QLabel("MongoDB: Checking..."),
            "strategy_status": QLabel("Strategy: Idle"),
            "market_status": QLabel("Market: Neutral"),
            "last_signal": QLabel("Last Signal: None")
        }
        
        row, col = 0, 0
        for key, label in self.stats_labels.items():
            stats_layout.addWidget(label, row, col)
            col += 1
            if col > 3:
                col = 0
                row += 1
        
        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)
        
        # Real-time indicators
        indicators_group = QGroupBox("Market Indicators")
        indicators_layout = QHBoxLayout()
        
        self.sector_indicators = {}
        for sector in ["XLK", "XLF", "XLV", "XLY"]:
            indicator = QLabel(sector)
            indicator.setAlignment(Qt.AlignCenter)
            indicator.setStyleSheet("""
                QLabel {
                    background-color: #555;
                    border-radius: 5px;
                    padding: 10px;
                    min-width: 80px;
                }
            """)
            self.sector_indicators[sector] = indicator
            indicators_layout.addWidget(indicator)
        
        indicators_group.setLayout(indicators_layout)
        layout.addWidget(indicators_group)
        
        layout.addStretch()
        widget.setLayout(layout)
        return widget
    
    def create_positions_tab(self):
        """Create positions management tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Positions table
        self.positions_table = QTableWidget()
        self.positions_table.setColumnCount(10)
        self.positions_table.setHorizontalHeaderLabels([
            "Symbol", "Type", "Qty", "Entry", "Current", "P&L", "P&L %", 
            "Stop", "Status", "Actions"
        ])
        
        # Set column widths
        self.positions_table.setColumnWidth(9, 150)  # Actions column
        
        layout.addWidget(self.positions_table)
        
        # Position summary
        summary_layout = QHBoxLayout()
        self.position_summary = QLabel("Total P&L: $0.00 | Open: 0 | Closed: 0")
        summary_layout.addWidget(self.position_summary)
        summary_layout.addStretch()
        
        layout.addLayout(summary_layout)
        
        widget.setLayout(layout)
        return widget
    
    def create_orders_tab(self):
        """Create orders management tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Orders table
        self.orders_table = QTableWidget()
        self.orders_table.setColumnCount(8)
        self.orders_table.setHorizontalHeaderLabels([
            "Order ID", "Symbol", "Type", "Direction", "Qty", "Price", 
            "Status", "Actions"
        ])
        
        # Set column widths
        self.orders_table.setColumnWidth(7, 150)  # Actions column
        
        layout.addWidget(self.orders_table)
        
        # Order summary
        summary_layout = QHBoxLayout()
        self.order_summary = QLabel("Open Orders: 0 | Filled: 0 | Canceled: 0")
        summary_layout.addWidget(self.order_summary)
        summary_layout.addStretch()
        
        layout.addLayout(summary_layout)
        
        widget.setLayout(layout)
        return widget
    
    def create_market_tab(self):
        """Create market data tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Market data table
        self.market_table = QTableWidget()
        self.market_table.setColumnCount(8)
        self.market_table.setHorizontalHeaderLabels([
            "Symbol", "Bid", "Ask", "Last", "Volume", "Change", "Trend", "Signal"
        ])
        
        layout.addWidget(self.market_table)
        
        widget.setLayout(layout)
        return widget
    
    def create_logs_tab(self):
        """Create activity logs tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Log text area
        self.activity_log = QTextEdit()
        self.activity_log.setReadOnly(True)
        
        layout.addWidget(self.activity_log)
        
        # Log controls
        controls_layout = QHBoxLayout()
        self.clear_log_btn = QPushButton("Clear Log")
        self.save_log_btn = QPushButton("Save Log")
        
        controls_layout.addWidget(self.clear_log_btn)
        controls_layout.addWidget(self.save_log_btn)
        controls_layout.addStretch()
        
        layout.addLayout(controls_layout)
        
        widget.setLayout(layout)
        return widget
    
    def apply_dark_theme(self):
        """Apply dark theme to dashboard"""
        self.setStyleSheet("""
            QWidget {
                background-color: #2b2b2b;
                color: #ffffff;
            }
            QGroupBox {
                border: 1px solid #555;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
                font-weight: bold;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QTableWidget {
                background-color: #1e1e1e;
                gridline-color: #555;
                selection-background-color: #3498db;
            }
            QTextEdit {
                background-color: #1e1e1e;
                border: 1px solid #555;
            }
            QPushButton {
                padding: 8px 15px;
                border-radius: 3px;
                border: none;
                font-weight: bold;
            }
            QPushButton:hover {
                opacity: 0.8;
            }
            QPushButton:disabled {
                background-color: #555;
                color: #999;
            }
            QTabWidget::pane {
                border: 1px solid #555;
            }
            QTabBar::tab {
                background-color: #3c3c3c;
                padding: 8px 20px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background-color: #555;
            }
            QStatusBar {
                background-color: #1e1e1e;
                border-top: 1px solid #555;
            }
        """)
    
    def log_activity(self, message, level="INFO"):
        """Add timestamped message to activity log"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        # Color code by level
        color = "#ffffff"  # Default white
        if level == "ERROR":
            color = "#e74c3c"
        elif level == "WARNING":
            color = "#f39c12"
        elif level == "SUCCESS":
            color = "#27ae60"
        elif level == "TRADE":
            color = "#3498db"
        
        formatted_message = f'<span style="color: #999">[{timestamp}]</span> <span style="color: {color}">{message}</span>'
        self.activity_log.append(formatted_message)
        
        # Auto-scroll
        scrollbar = self.activity_log.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
        # Update status bar
        self.status_bar.showMessage(message, 3000)  # Show for 3 seconds
    
    def add_position_with_controls(self, position):
        """Add position with exit and modify controls"""
        row = self.positions_table.rowCount()
        self.positions_table.insertRow(row)
        
        # Basic position data
        items = [
            QTableWidgetItem(position["symbol"]),
            QTableWidgetItem(position["type"]),
            QTableWidgetItem(str(position.get("quantity", 1))),
            QTableWidgetItem(f"${position['entry_price']:.2f}"),
            QTableWidgetItem(f"${position['current_price']:.2f}"),
            QTableWidgetItem(position["pl"]),
            QTableWidgetItem("0.0%"),
            QTableWidgetItem(f"${position['stop']:.2f}"),
            QTableWidgetItem("Open")
        ]
        
        for col, item in enumerate(items):
            self.positions_table.setItem(row, col, item)
        
        # Create action buttons
        actions_widget = QWidget()
        actions_layout = QHBoxLayout()
        actions_layout.setContentsMargins(5, 0, 5, 0)
        
        # Exit button
        exit_btn = QPushButton("Exit")
        exit_btn.setFixedSize(50, 25)
        exit_btn.setStyleSheet("background-color: #e74c3c;")
        exit_btn.clicked.connect(lambda: self.exit_position(position["symbol"]))
        
        # Modify stop button
        modify_btn = QPushButton("Modify")
        modify_btn.setFixedSize(50, 25)
        modify_btn.setStyleSheet("background-color: #3498db;")
        modify_btn.clicked.connect(lambda: self.modify_stop(position["symbol"]))
        
        actions_layout.addWidget(exit_btn)
        actions_layout.addWidget(modify_btn)
        
        actions_widget.setLayout(actions_layout)
        self.positions_table.setCellWidget(row, 9, actions_widget)
        
        # Store position reference
        self.active_positions[position["symbol"]] = row
        self.position_widgets[position["symbol"]] = {
            "row": row,
            "data": position
        }
    
    def add_order(self, order):
        """Add order to orders table"""
        row = self.orders_table.rowCount()
        self.orders_table.insertRow(row)
        
        # Order data
        items = [
            QTableWidgetItem(order.id),
            QTableWidgetItem(order.symbol),
            QTableWidgetItem(order.order_type),
            QTableWidgetItem(order.direction),
            QTableWidgetItem(str(order.quantity)),
            QTableWidgetItem(f"${order.stop_price:.2f}" if order.stop_price else "Market"),
            QTableWidgetItem(order.status)
        ]
        
        for col, item in enumerate(items):
            self.orders_table.setItem(row, col, item)
        
        # Action button
        if order.status == "Open":
            cancel_btn = QPushButton("Cancel")
            cancel_btn.setFixedSize(60, 25)
            cancel_btn.setStyleSheet("background-color: #e74c3c;")
            cancel_btn.clicked.connect(lambda: self.cancel_order(order.id))
            self.orders_table.setCellWidget(row, 7, cancel_btn)
    
    def update_position(self, symbol, updates):
        """Update position in table"""
        if symbol in self.active_positions:
            row = self.active_positions[symbol]
            
            if "current_price" in updates:
                self.positions_table.setItem(row, 4, QTableWidgetItem(f"${updates['current_price']:.2f}"))
            
            if "pl" in updates:
                pl_item = QTableWidgetItem(updates["pl"])
                # Color based on P&L
                if "$-" in updates["pl"] or "(-" in updates["pl"]:
                    pl_item.setForeground(QColor("#e74c3c"))
                else:
                    pl_item.setForeground(QColor("#27ae60"))
                self.positions_table.setItem(row, 5, pl_item)
            
            if "pl_pct" in updates:
                pct_item = QTableWidgetItem(f"{updates['pl_pct']:.1f}%")
                if updates['pl_pct'] < 0:
                    pct_item.setForeground(QColor("#e74c3c"))
                else:
                    pct_item.setForeground(QColor("#27ae60"))
                self.positions_table.setItem(row, 6, pct_item)
            
            if "stop" in updates:
                self.positions_table.setItem(row, 7, QTableWidgetItem(f"${updates['stop']:.2f}"))
            
            if "status" in updates:
                self.positions_table.setItem(row, 8, QTableWidgetItem(updates["status"]))
    
    def update_sector_indicator(self, sector, status, change_pct=0):
        """Update sector indicator color based on status"""
        if sector in self.sector_indicators:
            indicator = self.sector_indicators[sector]
            
            # Set color based on status
            if status == "bullish":
                color = "#27ae60"  # Green
            elif status == "bearish":
                color = "#e74c3c"  # Red
            else:
                color = "#555"  # Gray
            
            # Update style and text
            indicator.setStyleSheet(f"""
                QLabel {{
                    background-color: {color};
                    border-radius: 5px;
                    padding: 10px;
                    min-width: 80px;
                    font-weight: bold;
                }}
            """)
            
            indicator.setText(f"{sector}\n{change_pct:+.1f}%")
    
    def exit_position(self, symbol):
        """Handle position exit button click"""
        self.log_activity(f"Manual exit requested for {symbol}", "TRADE")
    
    def modify_stop(self, symbol):
        """Handle modify stop button click"""
        # Simple dialog to modify stop
        current_stop = 95.0  # Get from position data
        new_stop, ok = QInputDialog.getDouble(
            self, 
            "Modify Stop Loss", 
            f"Enter new stop price for {symbol}:",
            current_stop,
            0,
            10000,
            2
        )
        
        if ok:
            self.log_activity(f"Stop modified for {symbol}: ${new_stop:.2f}", "TRADE")
    
    def cancel_order(self, order_id):
        """Handle cancel order button click"""
        self.log_activity(f"Order {order_id} cancellation requested", "WARNING")

class CompleteSystemTest(QObject):
    """Complete system test controller"""
    
    def __init__(self):
        super().__init__()
        self.dashboard = EnhancedTestDashboard()
        self.market_generator = EnhancedMarketDataGenerator()
        
        # Core components
        self.config = None
        self.api = None
        self.strategy = None
        self.order_manager = None
        self.position_manager = None
        self.db = None
        
        # State
        self.running = False
        self.paused = False
        
        # Statistics
        self.stats = {
            "total_trades": 0,
            "open_positions": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": 0.0
        }
        
        # Connect all signals
        self.connect_signals()
    
    def connect_signals(self):
        """Connect all dashboard signals"""
        # Control buttons
        self.dashboard.start_btn.clicked.connect(self.start_test)
        self.dashboard.stop_btn.clicked.connect(self.stop_test)
        self.dashboard.pause_btn.clicked.connect(self.pause_test)
        self.dashboard.resume_btn.clicked.connect(self.resume_test)
        
        # Test buttons
        self.dashboard.test_trade_btn.clicked.connect(self.force_test_trade)
        self.dashboard.test_compression_btn.clicked.connect(self.test_compression)
        self.dashboard.test_stop_btn.clicked.connect(self.test_stop_loss)
        self.dashboard.kill_switch_btn.clicked.connect(self.test_kill_switch)
        
        # Database buttons
        self.dashboard.clear_db_btn.clicked.connect(self.clear_database)
        self.dashboard.export_btn.clicked.connect(self.export_positions)
        self.dashboard.recover_btn.clicked.connect(self.recover_positions)
        
        # Strategy controls
        self.dashboard.sector_radio.toggled.connect(self.update_strategy_config)
        self.dashboard.mag7_radio.toggled.connect(self.update_strategy_config)
        
        # Market data
        self.market_generator.data_signal.connect(self.handle_market_data)
        self.market_generator.stop_trigger_signal.connect(self.handle_stop_trigger)
        
        # Log controls
        self.dashboard.clear_log_btn.clicked.connect(self.dashboard.activity_log.clear)
    
    def start_test(self):
        """Start complete system test"""
        self.running = True
        self.dashboard.log_activity("Starting complete system test...", "INFO")
        
        # Update UI
        self.dashboard.start_btn.setEnabled(False)
        self.dashboard.stop_btn.setEnabled(True)
        self.dashboard.pause_btn.setEnabled(True)
        
        try:
            # Initialize configuration
            self.init_config()
            
            # Initialize MongoDB
            self.dashboard.log_activity("Connecting to MongoDB...")
            self.db = get_mongodb_handler()
            self.dashboard.stats_labels["db_status"].setText("MongoDB: Connected")
            
            # Initialize API
            self.dashboard.log_activity("Initializing API in test mode...")
            self.api = TastyTradeAPI("test_user", "test_pass")
            
            # Initialize components
            self.dashboard.log_activity("Initializing trading components...")
            
            # Mock market data client
            class MockMarketDataClient:
                def __init__(self, test_controller):
                    self.test = test_controller
                    self.candle_builder = None
                    
                def subscribe_to_sector_etfs(self):
                    self.test.dashboard.log_activity("Subscribed to sector ETFs")
                    return 1
                    
                def subscribe(self, symbols, event_types):
                    self.test.dashboard.log_activity(f"Subscribed to {symbols}")
                    return 2
                
                def subscribe_to_mag7_stocks(self, stocks):
                    self.test.dashboard.log_activity(f"Subscribed to Mag7: {stocks}")
                    return 3
            
            self.market_client = MockMarketDataClient(self)
            
            # Initialize managers
            self.instrument_fetcher = InstrumentFetcher(self.api)
            self.order_manager = MockOrderManager(self.api)
            self.position_manager = PositionManager()
            
            # Initialize strategy
            self.strategy = JigsawStrategy(
                instrument_fetcher=self.instrument_fetcher,
                market_data_client=self.market_client,
                order_manager=self.order_manager,
                config=self.config
            )
            
            # Override strategy methods
            self.override_strategy_methods()
            
            # Initialize strategy
            self.strategy.initialize()
            
            # Start market data
            self.market_generator.start()
            self.dashboard.log_activity("Market data generation started", "SUCCESS")
            
            # Start monitoring
            self.start_monitoring()
            
            self.dashboard.stats_labels["strategy_status"].setText("Strategy: Active")
            self.dashboard.log_activity("System test started successfully", "SUCCESS")
            
        except Exception as e:
            self.dashboard.log_activity(f"Error starting test: {str(e)}", "ERROR")
            self.stop_test()
    
    def init_config(self):
        """Initialize configuration from UI settings"""
        # Get selected sectors
        selected_sectors = []
        for sector, check in self.dashboard.sector_checks.items():
            if check.isChecked():
                selected_sectors.append(sector)
        
        self.config = {
            "broker": {
                "username": "test_user",
                "password": "test_pass",
                "account_id": "TEST123",
                "auto_trading_enabled": True
            },
            "trading_config": {
                "tickers": ["SPY", "QQQ"],
                "contracts_per_trade": 1,
                "use_mag7_confirmation": self.dashboard.mag7_radio.isChecked(),
                "sector_weight_threshold": self.dashboard.threshold_spin.value(),
                "mag7_threshold": self.dashboard.threshold_spin.value(),
                "selected_sectors": selected_sectors,
                "stop_loss_method": self.dashboard.stop_method_combo.currentText(),
                "trailing_stop_method": self.dashboard.trail_method_combo.currentText(),
                "bb_width_threshold": 0.05,
                "donchian_contraction_threshold": 0.6,
                "volume_squeeze_threshold": 0.3,
                "sector_weights": {
                    "XLK": 32,
                    "XLF": 14,
                    "XLV": 11,
                    "XLY": 11
                },
                "mag7_stocks": ["AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "TSLA", "META"],
                "sector_etfs": ["XLK", "XLF", "XLV", "XLY"]
            }
        }
    
    def override_strategy_methods(self):
        """Override strategy methods for testing"""
        original_enter = self.strategy.enter_trade
        original_exit = self.strategy.exit_trade
        original_update_stop = self.strategy._update_trailing_stop
        
        def mock_enter_trade(symbol, direction):
            self.dashboard.log_activity(f"TRADE ENTRY: {direction.upper()} on {symbol}", "TRADE")
            
            # Create position
            position = {
                "symbol": symbol,
                "type": "Long" if direction == "bullish" else "Short",
                "quantity": 1,
                "entry_price": self.market_generator.base_prices.get(symbol, 100),
                "current_price": self.market_generator.base_prices.get(symbol, 100),
                "pl": "$0.00 (0.0%)",
                "stop": self.market_generator.base_prices.get(symbol, 100) * 0.98,
                "entry_time": datetime.now().isoformat(),
                "option_symbol": f"{symbol} TEST OPTION",
                "status": "Open"
            }
            
            # Add to dashboard
            self.dashboard.add_position_with_controls(position)
            
            # Add to position manager
            self.position_manager.add_position(symbol, position)
            
            # Update stats
            self.stats["total_trades"] += 1
            self.stats["open_positions"] += 1
            self.update_statistics()
            
            # Set stop level in market generator
            self.market_generator.set_stop_level(symbol, position["stop"])
            
            # Add order to orders table
            order = MockOrder(
                f"ENTRY-{self.stats['total_trades']}",
                symbol,
                "Market",
                1,
                "Buy to Open" if direction == "bullish" else "Sell to Open"
            )
            order.status = "Filled"
            self.dashboard.add_order(order)
            
            # Call original
            original_enter(symbol, direction)
        
        def mock_exit_trade(symbol, reason="Manual exit"):
            self.dashboard.log_activity(f"TRADE EXIT: {symbol} - {reason}", "TRADE")
            
            # Get position
            position_data = self.position_manager.get_position(symbol)
            
            if position_data:
                # Calculate final P&L
                entry_price = position_data.get("entry_price", 100)
                exit_price = self.market_generator.base_prices.get(symbol, 100)
                
                if position_data["type"] == "Long":
                    pnl = (exit_price - entry_price) * 100  # 1 contract = 100 shares
                    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                else:
                    pnl = (entry_price - exit_price) * 100
                    pnl_pct = ((entry_price - exit_price) / entry_price) * 100
                
                # Update stats
                if pnl > 0:
                    self.stats["wins"] += 1
                else:
                    self.stats["losses"] += 1
                
                self.stats["total_pnl"] += pnl
                self.stats["open_positions"] -= 1
                
                # Close in position manager
                self.position_manager.close_position(symbol, {
                    "exit_price": exit_price,
                    "exit_reason": reason,
                    "pnl": pnl,
                    "pnl_percent": pnl_pct
                })
                
                # Update position status
                self.dashboard.update_position(symbol, {"status": "Closed"})
                
                # Remove from positions table after delay
                QTimer.singleShot(2000, lambda: self.dashboard.positions_table.removeRow(
                    self.dashboard.active_positions.get(symbol, -1)
                ))
                
                self.update_statistics()
            
            # Call original
            return original_exit(symbol, reason)
        
        def mock_update_trailing_stop(symbol):
            current_price = self.market_generator.base_prices.get(symbol, 100)
            new_stop = current_price * 0.98  # Simple 2% trailing stop
            
            self.dashboard.log_activity(f"Trailing stop updated for {symbol}: ${new_stop:.2f}")
            self.dashboard.update_position(symbol, {"stop": new_stop})
            
            # Update stop in market generator
            self.market_generator.set_stop_level(symbol, new_stop)
            
            # Update in order manager
            self.order_manager.update_stop_order(symbol, new_stop)
            
            # Call original
            original_update_stop(symbol)
        
        # Apply overrides
        self.strategy.enter_trade = mock_enter_trade
        self.strategy.exit_trade = mock_exit_trade
        self.strategy._update_trailing_stop = mock_update_trailing_stop
    
    def handle_market_data(self, data_type, data):
        """Handle market data and feed to strategy"""
        if not self.running or self.paused:
            return
        
        symbol = data.get("symbol")
        
        # Update market table
        if data_type in ["quote", "trade"]:
            self.update_market_display(symbol, data)
        
        # Feed to strategy
        if symbol in ["XLK", "XLF", "XLV", "XLY"]:
            # Sector update
            price = data.get("bid", data.get("price", 0))
            if price > 0 and symbol in self.market_generator.trends:
                trend = self.market_generator.trends[symbol]
                status = "bullish" if trend > 0.0001 else "bearish" if trend < -0.0001 else "neutral"
                change_pct = trend * 100
                
                self.strategy.update_sector_status(symbol, status, price)
                self.dashboard.update_sector_indicator(symbol, status, change_pct)
        
        elif symbol in self.config["trading_config"]["mag7_stocks"]:
            # Mag7 update
            price = data.get("bid", data.get("price", 0))
            if price > 0 and hasattr(self.strategy, 'mag7_strategy') and self.strategy.mag7_strategy:
                self.strategy.mag7_strategy.update_mag7_status(symbol, price)
        
        # Feed candle data for compression detection
        if data_type == "candle":
            # Process candle for compression detection
            pass
    
    def handle_stop_trigger(self, symbol, trigger_price):
        """Handle stop loss trigger"""
        self.dashboard.log_activity(f"STOP LOSS TRIGGERED: {symbol} at ${trigger_price:.2f}", "WARNING")
        
        # Exit the trade
        if symbol in self.strategy.active_trades:
            self.strategy.exit_trade(symbol, f"Stop loss hit at ${trigger_price:.2f}")
    
    def update_market_display(self, symbol, data):
        """Update market data display"""
        # Find or create row
        row = -1
        for i in range(self.dashboard.market_table.rowCount()):
            if self.dashboard.market_table.item(i, 0) and \
               self.dashboard.market_table.item(i, 0).text() == symbol:
                row = i
                break
        
        if row == -1:
            row = self.dashboard.market_table.rowCount()
            self.dashboard.market_table.insertRow(row)
            self.dashboard.market_table.setItem(row, 0, QTableWidgetItem(symbol))
        
        # Update data
        if "bid" in data:
            self.dashboard.market_table.setItem(row, 1, QTableWidgetItem(f"${data['bid']:.2f}"))
            self.dashboard.market_table.setItem(row, 2, QTableWidgetItem(f"${data['ask']:.2f}"))
        
        if "price" in data:
            self.dashboard.market_table.setItem(row, 3, QTableWidgetItem(f"${data['price']:.2f}"))
        
        if "volume" in data:
            self.dashboard.market_table.setItem(row, 4, QTableWidgetItem(f"{data['volume']:,}"))
    
    def start_monitoring(self):
        """Start strategy monitoring thread"""
        def monitor():
            while self.running:
                if not self.paused:
                    try:
                        # Check for trade setups
                        self.strategy.check_for_trade_setups()
                        
                        # Manage active trades
                        self.strategy.manage_active_trades()
                        
                        # Update position P&L
                        for symbol in list(self.dashboard.active_positions.keys()):
                            if symbol in self.market_generator.base_prices:
                                position = self.position_manager.get_position(symbol)
                                if position:
                                    current_price = self.market_generator.base_prices[symbol]
                                    entry_price = position.get("entry_price", 100)
                                    
                                    # Calculate P&L
                                    if position["type"] == "Long":
                                        pnl = (current_price - entry_price) * 100
                                        pnl_pct = ((current_price - entry_price) / entry_price) * 100
                                    else:
                                        pnl = (entry_price - current_price) * 100
                                        pnl_pct = ((entry_price - current_price) / entry_price) * 100
                                    
                                    # Update display
                                    self.dashboard.update_position(symbol, {
                                        "current_price": current_price,
                                        "pl": f"${pnl:+.2f}",
                                        "pl_pct": pnl_pct
                                    })
                        
                        # Update market status
                        aligned, direction, weight = self.strategy.detect_sector_alignment()
                        if aligned:
                            self.dashboard.stats_labels["market_status"].setText(
                                f"Market: {direction.upper()} ({weight}%)"
                            )
                        else:
                            self.dashboard.stats_labels["market_status"].setText("Market: Neutral")
                        
                    except Exception as e:
                        self.dashboard.log_activity(f"Monitoring error: {str(e)}", "ERROR")
                
                time.sleep(0.5)
        
        self.monitor_thread = threading.Thread(target=monitor)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def update_statistics(self):
        """Update statistics display"""
        # Calculate win rate
        total_closed = self.stats["wins"] + self.stats["losses"]
        win_rate = (self.stats["wins"] / total_closed * 100) if total_closed > 0 else 0
        
        # Update labels
        self.dashboard.stats_labels["total_trades"].setText(f"Total Trades: {self.stats['total_trades']}")
        self.dashboard.stats_labels["open_positions"].setText(f"Open Positions: {self.stats['open_positions']}")
        self.dashboard.stats_labels["wins"].setText(f"Wins: {self.stats['wins']}")
        self.dashboard.stats_labels["losses"].setText(f"Losses: {self.stats['losses']}")
        self.dashboard.stats_labels["win_rate"].setText(f"Win Rate: {win_rate:.1f}%")
        self.dashboard.stats_labels["total_pnl"].setText(f"Total P&L: ${self.stats['total_pnl']:+.2f}")
        
        # Update position summary
        self.dashboard.position_summary.setText(
            f"Total P&L: ${self.stats['total_pnl']:+.2f} | "
            f"Open: {self.stats['open_positions']} | "
            f"Closed: {total_closed}"
        )
    
    def pause_test(self):
        """Pause the test"""
        self.paused = True
        self.dashboard.pause_btn.setEnabled(False)
        self.dashboard.resume_btn.setEnabled(True)
        self.dashboard.log_activity("System test paused", "WARNING")
        self.dashboard.stats_labels["strategy_status"].setText("Strategy: Paused")
    
    def resume_test(self):
        """Resume the test"""
        self.paused = False
        self.dashboard.pause_btn.setEnabled(True)
        self.dashboard.resume_btn.setEnabled(False)
        self.dashboard.log_activity("System test resumed", "SUCCESS")
        self.dashboard.stats_labels["strategy_status"].setText("Strategy: Active")
    
    def stop_test(self):
        """Stop the test"""
        self.running = False
        self.market_generator.stop()
        
        self.dashboard.start_btn.setEnabled(True)
        self.dashboard.stop_btn.setEnabled(False)
        self.dashboard.pause_btn.setEnabled(False)
        self.dashboard.resume_btn.setEnabled(False)
        
        self.dashboard.log_activity("System test stopped", "WARNING")
        self.dashboard.stats_labels["strategy_status"].setText("Strategy: Stopped")
    
    def force_test_trade(self):
        """Force a test trade"""
        if self.strategy:
            symbol = random.choice(["SPY", "QQQ"])
            direction = random.choice(["bullish", "bearish"])
            
            self.dashboard.log_activity(f"Forcing test trade: {direction} on {symbol}", "INFO")
            self.strategy.enter_trade(symbol, direction)
            self.dashboard.stats_labels["last_signal"].setText(f"Last Signal: {direction.upper()} {symbol}")
    
    def test_compression(self):
        """Test compression detection"""
        self.dashboard.log_activity("Generating compression setup...", "INFO")
        self.market_generator.generate_compression_setup()
    
    def test_stop_loss(self):
        """Test stop loss trigger"""
        # Find an open position
        for symbol in self.dashboard.active_positions.keys():
            # Force price to hit stop
            if symbol in self.market_generator.stop_levels:
                stop_price = self.market_generator.stop_levels[symbol]
                self.market_generator.base_prices[symbol] = stop_price * 0.99
                self.dashboard.log_activity(f"Forcing stop loss test for {symbol}", "WARNING")
                break
    
    def test_kill_switch(self):
        """Test kill switch functionality"""
        self.dashboard.log_activity("KILL SWITCH ACTIVATED!", "ERROR")
        
        if self.order_manager:
            result = self.order_manager.kill_all_orders()
            
            self.dashboard.log_activity(
                f"Kill switch results: {result['orders_canceled']} orders canceled, "
                f"{result['positions_closed']} positions closed", 
                "WARNING"
            )
            
            # Exit all positions
            for symbol in list(self.strategy.active_trades.keys()):
                self.strategy.exit_trade(symbol, "Kill switch activated")
    
    def update_strategy_config(self):
        """Update strategy configuration"""
        if self.strategy:
            self.init_config()
            self.strategy.config = self.config
            self.strategy.trading_config = self.config["trading_config"]
            
            if self.dashboard.mag7_radio.isChecked():
                self.dashboard.log_activity(
                    f"Switched to Mag7 strategy ({self.dashboard.threshold_spin.value()}% threshold)", 
                    "INFO"
                )
            else:
                selected = [s for s, c in self.dashboard.sector_checks.items() if c.isChecked()]
                self.dashboard.log_activity(
                    f"Switched to Sector strategy (sectors: {', '.join(selected)}, "
                    f"{self.dashboard.threshold_spin.value()}% threshold)", 
                    "INFO"
                )
    
    def clear_database(self):
        """Clear test database"""
        reply = QMessageBox.question(
            self.dashboard,
            "Clear Database",
            "Clear all test data from MongoDB?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes and self.db:
            result = self.db.clear_all_data()
            self.dashboard.log_activity(f"Database cleared: {result}", "WARNING")
    
    def export_positions(self):
        """Export positions to file"""
        if self.position_manager:
            filename = f"test_positions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self.position_manager.export_positions(filename)
            self.dashboard.log_activity(f"Positions exported to {filename}", "SUCCESS")
    
    def recover_positions(self):
        """Test position recovery"""
        if self.strategy:
            self.dashboard.log_activity("Testing position recovery...", "INFO")
            self.strategy.recover_positions_on_startup()
            
            # Update UI with recovered positions
            recovered = self.position_manager.get_all_positions()
            for symbol, position in recovered.items():
                self.dashboard.add_position_with_controls(position)
            
            self.dashboard.log_activity(f"Recovered {len(recovered)} positions", "SUCCESS")
    
    def run(self):
        """Show dashboard and run test"""
        self.dashboard.show()

def main():
    """Main entry point"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # Create and run test
    test = CompleteSystemTest()
    test.run()
    
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()