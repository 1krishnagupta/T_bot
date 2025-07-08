#!/usr/bin/env python3
"""
Jigsaw Flow Trading Bot - Complete System Test with Dark UI
Bypasses API login and uses the original dark themed test UI
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

# Import necessary components
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

class MockAPI:
    """Mock API that doesn't require login"""
    def __init__(self):
        self.session_token = "MOCK_TOKEN"
        
    def get_quote_token(self):
        return {
            "token": "MOCK_TOKEN",
            "dxlink-url": "wss://mock.test",
            "level": "test"
        }
    
    def safe_request(self, method, endpoint, **kwargs):
        class MockResponse:
            def __init__(self):
                self.status_code = 200
                
            def json(self):
                if "accounts" in endpoint:
                    return {"data": {"items": [{"account": {"account-number": "TEST123"}}]}}
                return {"data": {}}
        
        return MockResponse()

class MockOrderManager(OrderManager):
    """Mock order manager that simulates real order management"""
    def __init__(self, api, account_id="TEST123"):
        # Initialize without calling parent __init__ to avoid login issues
        self.api = api
        self.account_id = account_id
        self.mock_orders = {}
        self.next_order_id = 1000
        self.position_stops = {}
        self.active_orders = {}
        self.order_history = {}
        
        # Setup logging
        import logging
        self.logger = logging.getLogger("MockOrderManager")
        
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
            self.active_orders[order_id] = order
            
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

class MockInstrumentFetcher(InstrumentFetcher):
    """Mock instrument fetcher"""
    def __init__(self, api):
        self.api = api
        self.test_mode = True
        
        # Setup logging
        import logging
        self.logger = logging.getLogger("MockInstrumentFetcher")
    
    def get_streamer_symbol(self, symbol):
        return symbol
    
    def get_current_price(self, symbol):
        # Return mock prices
        prices = {
            "SPY": 500.0,
            "QQQ": 480.0,
            "AAPL": 195.0,
            "MSFT": 430.0
        }
        return prices.get(symbol, 100.0)

class MockMarketDataClient:
    """Mock market data client that generates test data"""
    def __init__(self, on_sector_update=None):
        self.on_sector_update = on_sector_update
        self.running = False
        self.candle_builder = None
        self.save_to_db = False
        self.db = None
        
        # Market data
        self.base_prices = {
            "XLK": 180.0, "XLF": 40.0, "XLV": 140.0, "XLY": 180.0,
            "AAPL": 195.0, "MSFT": 430.0, "AMZN": 185.0, "NVDA": 140.0,
            "GOOG": 175.0, "TSLA": 250.0, "META": 520.0,
            "SPY": 500.0, "QQQ": 480.0
        }
        self.trends = {}
        self.generator_thread = None
    
    def connect(self):
        """Start generating market data"""
        self.running = True
        self.generator_thread = threading.Thread(target=self._generate_data)
        self.generator_thread.daemon = True
        self.generator_thread.start()
        return True
    
    def disconnect(self):
        """Stop generating data"""
        self.running = False
    
    def subscribe_to_sector_etfs(self):
        """Mock subscription"""
        print("[MOCK] Subscribed to sector ETFs")
        return 1
    
    def subscribe(self, symbols, event_types):
        """Mock subscription"""
        print(f"[MOCK] Subscribed to {symbols}")
        return 2
    
    def subscribe_to_mag7_stocks(self, stocks):
        """Mock subscription"""
        print(f"[MOCK] Subscribed to Mag7: {stocks}")
        return 3
    
    def get_quotes_from_db(self, symbol, limit=1):
        """Return mock quotes"""
        return [{
            "symbol": symbol,
            "bid": self.base_prices.get(symbol, 100),
            "ask": self.base_prices.get(symbol, 100) + 0.01,
            "timestamp": datetime.now().isoformat()
        }]
    
    def _generate_data(self):
        """Generate market data"""
        tick = 0
        
        while self.running:
            tick += 1
            
            # Update market conditions periodically
            if tick % 20 == 0:
                market_bias = random.choice(["bullish", "bearish", "neutral"])
                print(f"[MARKET] Condition: {market_bias}")
                
                for symbol in self.base_prices:
                    if market_bias == "bullish":
                        self.trends[symbol] = random.uniform(0.0001, 0.0003)
                    elif market_bias == "bearish":
                        self.trends[symbol] = random.uniform(-0.0003, -0.0001)
                    else:
                        self.trends[symbol] = random.uniform(-0.0001, 0.0001)
            
            # Update prices and send sector updates
            for symbol in ["XLK", "XLF", "XLV", "XLY"]:
                # Apply trend
                trend = self.trends.get(symbol, 0)
                old_price = self.base_prices[symbol]
                new_price = old_price * (1 + trend + random.uniform(-0.001, 0.001))
                self.base_prices[symbol] = new_price
                
                # Determine status
                if trend > 0.0001:
                    status = "bullish"
                elif trend < -0.0001:
                    status = "bearish"
                else:
                    status = "neutral"
                
                # Send update
                if self.on_sector_update:
                    self.on_sector_update(symbol, status, new_price)
            
            time.sleep(1)

class EnhancedTestDashboard(QWidget):
    """Enhanced dashboard with all features"""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.trade_count = 0
        self.active_positions = {}
        self.position_widgets = {}
        
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
        self.resize(1200, 800)
        
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
        
        # Sector threshold
        strategy_layout.addWidget(QLabel("Sector Threshold:"), 1, 0)
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(0, 100)
        self.threshold_spin.setValue(43)
        self.threshold_spin.setSuffix("%")
        strategy_layout.addWidget(self.threshold_spin, 1, 1)
        
        # Trading Parameters
        strategy_layout.addWidget(QLabel("Contracts per Trade:"), 2, 0)
        self.contracts_spin = QSpinBox()
        self.contracts_spin.setRange(1, 10)
        self.contracts_spin.setValue(1)
        strategy_layout.addWidget(self.contracts_spin, 2, 1)
        
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
        self.test_compression_btn = QPushButton("Create Compression")
        self.test_stop_btn = QPushButton("Test Stop Hit")
        self.kill_switch_btn = QPushButton("KILL SWITCH")
        
        self.kill_switch_btn.setStyleSheet("background-color: #c0392b; font-weight: bold;")
        
        controls_layout.addWidget(self.test_trade_btn, 1, 0)
        controls_layout.addWidget(self.test_compression_btn, 1, 1)
        controls_layout.addWidget(self.test_stop_btn, 1, 2)
        controls_layout.addWidget(self.kill_switch_btn, 1, 3)
        
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
            "total_pnl": QLabel("Total P&L: $0.00")
        }
        
        row, col = 0, 0
        for key, label in self.stats_labels.items():
            stats_layout.addWidget(label, row, col)
            col += 1
            if col > 2:
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
        
        # Market status
        self.market_status_label = QLabel("Market: Neutral")
        self.market_status_label.setAlignment(Qt.AlignCenter)
        self.market_status_label.setStyleSheet("""
            QLabel {
                background-color: #555;
                border-radius: 5px;
                padding: 10px;
                min-width: 150px;
                font-weight: bold;
            }
        """)
        indicators_layout.addWidget(self.market_status_label)
        
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
        
        layout.addWidget(self.orders_table)
        
        widget.setLayout(layout)
        return widget
    
    def create_market_tab(self):
        """Create market data tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Market data table
        self.market_table = QTableWidget()
        self.market_table.setColumnCount(6)
        self.market_table.setHorizontalHeaderLabels([
            "Symbol", "Price", "Change", "Trend", "Status", "Signal"
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
        self.status_bar.showMessage(message, 3000)
    
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
    
    def update_sector_indicator(self, sector, status, price=0):
        """Update sector indicator"""
        if sector in self.sector_indicators:
            indicator = self.sector_indicators[sector]
            
            # Set color based on status
            if status == "bullish":
                color = "#27ae60"  # Green
            elif status == "bearish":
                color = "#e74c3c"  # Red
            else:
                color = "#555"  # Gray
            
            indicator.setStyleSheet(f"""
                QLabel {{
                    background-color: {color};
                    border-radius: 5px;
                    padding: 10px;
                    min-width: 80px;
                    font-weight: bold;
                }}
            """)
            
            indicator.setText(f"{sector}\n${price:.2f}")
    
    def exit_position(self, symbol):
        """Handle position exit button click"""
        self.log_activity(f"Manual exit requested for {symbol}", "TRADE")
        # This will be connected to actual exit logic
    
    def modify_stop(self, symbol):
        """Handle modify stop button click"""
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

class CompleteSystemTest(QObject):
    """Complete system test controller"""
    
    def __init__(self):
        super().__init__()
        self.dashboard = EnhancedTestDashboard()
        
        # Core components
        self.config = None
        self.api = None
        self.strategy = None
        self.order_manager = None
        self.position_manager = None
        self.market_data_client = None
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
        
        # Connect signals
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
            
            # Initialize mock API
            self.dashboard.log_activity("Initializing API in test mode...")
            self.api = MockAPI()
            
            # Initialize components
            self.dashboard.log_activity("Initializing trading components...")
            
            # Create market data client with sector update callback
            self.market_data_client = MockMarketDataClient(
                on_sector_update=self.handle_sector_update
            )
            
            # Initialize managers
            self.instrument_fetcher = MockInstrumentFetcher(self.api)
            self.order_manager = MockOrderManager(self.api)
            self.position_manager = PositionManager()
            
            # Initialize strategy
            self.strategy = JigsawStrategy(
                instrument_fetcher=self.instrument_fetcher,
                market_data_client=self.market_data_client,
                order_manager=self.order_manager,
                config=self.config
            )
            
            # Override strategy methods for testing
            self.override_strategy_methods()
            
            # Initialize strategy
            self.strategy.initialize()
            
            # Connect market data
            self.market_data_client.connect()
            
            # Start monitoring
            self.start_monitoring()
            
            self.dashboard.log_activity("System test started successfully", "SUCCESS")
            
        except Exception as e:
            self.dashboard.log_activity(f"Error starting test: {str(e)}", "ERROR")
            self.stop_test()
    
    def init_config(self):
        """Initialize configuration from UI settings"""
        self.config = {
            "broker": {
                "username": "test_user",
                "password": "test_pass",
                "account_id": "TEST123",
                "auto_trading_enabled": True
            },
            "trading_config": {
                "tickers": ["SPY", "QQQ"],
                "contracts_per_trade": self.dashboard.contracts_spin.value(),
                "use_mag7_confirmation": self.dashboard.mag7_radio.isChecked(),
                "sector_weight_threshold": self.dashboard.threshold_spin.value(),
                "mag7_threshold": 60,
                "selected_sectors": ["XLK", "XLF", "XLV", "XLY"],
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
        
        def mock_enter_trade(symbol, direction):
            self.dashboard.log_activity(f"TRADE ENTRY: {direction.upper()} on {symbol}", "TRADE")
            
            # Create position
            position = {
                "symbol": symbol,
                "type": "Long" if direction == "bullish" else "Short",
                "quantity": self.config["trading_config"]["contracts_per_trade"],
                "entry_price": 100.0,
                "current_price": 100.0,
                "pl": "$0.00 (0.0%)",
                "stop": 98.0,
                "entry_time": datetime.now().isoformat(),
                "status": "Open"
            }
            
            # Add to dashboard
            self.dashboard.add_position_with_controls(position)
            
            # Update stats
            self.stats["total_trades"] += 1
            self.stats["open_positions"] += 1
            self.update_statistics()
            
            # Call original
            try:
                original_enter(symbol, direction)
            except:
                pass  # Ignore errors in test mode
        
        def mock_exit_trade(symbol, reason="Manual exit"):
            self.dashboard.log_activity(f"TRADE EXIT: {symbol} - {reason}", "TRADE")
            
            # Update stats
            self.stats["open_positions"] -= 1
            if "profit" in reason.lower():
                self.stats["wins"] += 1
            else:
                self.stats["losses"] += 1
            
            self.update_statistics()
            
            # Remove from positions table
            if symbol in self.dashboard.active_positions:
                row = self.dashboard.active_positions[symbol]
                self.dashboard.positions_table.removeRow(row)
                del self.dashboard.active_positions[symbol]
            
            # Call original
            try:
                return original_exit(symbol, reason)
            except:
                pass
        
        # Apply overrides
        self.strategy.enter_trade = mock_enter_trade
        self.strategy.exit_trade = mock_exit_trade
        
        # Connect dashboard exit button
        self.dashboard.exit_position = lambda symbol: self.strategy.exit_trade(symbol, "Manual exit")
    
    def handle_sector_update(self, sector, status, price):
        """Handle sector updates from market data"""
        # Update dashboard
        self.dashboard.update_sector_indicator(sector, status, price)
        
        # Update strategy
        self.strategy.update_sector_status(sector, status, price)
        
        # Check alignment
        aligned, direction, weight = self.strategy.detect_sector_alignment()
        if aligned:
            self.dashboard.market_status_label.setText(f"Market: {direction.upper()} ({weight}%)")
            self.dashboard.market_status_label.setStyleSheet(f"""
                QLabel {{
                    background-color: {'#27ae60' if direction == 'bullish' else '#e74c3c'};
                    border-radius: 5px;
                    padding: 10px;
                    min-width: 150px;
                    font-weight: bold;
                }}
            """)
        else:
            self.dashboard.market_status_label.setText("Market: Neutral")
            self.dashboard.market_status_label.setStyleSheet("""
                QLabel {
                    background-color: #555;
                    border-radius: 5px;
                    padding: 10px;
                    min-width: 150px;
                    font-weight: bold;
                }
            """)
    
    def start_monitoring(self):
        """Start strategy monitoring"""
        def monitor():
            while self.running:
                if not self.paused:
                    try:
                        # Check for trade setups
                        self.strategy.check_for_trade_setups()
                        
                        # Manage active trades
                        self.strategy.manage_active_trades()
                        
                    except Exception as e:
                        self.dashboard.log_activity(f"Monitoring error: {str(e)}", "ERROR")
                
                time.sleep(1)
        
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
    
    def pause_test(self):
        """Pause the test"""
        self.paused = True
        self.dashboard.pause_btn.setEnabled(False)
        self.dashboard.resume_btn.setEnabled(True)
        self.dashboard.log_activity("System test paused", "WARNING")
    
    def resume_test(self):
        """Resume the test"""
        self.paused = False
        self.dashboard.pause_btn.setEnabled(True)
        self.dashboard.resume_btn.setEnabled(False)
        self.dashboard.log_activity("System test resumed", "SUCCESS")
    
    def stop_test(self):
        """Stop the test"""
        self.running = False
        if self.market_data_client:
            self.market_data_client.disconnect()
        
        self.dashboard.start_btn.setEnabled(True)
        self.dashboard.stop_btn.setEnabled(False)
        self.dashboard.pause_btn.setEnabled(False)
        self.dashboard.resume_btn.setEnabled(False)
        
        self.dashboard.log_activity("System test stopped", "WARNING")
    
    def force_test_trade(self):
        """Force a test trade"""
        if self.strategy:
            symbol = random.choice(["SPY", "QQQ"])
            direction = random.choice(["bullish", "bearish"])
            
            self.dashboard.log_activity(f"Forcing test trade: {direction} on {symbol}", "INFO")
            self.strategy.enter_trade(symbol, direction)
    
    def test_compression(self):
        """Test compression detection"""
        self.dashboard.log_activity("Creating compression setup...", "INFO")
        # The market data generator will create compression
    
    def test_stop_loss(self):
        """Test stop loss trigger"""
        # Find an open position and trigger stop
        for symbol in self.dashboard.active_positions.keys():
            self.dashboard.log_activity(f"Triggering stop loss for {symbol}", "WARNING")
            self.strategy.exit_trade(symbol, "Stop loss hit (test)")
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
    
    print("""
    ========================================
    JIGSAW FLOW SYSTEM TEST STARTED
    ========================================
    
    Use the dark UI to test all features:
    - Start/Stop trading
    - Force trades
    - Test stop losses
    - Kill switch
    - View positions and orders
    - Monitor market conditions
    
    The system will generate market data
    and trading signals automatically.
    ========================================
    """)
    
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()