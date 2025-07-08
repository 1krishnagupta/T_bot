import sys
import os
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLabel, QLineEdit, QPushButton, QCheckBox, 
                            QComboBox, QRadioButton, QStackedWidget, QSpinBox, QDoubleSpinBox, 
                            QTableWidget, QTableWidgetItem, QGroupBox, QFormLayout, QTimeEdit, 
                            QMessageBox, QSplitter, QTextEdit, QGridLayout, QFrame, 
                            QProgressBar, QFileDialog, QScrollArea)
from PyQt5.QtCore import Qt, QTime, QTimer, pyqtSignal, QThread, QMetaType, pyqtSlot
from PyQt5.QtGui import QFont, QColor, QPalette, QPixmap, QCursor, QLinearGradient, QBrush, QGradient, QPainter, QTextCursor
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtCore import QMetaObject, Q_ARG

# Register QTextCursor for thread-safe operations
try:
    # Try to register the type
    QMetaType.type("QTextCursor")
except:
    # If not registered, register it
    QMetaType(QMetaType.Type.User, b"QTextCursor")


class StyledPushButton(QPushButton):
    """Custom styled button with hover effects"""
    def __init__(self, text, parent=None, primary=False):
        super(StyledPushButton, self).__init__(text, parent)
        self.primary = primary
        self.setMinimumHeight(50)
        self.setCursor(QCursor(Qt.PointingHandCursor))
        
        # Initial style
        self.setFont(QFont("Arial", 12, QFont.Bold))
        self.update_style(False)
        
    def enterEvent(self, event):
        """Handle mouse enter event"""
        self.update_style(True)
        super().enterEvent(event)
        
    def leaveEvent(self, event):
        """Handle mouse leave event"""
        self.update_style(False)
        super().leaveEvent(event)
        
    def update_style(self, hover):
        """Update button style based on state"""
        if self.primary:
            # Primary button style (blue)
            if hover:
                self.setStyleSheet("""
                    QPushButton {
                        background-color: #3498db;
                        color: white;
                        border: 2px solid #2980b9;
                        border-radius: 8px;
                        padding: 10px;
                    }
                """)
            else:
                self.setStyleSheet("""
                    QPushButton {
                        background-color: #2980b9;
                        color: white;
                        border: 2px solid #2980b9;
                        border-radius: 8px;
                        padding: 10px;
                    }
                """)
        else:
            # Secondary button style (green)
            if hover:
                self.setStyleSheet("""
                    QPushButton {
                        background-color: #2ecc71;
                        color: white;
                        border: 2px solid #27ae60;
                        border-radius: 8px;
                        padding: 10px;
                    }
                """)
            else:
                self.setStyleSheet("""
                    QPushButton {
                        background-color: #27ae60;
                        color: white;
                        border: 2px solid #27ae60;
                        border-radius: 8px;
                        padding: 10px;
                    }
                """)


class LoginWidget(QWidget):
    """Widget for login screen - enhanced visual design"""
    login_requested = pyqtSignal(str)  # Path to config file
    
    def __init__(self):
        super().__init__()
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#f5f5f5"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)
        self.initUI()
        
    def initUI(self):
        # Main layout with more spacing
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(50, 50, 50, 50)  # Add more space around edges
        main_layout.setSpacing(30)  # Increase space between elements
        
        # Create card-like container for login
        card = QFrame()
        card.setFrameShape(QFrame.StyledPanel)
        card.setStyleSheet("""
            QFrame {
                background-color: white;
                border-radius: 15px;
                border: 1px solid #ddd;
            }
        """)
        card_layout = QVBoxLayout()
        card_layout.setContentsMargins(30, 40, 30, 40)  # Padding inside card
        card_layout.setSpacing(20)
        
        # Title with gradient effect
        title_label = QLabel("Jigsaw Flow Trading Bot")
        title_label.setAlignment(Qt.AlignCenter)
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("""
            QLabel {
                color: #2980b9;
                margin-bottom: 20px;
            }
        """)
        card_layout.addWidget(title_label)
        
        # Add logo or icon (placeholder with styling)
        logo_label = QLabel()
        logo_path = os.path.join(os.path.dirname(__file__), 'assets', 'logo.png')
        if os.path.exists(logo_path):
            pixmap = QPixmap(logo_path)
            if not pixmap.isNull():
                # Calculate proper size - don't use fixed size, adjust to container
                logo_label.setPixmap(pixmap.scaled(150, 150, Qt.KeepAspectRatio, Qt.SmoothTransformation))
                logo_label.setAlignment(Qt.AlignCenter)
                # Remove the blue background
                logo_label.setStyleSheet("")
        else:
            # Hide logo if not found
            logo_label.setMaximumHeight(0)
            logo_label.setVisible(False)
        # Logo path can be set externally
        # For now, add a placeholder with styling
        logo_label.setStyleSheet("""
            QLabel {
                background-color: #3498db;
                border-radius: 10px;
                min-height: 120px;
            }
        """)
        logo_label.setAlignment(Qt.AlignCenter)
        card_layout.addWidget(logo_label)
        
        # Welcome message with better styling
        welcome_label = QLabel("Welcome to Jigsaw Flow Options Trading Bot")
        welcome_label.setAlignment(Qt.AlignCenter)
        welcome_font = QFont()
        welcome_font.setPointSize(14)
        welcome_label.setFont(welcome_font)
        welcome_label.setStyleSheet("color: #333; margin-top: 20px;")
        card_layout.addWidget(welcome_label)
        
        # Status message with styling
        self.status_label = QLabel("Credentials will be loaded from config/credentials.txt")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("""
            QLabel {
                color: #7f8c8d;
                font-size: 12px;
                padding: 10px;
            }
        """)
        card_layout.addWidget(self.status_label)
        
        # Progress bar with styling
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #bdc3c7;
                border-radius: 5px;
                text-align: center;
                height: 25px;
            }
            QProgressBar::chunk {
                background-color: #3498db;
                border-radius: 5px;
            }
        """)
        card_layout.addWidget(self.progress_bar)
        
        # Spacer before button
        card_layout.addSpacing(10)
        
        # Login button with enhanced styling
        self.login_button = StyledPushButton("Login", primary=True)
        card_layout.addWidget(self.login_button)
        self.login_button.clicked.connect(self.on_login_clicked)
        
        # Finish setting up card
        card.setLayout(card_layout)
        main_layout.addWidget(card)
        
        # Option to use alternative config file in its own card
        config_box = QGroupBox("Configuration Options")
        config_box.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #ddd;
                margin-top: 20px;
                font-weight: bold;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2980b9;
            }
        """)
        config_layout = QVBoxLayout()
        config_layout.setContentsMargins(20, 30, 20, 20)  # Padding inside
        
        # Config checkbox with styling
        self.alt_config_checkbox = QCheckBox("Use alternative config file")
        self.alt_config_checkbox.setStyleSheet("""
            QCheckBox {
                font-size: 14px;
                color: #333;
            }
            QCheckBox::indicator {
                width: 20px;
                height: 20px;
            }
        """)
        config_layout.addWidget(self.alt_config_checkbox)
        
        # Config path layout
        path_layout = QHBoxLayout()
        path_layout.setSpacing(10)
        
        self.alt_config_path = QLineEdit()
        self.alt_config_path.setPlaceholderText("Path to alternative config file")
        self.alt_config_path.setEnabled(False)
        self.alt_config_path.setStyleSheet("""
            QLineEdit {
                padding: 10px;
                border: 1px solid #bdc3c7;
                border-radius: 5px;
                background-color: #f9f9f9;
                font-size: 14px;
            }
            QLineEdit:disabled {
                background-color: #ecf0f1;
                color: #7f8c8d;
            }
        """)
        
        self.browse_button = StyledPushButton("Browse...")
        self.browse_button.setEnabled(False)
        self.browse_button.setMinimumHeight(40)  # Smaller than login button
        
        path_layout.addWidget(self.alt_config_path)
        path_layout.addWidget(self.browse_button)
        config_layout.addLayout(path_layout)
        
        config_box.setLayout(config_layout)
        main_layout.addWidget(config_box)
        
        # Spacer to push everything to the top
        main_layout.addStretch()
        
        # Connect checkbox to enable/disable text field and browse button
        self.alt_config_checkbox.stateChanged.connect(self.toggle_alt_config)
        self.browse_button.clicked.connect(self.browse_config)
        
        # Set layout
        self.setLayout(main_layout)
        
    def toggle_alt_config(self, state):
        """Toggle alternative config file fields"""
        enabled = state == Qt.Checked
        self.alt_config_path.setEnabled(enabled)
        self.browse_button.setEnabled(enabled)
        
    def browse_config(self):
        """Browse for configuration file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Config File", "", "YAML Files (*.yaml *.yml);;Text Files (*.txt);;All Files (*)"
        )
        if file_path:
            self.alt_config_path.setText(file_path)
            
    def on_login_clicked(self):
        """Handle login button click"""
        # Get config path
        if self.alt_config_checkbox.isChecked() and self.alt_config_path.text():
            config_path = self.alt_config_path.text()
        else:
            config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'credentials.txt'))
            
        # Emit signal with config path
        self.login_requested.emit(config_path)
        
    def update_login_progress(self, message, progress):
        """Update progress bar and status message during login"""
        self.status_label.setText(message)
        self.progress_bar.setValue(progress)
        
    def update_status(self, message, is_error=False):
        """Update status message"""
        self.status_label.setText(message)
        if is_error:
            self.status_label.setStyleSheet("color: #e74c3c; font-size: 12px; padding: 10px;")
        else:
            self.status_label.setStyleSheet("color: #27ae60; font-size: 12px; padding: 10px;")
            
    def set_login_in_progress(self, in_progress):
        """Set login in progress state"""
        self.progress_bar.setVisible(in_progress)
        self.login_button.setEnabled(not in_progress)
        if in_progress:
            self.login_button.setText("Logging in...")
        else:
            self.login_button.setText("Login")



class TradingDashboardWidget(QWidget):
    """Widget for trading dashboard - UI with enhanced styling"""
    start_bot_requested = pyqtSignal()
    pause_bot_requested = pyqtSignal()
    resume_bot_requested = pyqtSignal()
    stop_bot_requested = pyqtSignal()
    kill_bot_requested = pyqtSignal()
    cancel_trade_requested = pyqtSignal(str)  # Order ID
    
    def __init__(self):
        super().__init__()
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#f5f5f5"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)
        self.initUI()
        
    def initUI(self):
        # Use a QScrollArea to enable scrolling
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.NoFrame)
        self.trades_table = QTableWidget()
        self.trades_table.setColumnCount(11)  # Increased columns
        self.trades_table.setHorizontalHeaderLabels([
            "Ticker", "Option Symbol", "Type", "Strike", "Expiry", 
            "Entry Time", "Entry Price", "Current P/L", 
            "Stop Level", "Status", "Action"
        ])
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Create the content widget for the scroll area
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # Account information panel with gradient background
        account_box = QGroupBox("Account Information")
        account_box.setStyleSheet("""
            QGroupBox {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #2c3e50, stop:1 #3498db);
                color: white;
                border-radius: 10px;
                font-size: 14px;
                font-weight: bold;
                padding: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: white;
            }
        """)
        account_layout = QHBoxLayout()
        account_layout.setContentsMargins(15, 25, 15, 15)
        account_layout.setSpacing(20)
        
        # Styled account information with icons
        self.account_label = QLabel("Account: Not logged in")
        self.account_label.setStyleSheet("color: white; font-size: 14px;")
        self.balance_label = QLabel("Balance: $0.00")
        self.balance_label.setStyleSheet("color: white; font-size: 14px;")
        self.available_label = QLabel("Available: $0.00")
        self.available_label.setStyleSheet("color: white; font-size: 14px;")
        
        account_layout.addWidget(self.account_label)
        account_layout.addWidget(self.balance_label)
        account_layout.addWidget(self.available_label)
        
        account_box.setLayout(account_layout)
        layout.addWidget(account_box)
        
        # Control buttons with different colors and hover effects
        control_layout = QHBoxLayout()
        control_layout.setSpacing(10)
        
        # Start button (green)
        self.start_button = QPushButton("Start Trading")
        self.start_button.setStyleSheet("""
            QPushButton {
                background-color: #2ecc71;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #27ae60;
            }
            QPushButton:pressed {
                background-color: #1d8348;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.start_button.setMinimumHeight(50)
        self.start_button.setCursor(Qt.PointingHandCursor)
        
        # Pause button (orange)
        self.pause_button = QPushButton("Pause")
        self.pause_button.setStyleSheet("""
            QPushButton {
                background-color: #f39c12;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #d35400;
            }
            QPushButton:pressed {
                background-color: #a04000;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.pause_button.setMinimumHeight(50)
        self.pause_button.setCursor(Qt.PointingHandCursor)
        self.pause_button.setEnabled(False)
        
        # Resume button (blue)
        self.resume_button = QPushButton("Resume")
        self.resume_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #1b4f72;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.resume_button.setMinimumHeight(50)
        self.resume_button.setCursor(Qt.PointingHandCursor)
        self.resume_button.setEnabled(False)
        
        # Stop button (red)
        self.stop_button = QPushButton("Stop")
        self.stop_button.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
            QPushButton:pressed {
                background-color: #922b21;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.stop_button.setMinimumHeight(50)
        self.stop_button.setCursor(Qt.PointingHandCursor)
        self.stop_button.setEnabled(False)
        
        # Kill switch button (dark red with warning styling)
        self.kill_button = QPushButton("KILL SWITCH")
        self.kill_button.setStyleSheet("""
            QPushButton {
                background-color: #c0392b;
                color: white;
                border: 2px solid #922b21;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #922b21;
                border: 2px solid #7b241c;
            }
            QPushButton:pressed {
                background-color: #7b241c;
            }
        """)
        self.kill_button.setMinimumHeight(50)
        self.kill_button.setCursor(Qt.PointingHandCursor)
        
        control_layout = QHBoxLayout()
        control_layout.setSpacing(10)

        control_layout.addWidget(self.start_button)
        control_layout.addWidget(self.pause_button)
        control_layout.addWidget(self.resume_button)
        control_layout.addWidget(self.stop_button)
        control_layout.addWidget(self.kill_button)
        # NO test_button here anymore

        # Connect signals (without test button)
        self.start_button.clicked.connect(self.start_bot_requested)
        self.pause_button.clicked.connect(self.pause_bot_requested)
        self.resume_button.clicked.connect(self.resume_bot_requested)
        self.stop_button.clicked.connect(self.stop_bot_requested)
        self.kill_button.clicked.connect(self.kill_bot_requested)
        
        layout.addLayout(control_layout)
        
        # Market summary panel with sector status and comparison
        market_summary_box = QGroupBox("Market Summary")
        market_summary_box.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #bdc3c7;
                font-size: 14px;
                font-weight: bold;
                margin-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2c3e50;
            }
        """)
        market_summary_layout = QVBoxLayout()
        market_summary_layout.setContentsMargins(15, 25, 15, 15)
        
        # Market weight indicators
        weights_layout = QHBoxLayout()
        
        # Bullish weight
        bullish_frame = QFrame()
        bullish_frame.setStyleSheet("""
            QFrame {
                background-color: #e8f8f5;
                border: 1px solid #2ecc71;
                border-radius: 5px;
                padding: 5px;
            }
        """)
        bullish_layout = QVBoxLayout(bullish_frame)
        bullish_layout.setContentsMargins(10, 10, 10, 10)
        
        bullish_title = QLabel("Bullish Weight")
        bullish_title.setStyleSheet("font-weight: bold; color: #2ecc71;")
        self.bullish_weight = QLabel("0%")
        self.bullish_weight.setStyleSheet("font-size: 18px; font-weight: bold; color: #2ecc71;")
        
        bullish_layout.addWidget(bullish_title, alignment=Qt.AlignCenter)
        bullish_layout.addWidget(self.bullish_weight, alignment=Qt.AlignCenter)
        
        # Bearish weight
        bearish_frame = QFrame()
        bearish_frame.setStyleSheet("""
            QFrame {
                background-color: #fdedec;
                border: 1px solid #e74c3c;
                border-radius: 5px;
                padding: 5px;
            }
        """)
        bearish_layout = QVBoxLayout(bearish_frame)
        bearish_layout.setContentsMargins(10, 10, 10, 10)
        
        bearish_title = QLabel("Bearish Weight")
        bearish_title.setStyleSheet("font-weight: bold; color: #e74c3c;")
        self.bearish_weight = QLabel("0%")
        self.bearish_weight.setStyleSheet("font-size: 18px; font-weight: bold; color: #e74c3c;")
        
        bearish_layout.addWidget(bearish_title, alignment=Qt.AlignCenter)
        bearish_layout.addWidget(self.bearish_weight, alignment=Qt.AlignCenter)
        
        # Neutral weight
        neutral_frame = QFrame()
        neutral_frame.setStyleSheet("""
            QFrame {
                background-color: #f8f9f9;
                border: 1px solid #95a5a6;
                border-radius: 5px;
                padding: 5px;
            }
        """)
        neutral_layout = QVBoxLayout(neutral_frame)
        neutral_layout.setContentsMargins(10, 10, 10, 10)
        
        neutral_title = QLabel("Neutral Weight")
        neutral_title.setStyleSheet("font-weight: bold; color: #95a5a6;")
        self.neutral_weight = QLabel("100%")
        self.neutral_weight.setStyleSheet("font-size: 18px; font-weight: bold; color: #95a5a6;")
        
        neutral_layout.addWidget(neutral_title, alignment=Qt.AlignCenter)
        neutral_layout.addWidget(self.neutral_weight, alignment=Qt.AlignCenter)
        
        # Overall market status
        status_frame = QFrame()
        status_frame.setStyleSheet("""
            QFrame {
                background-color: #eafaf1;
                border: 1px solid #27ae60;
                border-radius: 5px;
                padding: 5px;
            }
        """)
        status_layout = QVBoxLayout(status_frame)
        status_layout.setContentsMargins(10, 10, 10, 10)
        
        status_title = QLabel("Market Status")
        status_title.setStyleSheet("font-weight: bold; color: #27ae60;")
        self.market_status = QLabel("NEUTRAL")
        self.market_status.setStyleSheet("font-size: 18px; font-weight: bold; color: #27ae60;")
        
        status_layout.addWidget(status_title, alignment=Qt.AlignCenter)
        status_layout.addWidget(self.market_status, alignment=Qt.AlignCenter)
        
        weights_layout.addWidget(bullish_frame)
        weights_layout.addWidget(bearish_frame)
        weights_layout.addWidget(neutral_frame)
        weights_layout.addWidget(status_frame)
        
        market_summary_layout.addLayout(weights_layout)
        
        # Sector status with more attractive indicators and percentage change
        sectors = ["XLK (Tech)", "XLF (Financials)", "XLV (Health Care)", "XLY (Consumer)"]
        self.sector_status = {}
        self.sector_prices = {}
        self.sector_changes = {}
                
        sector_grid = QGridLayout()
        sector_grid.setSpacing(15)
        
        # Define sector weights
        sector_weights = {
            "XLK (Tech)": "32%",
            "XLF (Financials)": "14%", 
            "XLV (Health Care)": "11%",
            "XLY (Consumer)": "11%"
        }

        for i, sector in enumerate(sectors):
            # Create container frame for each sector
            sector_frame = QFrame()
            sector_frame.setStyleSheet("""
                QFrame {
                    border: 1px solid #ecf0f1;
                    border-radius: 5px;
                    background-color: #f9f9f9;
                }
            """)
            sector_layout = QHBoxLayout(sector_frame)
            sector_layout.setContentsMargins(10, 5, 10, 5)
            
            # Sector label
            label = QLabel(sector)
            label.setStyleSheet("font-weight: bold; color: #2c3e50;")
            
            # Price display
            price = QLabel("$0.00")
            price.setStyleSheet("color: #34495e; font-weight: bold;")
            self.sector_prices[sector] = price
            
            # Weight display (static percentage)
            weight = QLabel(sector_weights[sector])
            weight.setStyleSheet("color: #7f8c8d; font-weight: bold;")
            self.sector_changes[sector] = weight
            
            # Status indicator
            status = QLabel("Neutral")
            status.setStyleSheet("color: gray; font-weight: bold; padding: 2px 8px; background-color: #ecf0f1; border-radius: 3px;")
            
            sector_layout.addWidget(label)
            sector_layout.addStretch()
            sector_layout.addWidget(price)
            sector_layout.addWidget(weight)
            sector_layout.addWidget(status)
            
            sector_grid.addWidget(sector_frame, i // 2, i % 2)
            self.sector_status[sector] = status
            # Store price and change widgets in the class dictionaries
            self.sector_prices[sector] = price
            # self.sector_changes[sector] = change

        
        # Add sector alignment indicator (XLK + another sector > 43%)
        self.sector_alignment_frame = QFrame()
        self.sector_alignment_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #ecf0f1;
                border-radius: 5px;
                background-color: #f5f7fa;
                padding: 5px;
            }
        """)
        align_layout = QHBoxLayout(self.sector_alignment_frame)
        align_layout.setContentsMargins(10, 5, 10, 5)

        align_label = QLabel("Sector Alignment:")
        align_label.setStyleSheet("font-weight: bold; color: #2c3e50;")

        self.sector_alignment_status = QLabel("No Alignment Detected")
        self.sector_alignment_status.setStyleSheet("color: gray; font-weight: bold; padding: 2px 8px; background-color: #ecf0f1; border-radius: 3px;")

        align_layout.addWidget(align_label)
        align_layout.addStretch()
        align_layout.addWidget(self.sector_alignment_status)

        market_summary_layout.addLayout(sector_grid)
        market_summary_layout.addWidget(self.sector_alignment_frame)
        
        # Threshold display label
        self.threshold_label = QLabel("Sector Alignment Threshold: 43%")
        self.threshold_label.setStyleSheet("""
            QLabel {
                font-size: 12px;
                color: #7f8c8d;
                font-style: italic;
                padding: 5px;
                background-color: #f0f0f0;
                border-radius: 3px;
                margin-top: 5px;
            }
        """)
        market_summary_layout.addWidget(self.threshold_label)

        
        # Add compression status
        comp_frame = QFrame()
        comp_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #ecf0f1;
                border-radius: 5px;
                background-color: #f9f9f9;
                padding: 5px;
            }
        """)
        comp_layout = QHBoxLayout(comp_frame)
        comp_layout.setContentsMargins(10, 5, 10, 5)

        comp_label = QLabel("Compression Status:")
        comp_label.setStyleSheet("font-weight: bold; color: #2c3e50;")

        self.comp_status = QLabel("No Compression Detected")
        self.comp_status.setStyleSheet("color: gray; font-weight: bold; padding: 2px 8px; background-color: #ecf0f1; border-radius: 3px;")

        comp_layout.addWidget(comp_label)
        comp_layout.addStretch()
        comp_layout.addWidget(self.comp_status)

        market_summary_layout.addWidget(comp_frame)
        
        market_summary_box.setLayout(market_summary_layout)
        layout.addWidget(market_summary_box)
        
        # Splitter for the bottom section - allows user to resize panels
        bottom_splitter = QSplitter(Qt.Vertical)
        bottom_splitter.setChildrenCollapsible(False)
        
        # Active trades table with styling - make this larger
        trades_group = QGroupBox("Active Trades")
        trades_group.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #bdc3c7;
                font-size: 14px;
                font-weight: bold;
                margin-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2c3e50;
            }
        """)
        trades_layout = QVBoxLayout()
        trades_layout.setContentsMargins(15, 25, 15, 15)
        
        self.trades_table = QTableWidget()
        self.trades_table.setColumnCount(8)  # Increased column count to include cancel button
        self.trades_table.setHorizontalHeaderLabels([
            "Ticker", "Type", "Entry Time", "Entry Price", "Current P/L", "Stop Level", "Status", "Action"
        ])
        self.trades_table.horizontalHeader().setStretchLastSection(True)
        self.trades_table.setStyleSheet("""
            QTableWidget {
                border: none;
                gridline-color: #ecf0f1;
                outline: none;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                font-weight: bold;
                padding: 6px;
                border: none;
            }
            QTableWidget::item {
                padding: 5px;
                border-bottom: 1px solid #ecf0f1;
            }
            QTableWidget::item:selected {
                background-color: #3498db;
                color: white;
            }
        """)
        # Make the table taller to show more trades
        self.trades_table.setMinimumHeight(300)
        
        trades_layout.addWidget(self.trades_table)
        trades_group.setLayout(trades_layout)
        bottom_splitter.addWidget(trades_group)
        
        # Log output with styled text area
        log_group = QGroupBox("Bot Log")
        log_group.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #bdc3c7;
                font-size: 14px;
                font-weight: bold;
                margin-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2c3e50;
            }
        """)
        log_layout = QVBoxLayout()
        log_layout.setContentsMargins(15, 25, 15, 15)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("""
            QTextEdit {
                background-color: #2c3e50;
                color: #ecf0f1;
                border: none;
                border-radius: 5px;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 13px;
                padding: 10px;
            }
        """)
        
        log_layout.addWidget(self.log_output)
        log_group.setLayout(log_layout)
        bottom_splitter.addWidget(log_group)
        
        # Set initial splitter sizes to make trades panel larger
        bottom_splitter.setSizes([300, 200])
        
        layout.addWidget(bottom_splitter)
        
        # Set the content widget for the scroll area
        self.scroll_area.setWidget(content_widget)
        
        # Create main layout for this widget
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(self.scroll_area)


    def update_strategy_display(self, strategy_name):
        """Update the display to show which strategy is active"""
        # You can add a label to show the active strategy
        if hasattr(self, 'strategy_label'):
            self.strategy_label.setText(f"Active Strategy: {strategy_name}")
            if "Mag7" in strategy_name:
                self.strategy_label.setStyleSheet("""
                    QLabel {
                        font-size: 14px;
                        font-weight: bold;
                        color: #8e44ad;
                        padding: 5px;
                        background-color: #f4ecf7;
                        border-radius: 3px;
                    }
                """)
            else:
                self.strategy_label.setStyleSheet("""
                    QLabel {
                        font-size: 14px;
                        font-weight: bold;
                        color: #2980b9;
                        padding: 5px;
                        background-color: #ebf5fb;
                        border-radius: 3px;
                    }
                """)
                
        
    def set_account_info(self, account_id, balance, available):
        """Update account display with values"""
        self.account_label.setText(f"Account: {account_id}")
        self.balance_label.setText(f"Balance: ${balance:,.2f}")
        self.available_label.setText(f"Available: ${available:,.2f}")
    
    
    @pyqtSlot(str)
    def update_log(self, message):
        """Add message to log output with timestamp and color coding"""
        # Check if we're in the main thread
        if QThread.currentThread() != QApplication.instance().thread():
            # We're in a different thread, use invokeMethod to update UI safely
            from PyQt5.QtCore import QMetaObject, Q_ARG
            QMetaObject.invokeMethod(self, "_update_log_internal", 
                                    Qt.QueuedConnection,
                                    Q_ARG(str, message))
        else:
            # We're in the main thread, update directly
            self._update_log_internal(message)

    # ADD THIS METHOD RIGHT AFTER update_log:
    @pyqtSlot(str)
    def _update_log_internal(self, message):
        """Internal method to actually update the log (must be called from main thread)"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color code different types of messages
        if "ERROR" in message:
            html = f'<span style="color:#e74c3c">{timestamp} - {message}</span>'
        elif "Warning" in message or "Alert" in message:
            html = f'<span style="color:#f39c12">{timestamp} - {message}</span>'
        elif "Success" in message or "started" in message:
            html = f'<span style="color:#2ecc71">{timestamp} - {message}</span>'
        elif "Market Condition:" in message:
            if "BULLISH" in message:
                html = f'<span style="color:#2ecc71; font-weight:bold">{timestamp} - {message}</span>'
            elif "BEARISH" in message:
                html = f'<span style="color:#e74c3c; font-weight:bold">{timestamp} - {message}</span>'
            else:
                html = f'<span style="color:#95a5a6; font-weight:bold">{timestamp} - {message}</span>'
        elif "Market Summary:" in message:
            html = f'<span style="color:#3498db; font-style:italic">{timestamp} - {message}</span>'
        else:
            html = f'<span style="color:#ecf0f1">{timestamp} - {message}</span>'
            
        self.log_output.append(html)
        # Auto-scroll to bottom
        self.log_output.verticalScrollBar().setValue(
            self.log_output.verticalScrollBar().maximum()
        )
        
        # Update market weight indicators if this is a summary message
        if "Market Summary:" in message:
            try:
                # Parse percentages from the message
                parts = message.split(":")
                if len(parts) > 1:
                    weight_parts = parts[1].strip().split(",")
                    for part in weight_parts:
                        if "Bullish" in part:
                            bullish = part.split("%")[0].strip().split(" ")[-1]
                            self.bullish_weight.setText(f"{bullish}%")
                        elif "Bearish" in part:
                            bearish = part.split("%")[0].strip().split(" ")[-1]
                            self.bearish_weight.setText(f"{bearish}%")
                        elif "Neutral" in part:
                            neutral = part.split("%")[0].strip().split(" ")[-1]
                            self.neutral_weight.setText(f"{neutral}%")
            except Exception as e:
                print(f"Error parsing market summary: {e}")
                
        # Update market status indicator if this is a market condition message
        if "Market Condition:" in message:
            try:
                if "BULLISH" in message:
                    self.market_status.setText("BULLISH")
                    self.market_status.setStyleSheet("font-size: 18px; font-weight: bold; color: #2ecc71;")
                    status_frame = self.market_status.parentWidget()
                    status_frame.setStyleSheet("""
                        QFrame {
                            background-color: #eafaf1;
                            border: 1px solid #27ae60;
                            border-radius: 5px;
                            padding: 5px;
                        }
                    """)
                    status_title = status_frame.findChild(QLabel, "")
                    if status_title:
                        status_title.setStyleSheet("font-weight: bold; color: #27ae60;")
                elif "BEARISH" in message:
                    self.market_status.setText("BEARISH")
                    self.market_status.setStyleSheet("font-size: 18px; font-weight: bold; color: #e74c3c;")
                    status_frame = self.market_status.parentWidget()
                    status_frame.setStyleSheet("""
                        QFrame {
                            background-color: #fdedec;
                            border: 1px solid #e74c3c;
                            border-radius: 5px;
                            padding: 5px;
                        }
                    """)
                    status_title = status_frame.findChild(QLabel, "")
                    if status_title:
                        status_title.setStyleSheet("font-weight: bold; color: #e74c3c;")
                else:
                    self.market_status.setText("NEUTRAL")
                    self.market_status.setStyleSheet("font-size: 18px; font-weight: bold; color: #95a5a6;")
                    status_frame = self.market_status.parentWidget()
                    status_frame.setStyleSheet("""
                        QFrame {
                            background-color: #f8f9f9;
                            border: 1px solid #95a5a6;
                            border-radius: 5px;
                            padding: 5px;
                        }
                    """)
                    status_title = status_frame.findChild(QLabel, "")
                    if status_title:
                        status_title.setStyleSheet("font-weight: bold; color: #95a5a6;")
            except Exception as e:
                print(f"Error updating market status: {e}")
                


    def refresh_market_summary(self):
        """Refresh market summary display with current configuration"""
        try:
            # Try to get the main app window
            main_window = self.window()
            if hasattr(main_window, 'get_config_widget'):
                config_widget = main_window.get_config_widget()
                if config_widget:
                    config = config_widget.get_configuration()
                    threshold = config.get('sector_weight_threshold', 43)
                    
                    # Update the threshold display
                    if hasattr(self, 'threshold_label'):
                        self.threshold_label.setText(f"Sector Alignment Threshold: {threshold}%")
                    
                    # Update sector alignment frame title or status
                    if hasattr(self, 'sector_alignment_status'):
                        current_text = self.sector_alignment_status.text()
                        if "No Alignment" in current_text:
                            self.sector_alignment_status.setText(f"No Alignment (<{threshold}%)")
                    
                    # Refresh sector weights if they changed
                    if 'sector_weights' in config:
                        self._update_sector_weights_display(config['sector_weights'])
                        
        except Exception as e:
            print(f"Error refreshing market summary: {e}")
    
    def _update_sector_weights_display(self, sector_weights):
        """Update the displayed sector weights"""
        # Update the sector weight labels if they exist
        sector_mapping = {
            "XLK": "XLK (Tech)",
            "XLF": "XLF (Financials)",
            "XLV": "XLV (Health Care)",
            "XLY": "XLY (Consumer)"
        }
        
        for sector_code, display_name in sector_mapping.items():
            if sector_code in sector_weights and display_name in self.sector_changes:
                weight_label = self.sector_changes[display_name]
                weight_label.setText(f"{sector_weights[sector_code]}%")



    @pyqtSlot(str, str, float, float)
    def update_sector_status(self, symbol, status, price=None, change_pct=None):
        """
        Update sector status in the UI with better styling and percentage change
        
        Args:
            symbol (str): Sector symbol
            status (str): Status ("bullish", "bearish", "neutral")
            price (float): Current price
            change_pct (float): Percentage change
        """
        # Check if we're in the main thread
        if QThread.currentThread() != QApplication.instance().thread():
            # We're in a different thread, use invokeMethod to update UI safely
            from PyQt5.QtCore import QMetaObject, Q_ARG
            QMetaObject.invokeMethod(self, "_update_sector_status_internal", 
                                    Qt.QueuedConnection,
                                    Q_ARG(str, symbol),
                                    Q_ARG(str, status),
                                    Q_ARG(object, price),
                                    Q_ARG(object, change_pct))
        else:
            # We're in the main thread, update directly
            self._update_sector_status_internal(symbol, status, price, change_pct)

    @pyqtSlot(str, str, object, object)
    def _update_sector_status_internal(self, symbol, status, price=None, change_pct=None):
        """Internal method to update sector status (must be called from main thread)"""
        try:
            # Map symbol to sector names
            sector_names = {
                "XLK": "XLK (Tech)",
                "XLF": "XLF (Financials)",
                "XLV": "XLV (Health Care)",
                "XLY": "XLY (Consumer)"
            }
            
            # Get friendly name
            sector_name = sector_names.get(symbol, symbol)
            
            # Update the UI elements if the sector exists
            if sector_name in self.sector_status:
                # Update price if provided
                if price is not None and sector_name in self.sector_prices:
                    price_str = f"${price:.2f}"
                    self.sector_prices[sector_name].setText(price_str)
                
                # Update status with style
                status_widget = self.sector_status[sector_name]
                if status == "bullish":
                    status_widget.setText("Bullish")
                    status_widget.setStyleSheet(
                        "color: white; font-weight: bold; padding: 2px 8px; "
                        "background-color: #2ecc71; border-radius: 3px;"
                    )
                elif status == "bearish":
                    status_widget.setText("Bearish")
                    status_widget.setStyleSheet(
                        "color: white; font-weight: bold; padding: 2px 8px; "
                        "background-color: #e74c3c; border-radius: 3px;"
                    )
                else:
                    status_widget.setText("Neutral")
                    status_widget.setStyleSheet(
                        "color: white; font-weight: bold; padding: 2px 8px; "
                        "background-color: #7f8c8d; border-radius: 3px;"
                    )
                    
                # Note: We're keeping the static weight display, not updating with change_pct
                # The sector weights (32%, 14%, 11%, 11%) remain constant as per spec
                    
        except Exception as e:
            self.update_log(f"Error updating sector status: {str(e)}")     
    

    @pyqtSlot(bool, str, float)
    def update_sector_alignment(self, aligned, direction=None, combined_weight=0):
        """
        Update sector alignment status (XLK plus one other sector are trending in the same direction)
        
        Args:
            aligned (bool): Whether sectors are aligned
            direction (str): Direction of alignment ("bullish", "bearish", or None)
            combined_weight (float): Combined weight of aligned sectors
        """
        # Get current threshold from configuration if possible
        threshold = 43  # Default
        try:
            main_window = self.window()
            if hasattr(main_window, 'get_config_widget'):
                config_widget = main_window.get_config_widget()
                if config_widget:
                    config = config_widget.get_configuration()
                    threshold = config.get('sector_weight_threshold', 43)
        except:
            pass
            
        if aligned:
            if direction == "bullish":
                self.sector_alignment_status.setText(f"ALIGNED BULLISH ({combined_weight}% > {threshold}%)")
                self.sector_alignment_status.setStyleSheet(
                    "color: white; font-weight: bold; padding: 2px 8px; "
                    "background-color: #2ecc71; border-radius: 3px;"
                )
                self.sector_alignment_frame.setStyleSheet("""
                    QFrame {
                        border: 1px solid #2ecc71;
                        border-radius: 5px;
                        background-color: #e8f8f5;
                        padding: 5px;
                    }
                """)
            elif direction == "bearish":
                self.sector_alignment_status.setText(f"ALIGNED BEARISH ({combined_weight}% > {threshold}%)")
                self.sector_alignment_status.setStyleSheet(
                    "color: white; font-weight: bold; padding: 2px 8px; "
                    "background-color: #e74c3c; border-radius: 3px;"
                )
                self.sector_alignment_frame.setStyleSheet("""
                    QFrame {
                        border: 1px solid #e74c3c;
                        border-radius: 5px;
                        background-color: #fdedec;
                        padding: 5px;
                    }
                """)
        else:
            self.sector_alignment_status.setText(f"No Alignment (<{threshold}%)")
            self.sector_alignment_status.setStyleSheet(
                "color: white; font-weight: bold; padding: 2px 8px; "
                "background-color: #7f8c8d; border-radius: 3px;"
            )
            self.sector_alignment_frame.setStyleSheet("""
                QFrame {
                    border: 1px solid #ecf0f1;
                    border-radius: 5px;
                    background-color: #f5f7fa;
                    padding: 5px;
                }
            """)


    @pyqtSlot(bool, str)
    def update_compression_status(self, detected, direction=None):
        """Update compression status in the UI with better styling"""
        if detected:
            if direction == "bullish":
                self.comp_status.setText("Sector Aligned + Compression (Bullish)")
                self.comp_status.setStyleSheet(
                    "color: white; font-weight: bold; padding: 2px 8px; "
                    "background-color: #2ecc71; border-radius: 3px;"
                )
            elif direction == "bearish":
                self.comp_status.setText("Sector Aligned + Compression (Bearish)")
                self.comp_status.setStyleSheet(
                    "color: white; font-weight: bold; padding: 2px 8px; "
                    "background-color: #e74c3c; border-radius: 3px;"
                )
            else:
                self.comp_status.setText("Compression Detected")
                self.comp_status.setStyleSheet(
                    "color: white; font-weight: bold; padding: 2px 8px; "
                    "background-color: #3498db; border-radius: 3px;"
                )
        else:
            self.comp_status.setText("No Sector Alignment/Compression")
            self.comp_status.setStyleSheet(
                "color: white; font-weight: bold; padding: 2px 8px; "
                "background-color: #7f8c8d; border-radius: 3px;"
            )
            
    @pyqtSlot(dict)
    def add_trade(self, trade_data):
        """Add trade to the active trades table with enhanced option details"""
        row_position = self.trades_table.rowCount()
        self.trades_table.insertRow(row_position)
        
        # Create items for all columns
        columns = [
            trade_data.get("ticker", ""),
            trade_data.get("option_symbol", ""),
            trade_data.get("type", ""),
            trade_data.get("strike", ""),
            trade_data.get("expiry", ""),
            trade_data.get("entry_time", ""),
            trade_data.get("entry_price", ""),
            trade_data.get("pl", ""),
            trade_data.get("stop", ""),
            trade_data.get("status", "")
        ]
        
        for col, value in enumerate(columns):
            item = QTableWidgetItem(str(value))
            self.trades_table.setItem(row_position, col, item)
            
        # Add styling based on option type
        if "Call" in trade_data.get("type", ""):
            color = QColor(240, 255, 240)  # Light green for calls
        else:
            color = QColor(255, 240, 240)  # Light red for puts
            
        for col in range(len(columns)):
            item = self.trades_table.item(row_position, col)
            if item:
                item.setBackground(color)
                
        # Special handling for type column
        type_item = self.trades_table.item(row_position, 1)
        if type_item:
            type_item.setBackground(type_color)
            type_item.setForeground(QColor(30, 130, 30) if trade_data["type"] == "Long" else QColor(180, 30, 30))
            
        # Enhanced P/L color coding with gradient effect
        try:
            pl_text = trade_data["pl"].replace("$", "").replace("%", "")
            pl_value = float(pl_text)
            if pl_value > 0:
                # Green gradient based on P/L value
                intensity = min(255, 100 + int(pl_value * 3))
                self.trades_table.item(row_position, 4).setForeground(QColor(30, intensity, 30))
                self.trades_table.item(row_position, 4).setBackground(QColor(220, 255, 220))
                # Make the text bold for emphasis
                font = self.trades_table.item(row_position, 4).font()
                font.setBold(True)
                self.trades_table.item(row_position, 4).setFont(font)
            elif pl_value < 0:
                # Red gradient based on P/L value
                intensity = min(255, 100 + int(abs(pl_value) * 3))
                self.trades_table.item(row_position, 4).setForeground(QColor(intensity, 30, 30))
                self.trades_table.item(row_position, 4).setBackground(QColor(255, 220, 220))
                # Make the text bold for emphasis
                font = self.trades_table.item(row_position, 4).font()
                font.setBold(True)
                self.trades_table.item(row_position, 4).setFont(font)
        except (ValueError, AttributeError):
            # If we can't parse the P/L value, ignore the color coding
            pass
        
        # Add status highlighting
        status_cell = self.trades_table.item(row_position, 6)
        if status_cell:
            if trade_data["status"] == "Open":
                status_cell.setBackground(QColor(230, 250, 230))  # Light green
                status_cell.setForeground(QColor(30, 130, 30))    # Dark green
            elif trade_data["status"] == "Closed":
                status_cell.setBackground(QColor(240, 240, 240))  # Light gray
                status_cell.setForeground(QColor(100, 100, 100))  # Dark gray
            elif "Stop" in trade_data["status"]:
                status_cell.setBackground(QColor(255, 235, 230))  # Light orange-red
                status_cell.setForeground(QColor(180, 80, 30))    # Dark orange
        
        # Add a Cancel button
        cancel_button = QPushButton("Cancel")
        cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 5px;
                font-weight: bold;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
            QPushButton:pressed {
                background-color: #922b21;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        cancel_button.setEnabled(trade_data["status"] == "Open")
        
        # Store the order ID in the button's property
        if "order_id" in trade_data:
            cancel_button.setProperty("order_id", trade_data["order_id"])
        
        # Connect the cancel button signal to a callback function
        cancel_button.clicked.connect(lambda: self.cancel_trade(trade_data))
        
        # Add the button to the table
        self.trades_table.setCellWidget(row_position, 7, cancel_button)
    
    def cancel_trade(self, trade_data):
        """Handle cancel button click for a trade"""
        if "order_id" in trade_data:
            order_id = trade_data["order_id"]
            # Emit a signal to cancel the order
            # This signal should be connected to a method in the controller
            # that actually cancels the order
            self.cancel_trade_requested.emit(order_id)


    def update_bot_controls(self, running=False, paused=False):
        """Update control button states based on bot status"""
        self.start_button.setEnabled(not running)
        self.pause_button.setEnabled(running and not paused)
        self.resume_button.setEnabled(running and paused)
        self.stop_button.setEnabled(running)


class MongoDBManagerWidget(QWidget):
    """Widget for MongoDB database management"""
    refresh_requested = pyqtSignal()
    clear_db_requested = pyqtSignal()
    start_mongodb_requested = pyqtSignal()
    stop_mongodb_requested = pyqtSignal()
    
    def __init__(self):
        super().__init__()
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#f5f5f5"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)
        self.initUI()
        
    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # Header
        header_layout = QVBoxLayout()
        title = QLabel("MongoDB Database Manager")
        title.setStyleSheet("""
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        """)
        description = QLabel("View and manage MongoDB collections and data")
        description.setStyleSheet("""
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 15px;
        """)
        
        header_layout.addWidget(title)
        header_layout.addWidget(description)
        layout.addLayout(header_layout)
        
        # MongoDB Control Panel
        control_box = QGroupBox("MongoDB Control")
        control_box.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #bdc3c7;
                font-size: 14px;
                font-weight: bold;
                margin-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2c3e50;
            }
        """)
        control_layout = QVBoxLayout()
        control_layout.setContentsMargins(15, 25, 15, 15)
        
        # Status indicator
        status_layout = QHBoxLayout()
        self.status_indicator = QLabel()
        self.status_indicator.setFixedSize(16, 16)
        self.status_indicator.setStyleSheet("""
            background-color: #e74c3c;
            border-radius: 8px;
        """)
        
        self.status_text = QLabel("MongoDB Status: Not Running")
        self.status_text.setStyleSheet("""
            font-size: 14px;
            font-weight: bold;
            color: #e74c3c;
        """)
        
        status_layout.addWidget(self.status_indicator)
        status_layout.addWidget(self.status_text)
        status_layout.addStretch()
        
        control_layout.addLayout(status_layout)
        
        # MongoDB control buttons
        buttons_layout = QHBoxLayout()
        
        self.start_button = QPushButton("Start MongoDB")
        self.start_button.setStyleSheet("""
            QPushButton {
                background-color: #2ecc71;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #27ae60;
            }
            QPushButton:pressed {
                background-color: #1d8348;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.start_button.setMinimumHeight(40)
        self.start_button.setCursor(Qt.PointingHandCursor)
        self.start_button.clicked.connect(self.start_mongodb_requested)
        
        self.stop_button = QPushButton("Stop MongoDB")
        self.stop_button.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
            QPushButton:pressed {
                background-color: #922b21;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.stop_button.setMinimumHeight(40)
        self.stop_button.setCursor(Qt.PointingHandCursor)
        self.stop_button.clicked.connect(self.stop_mongodb_requested)
        
        buttons_layout.addWidget(self.start_button)
        buttons_layout.addWidget(self.stop_button)
        
        control_layout.addLayout(buttons_layout)
        control_box.setLayout(control_layout)
        
        layout.addWidget(control_box)
        
        # Database information panel
        info_box = QGroupBox("Database Information")
        info_box.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #bdc3c7;
                font-size: 14px;
                font-weight: bold;
                margin-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #2c3e50;
            }
        """)
        info_layout = QVBoxLayout()
        info_layout.setContentsMargins(15, 25, 15, 15)
        
        # Database name and status
        db_info_layout = QHBoxLayout()
        
        self.db_name_label = QLabel("Database: trading_bot")
        self.db_name_label.setStyleSheet("""
            font-size: 14px;
            font-weight: bold;
            color: #2c3e50;
        """)
        
        self.status_label = QLabel("Status: Connected")
        self.status_label.setStyleSheet("""
            font-size: 14px;
            color: #27ae60;
        """)
        
        db_info_layout.addWidget(self.db_name_label)
        db_info_layout.addStretch()
        db_info_layout.addWidget(self.status_label)
        
        info_layout.addLayout(db_info_layout)
        
        # Add table for collections
        self.collections_table = QTableWidget()
        self.collections_table.setColumnCount(3)
        self.collections_table.setHorizontalHeaderLabels([
            "Collection Name", "Document Count", "Size"
        ])
        self.collections_table.horizontalHeader().setStretchLastSection(True)
        self.collections_table.setStyleSheet("""
            QTableWidget {
                border: none;
                gridline-color: #ecf0f1;
                outline: none;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                font-weight: bold;
                padding: 6px;
                border: none;
            }
            QTableWidget::item {
                padding: 5px;
                border-bottom: 1px solid #ecf0f1;
            }
            QTableWidget::item:selected {
                background-color: #3498db;
                color: white;
            }
        """)
        
        info_layout.addWidget(self.collections_table)
        
        # Add refresh button
        button_layout = QHBoxLayout()
        
        self.refresh_button = QPushButton("Refresh Statistics")
        self.refresh_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #1b4f72;
            }
        """)
        self.refresh_button.setMinimumHeight(40)
        self.refresh_button.setCursor(Qt.PointingHandCursor)
        self.refresh_button.clicked.connect(self.refresh_requested)
        
        # Clear button
        self.clear_button = QPushButton("Clear Database")
        self.clear_button.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
            QPushButton:pressed {
                background-color: #922b21;
            }
        """)
        self.clear_button.setMinimumHeight(40)
        self.clear_button.setCursor(Qt.PointingHandCursor)
        self.clear_button.clicked.connect(self.clear_db_requested)
        
        button_layout.addWidget(self.refresh_button)
        button_layout.addWidget(self.clear_button)
        
        info_layout.addLayout(button_layout)
        info_box.setLayout(info_layout)
        
        layout.addWidget(info_box)
        
        # Add warning notice
        warning_label = QLabel("Warning: Clearing the database will permanently delete all data. This action cannot be undone.")
        warning_label.setStyleSheet("""
            color: #e74c3c;
            font-weight: bold;
            font-style: italic;
            font-size: 12px;
            padding: 10px;
            background-color: #fadbd8;
            border-radius: 5px;
            border: 1px solid #e74c3c;
        """)
        warning_label.setWordWrap(True)
        layout.addWidget(warning_label)
        
        # Add spacer
        layout.addStretch()
        
        # Set layout
        self.setLayout(layout)
        
    def update_stats(self, stats):
        """Update collection statistics in the table"""
        # Clear table
        self.collections_table.setRowCount(0)
        
        # Add collections to table
        for collection_name, collection_stats in stats.items():
            row_position = self.collections_table.rowCount()
            self.collections_table.insertRow(row_position)
            
            # Set data in table
            self.collections_table.setItem(row_position, 0, QTableWidgetItem(collection_name))
            self.collections_table.setItem(row_position, 1, QTableWidgetItem(str(collection_stats.get("count", 0))))
            self.collections_table.setItem(row_position, 2, QTableWidgetItem(f"{collection_stats.get('size_mb', 0)} MB"))
            
            # Add some styling
            self.collections_table.item(row_position, 0).setForeground(QColor(44, 62, 80))  # Dark blue text
            self.collections_table.item(row_position, 0).setBackground(QColor(236, 240, 241))  # Light gray background
            
            # Highlight rows with data
            if collection_stats.get("count", 0) > 0:
                # Make the count bold
                font = self.collections_table.item(row_position, 1).font()
                font.setBold(True)
                self.collections_table.item(row_position, 1).setFont(font)
                self.collections_table.item(row_position, 1).setForeground(QColor(46, 204, 113))  # Green text
        
        # Update status
        if len(stats) > 0:
            self.status_label.setText("Status: Connected")
            self.status_label.setStyleSheet("font-size: 14px; color: #27ae60;")
        else:
            self.status_label.setText("Status: No collections found")
            self.status_label.setStyleSheet("font-size: 14px; color: #f39c12;")
            
        # Resize columns to content
        self.collections_table.resizeColumnsToContents()
    
    def update_mongodb_status(self, is_running):
        """Update MongoDB running status indicators"""
        if is_running:
            self.status_indicator.setStyleSheet("""
                background-color: #2ecc71;
                border-radius: 8px;
            """)
            self.status_text.setText("MongoDB Status: Running")
            self.status_text.setStyleSheet("""
                font-size: 14px;
                font-weight: bold;
                color: #2ecc71;
            """)
            
            # Update button states
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
        else:
            self.status_indicator.setStyleSheet("""
                background-color: #e74c3c;
                border-radius: 8px;
            """)
            self.status_text.setText("MongoDB Status: Not Running")
            self.status_text.setStyleSheet("""
                font-size: 14px;
                font-weight: bold;
                color: #e74c3c;
            """)
            
            # Update button states
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
    
    def set_controls_enabled(self, enabled):
        """Enable or disable control buttons"""
        self.start_button.setEnabled(enabled)
        self.stop_button.setEnabled(enabled)
        self.clear_button.setEnabled(enabled)
        self.refresh_button.setEnabled(enabled)



class ConfigurationWidget(QWidget):
    """Widget for configuring trading bot parameters with enhanced styling"""
    save_config_requested = pyqtSignal(dict)
    
    def __init__(self):
        super().__init__()
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#f5f5f5"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)
        self.initUI()
        
    def create_info_button(self, tooltip_text):
        """Create a small info button with tooltip"""
        info_button = QPushButton("ⓘ")
        info_button.setFixedSize(20, 20)
        info_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border-radius: 10px;
                font-size: 12px;
                font-weight: bold;
                border: none;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #21618c;
            }
        """)
        info_button.setToolTip(tooltip_text)
        info_button.setCursor(Qt.WhatsThisCursor)
        
        # Show tooltip on click
        info_button.clicked.connect(lambda: QMessageBox.information(self, "Information", tooltip_text))
        
        return info_button
        
    def initUI(self):
        # Main layout
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)  # Reduce main layout margins
        
        # Header and description - KEEP THESE OUTSIDE the scroll area
        header_layout = QVBoxLayout()
        header_layout.setContentsMargins(20, 20, 20, 10)
        
        title = QLabel("Bot Configuration")
        title.setStyleSheet("""
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        """)
        description = QLabel("Configure your trading parameters and strategies")
        description.setStyleSheet("""
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 15px;
        """)
        
        header_layout.addWidget(title)
        header_layout.addWidget(description)
        layout.addLayout(header_layout)
        
        # CREATE SCROLL AREA
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        scroll_area.setStyleSheet("""
            QScrollArea {
                background-color: transparent;
                border: none;
            }
        """)
        
        # Create content widget for scroll area
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(20, 10, 20, 20)
        content_layout.setSpacing(15)
        
        # Add sections to organize settings
        sections = [
            ("Basic Settings", self.create_basic_settings),
            ("Time Parameters", self.create_time_settings),
            ("Stop Loss Settings", self.create_stop_loss_settings),
            ("Technical Indicators", self.create_indicator_settings),
            ("Compression Detection", self.create_compression_settings),
            ("Sector Configuration", self.create_sector_settings),
            ("Stochastic Settings", self.create_stochastic_settings),
            ("Liquidity Filters", self.create_liquidity_settings)
        ]
        
        # Add each section to the content layout
        for title, create_func in sections:
            section = self.create_section(title, create_func())
            content_layout.addWidget(section)
        
        # Set the content widget for scroll area
        scroll_area.setWidget(content_widget)
        
        # Add scroll area to main layout
        layout.addWidget(scroll_area)
        
        # Save and reset buttons - KEEP THESE OUTSIDE the scroll area
        button_layout = QHBoxLayout()
        button_layout.setContentsMargins(20, 10, 20, 20)
        button_layout.setSpacing(15)
        
        self.save_button = QPushButton("Save Configuration")
        self.save_button.setStyleSheet("""
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px;
                font-weight: bold;
                font-size: 14px;
                min-width: 200px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
            QPushButton:pressed {
                background-color: #1a5276;
            }
        """)
        self.save_button.setCursor(Qt.PointingHandCursor)
        
        self.reset_button = QPushButton("Reset to Defaults")
        self.reset_button.setStyleSheet("""
            QPushButton {
                background-color: #95a5a6;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px;
                font-weight: bold;
                font-size: 14px;
                min-width: 200px;
            }
            QPushButton:hover {
                background-color: #7f8c8d;
            }
            QPushButton:pressed {
                background-color: #5c686a;
            }
        """)
        self.reset_button.setCursor(Qt.PointingHandCursor)
        
        # Add some spacing to push buttons to the right
        button_layout.addStretch()
        button_layout.addWidget(self.reset_button)
        button_layout.addWidget(self.save_button)
        
        # Connect signals
        self.save_button.clicked.connect(self.save_configuration)
        self.reset_button.clicked.connect(self.reset_configuration)
        
        layout.addLayout(button_layout)
        
        # Set layout
        self.setLayout(layout)
    
    def save_configuration(self):
        """Save configuration to file"""
        # Get current configuration
        trading_config = self.get_configuration()
        
        # Emit signal to save config
        self.save_config_requested.emit(trading_config)
        
        # Note: The actual update of UI will be done by set_configuration 
        # which is called from ui_controller after successful save

    def get_configuration(self):
        """Get configuration from widget values"""
        # Determine which strategy is selected
        use_mag7 = "Mag7" in self.strategy_selector.currentText()
        
        config = {
            'tickers': [t.strip() for t in self.ticker_list.text().split(',')],
            'contracts_per_trade': self.contracts.value(),
            'trailing_stop_method': self.trailing_stop.currentText(),
            'no_trade_window_minutes': self.no_trade_minutes.value(),
            'auto_close_minutes': self.auto_close_minutes.value(),
            'cutoff_time': self.cutoff_time.time().toString("hh:mm"),
            'ema_value': self.ema_value.value(),
            'failsafe_minutes': self.failsafe_minutes.value(),
            'adx_filter': self.adx_filter.isChecked(),
            'adx_minimum': self.adx_value.value(),
            'news_filter': self.news_filter.isChecked(),
            'bb_width_threshold': self.bb_width.value(),
            'donchian_contraction_threshold': self.donchian_threshold.value(),
            'volume_squeeze_threshold': self.volume_squeeze.value(),
            
            # Stop loss configuration
            'stop_loss_method': self.sl_method.currentText(),
            'atr_multiple': self.atr_multiple.value(),
            'fixed_stop_percentage': self.fixed_percentage.value(),
            
            # Auto-trading configuration
            'auto_trading_enabled': self.auto_trading.isChecked(),
            
            # Stochastic settings
            'stochastic_k_period': self.stoch_k.value(),
            'stochastic_d_period': self.stoch_d.value(),
            'stochastic_smooth': self.stoch_smooth.value(),
            
            # Liquidity settings
            'volume_spike_threshold': self.vol_spike.value(),
            'liquidity_min_volume': self.liquidity_min_vol.value(),
            'liquidity_min_oi': self.min_oi.value(),
            'liquidity_max_spread': self.max_spread.value(),
            
            # Strategy selection
            'use_mag7_confirmation': use_mag7,
            
            # Sector settings (always include, even if not active)
            'sector_etfs': [t.strip() for t in self.sector_etfs.text().split(',')],
            'sector_weight_threshold': self.sector_threshold.value(),
            'sector_weights': {
                'XLK': self.xlk_weight.value(),
                'XLF': self.xlf_weight.value(),
                'XLV': self.xlv_weight.value(),
                'XLY': self.xly_weight.value()
            },
            
            # Mag7 settings (always include, even if not active)
            'mag7_stocks': [t.strip() for t in self.mag7_stocks.text().split(',')],
            'mag7_threshold': self.mag7_threshold.value(),
            'mag7_price_change_threshold': self.mag7_price_change.value(),
            'mag7_min_aligned': self.mag7_min_aligned.value()
        }
        
        return config
        
    def reset_configuration(self):
        """Reset configuration to defaults"""
        # Reset to default values
        self.ticker_list.setText("SPY, QQQ, TSLA, AAPL")
        self.contracts.setValue(1)
        self.trailing_stop.setCurrentIndex(0)
        self.no_trade_minutes.setValue(3)
        self.auto_close_minutes.setValue(15)
        self.cutoff_time.setTime(QTime(15, 15))
        self.ema_value.setValue(15)
        self.failsafe_minutes.setValue(20)
        self.adx_filter.setChecked(True)
        self.adx_value.setValue(20)
        self.news_filter.setChecked(False)
        self.bb_width.setValue(0.05)
        self.donchian_threshold.setValue(0.6)
        self.volume_squeeze.setValue(0.3)
        
        # Reset stop loss configuration
        self.sl_method.setCurrentIndex(0)  # ATR Multiple
        self.atr_multiple.setValue(1.5)
        self.fixed_percentage.setValue(1.0)
        
        # Reset stochastic settings
        self.stoch_k.setValue(5)
        self.stoch_d.setValue(3)
        self.stoch_smooth.setValue(2)
        
        # Reset liquidity settings
        self.vol_spike.setValue(1.5)
        self.liquidity_min_vol.setValue(1000000)
        self.min_oi.setValue(500)
        self.max_spread.setValue(0.10)
        
        # Reset sector settings
        self.sector_etfs.setText("XLK, XLF, XLV, XLY")
        self.sector_threshold.setValue(43)
        self.xlk_weight.setValue(32)
        self.xlf_weight.setValue(14)
        self.xlv_weight.setValue(11)
        self.xly_weight.setValue(11)

        # Reset strategy selection
        self.strategy_selector.setCurrentText("Sector Alignment Strategy")
        
        # Reset Mag7 settings
        self.mag7_stocks.setText("AAPL, MSFT, AMZN, NVDA, GOOG, TSLA, META")
        self.mag7_threshold.setValue(60)
        self.mag7_price_change.setValue(0.1)
        self.mag7_min_aligned.setValue(5)
        
        QMessageBox.information(self, "Reset Configuration", "Configuration has been reset to defaults")
        
    def set_configuration(self, config):
        """Set widget values from config"""
        if not config:
            return
        
        # Set strategy selector first
        if "use_mag7_confirmation" in config:
            if config["use_mag7_confirmation"]:
                self.strategy_selector.setCurrentText("Magnificent 7 (Mag7) Strategy")
            else:
                self.strategy_selector.setCurrentText("Sector Alignment Strategy")
        
        # Debug logging
        print(f"[DEBUG] Setting configuration in UI:")
        print(f"  - sector_weight_threshold: {config.get('sector_weight_threshold', 43)}")
            
        if "auto_trading_enabled" in config:
            self.auto_trading.setChecked(config["auto_trading_enabled"])

        # Set values from config
        if "tickers" in config:
            if isinstance(config["tickers"], list):
                self.ticker_list.setText(", ".join(config["tickers"]))
            else:
                self.ticker_list.setText(str(config["tickers"]))
                
        if "contracts_per_trade" in config:
            self.contracts.setValue(config["contracts_per_trade"])
            
        if "trailing_stop_method" in config:
            index = self.trailing_stop.findText(config["trailing_stop_method"])
            if index >= 0:
                self.trailing_stop.setCurrentIndex(index)
                
        if "no_trade_window_minutes" in config:
            self.no_trade_minutes.setValue(config["no_trade_window_minutes"])
            
        if "auto_close_minutes" in config:
            self.auto_close_minutes.setValue(config["auto_close_minutes"])
            
        if "cutoff_time" in config:
            time_parts = config["cutoff_time"].split(":")
            if len(time_parts) == 2:
                self.cutoff_time.setTime(QTime(int(time_parts[0]), int(time_parts[1])))
                
        if "ema_value" in config:
            self.ema_value.setValue(config["ema_value"])
            
        if "failsafe_minutes" in config:
            self.failsafe_minutes.setValue(config["failsafe_minutes"])
            
        if "adx_filter" in config:
            self.adx_filter.setChecked(config["adx_filter"])
            
        if "adx_minimum" in config:
            self.adx_value.setValue(config["adx_minimum"])
            
        if "news_filter" in config:
            self.news_filter.setChecked(config["news_filter"])
            
        if "bb_width_threshold" in config:
            self.bb_width.setValue(config["bb_width_threshold"])
            
        if "donchian_contraction_threshold" in config:
            self.donchian_threshold.setValue(config["donchian_contraction_threshold"])
            
        if "volume_squeeze_threshold" in config:
            self.volume_squeeze.setValue(config["volume_squeeze_threshold"])

        # Set stop loss configuration
        if "stop_loss_method" in config:
            index = self.sl_method.findText(config["stop_loss_method"])
            if index >= 0:
                self.sl_method.setCurrentIndex(index)
                
        if "atr_multiple" in config:
            self.atr_multiple.setValue(config["atr_multiple"])
            
        if "fixed_stop_percentage" in config:
            self.fixed_percentage.setValue(config["fixed_stop_percentage"])
            
        # Set stochastic settings
        if "stochastic_k_period" in config:
            self.stoch_k.setValue(config["stochastic_k_period"])
            
        if "stochastic_d_period" in config:
            self.stoch_d.setValue(config["stochastic_d_period"])
            
        if "stochastic_smooth" in config:
            self.stoch_smooth.setValue(config["stochastic_smooth"])
            
        # Set liquidity settings
        if "volume_spike_threshold" in config:
            self.vol_spike.setValue(config["volume_spike_threshold"])
            
        if "liquidity_min_volume" in config:
            self.liquidity_min_vol.setValue(config["liquidity_min_volume"])
            
        if "liquidity_min_oi" in config:
            self.min_oi.setValue(config["liquidity_min_oi"])
            
        if "liquidity_max_spread" in config:
            self.max_spread.setValue(config["liquidity_max_spread"])
            
        # Set sector settings
        if "sector_etfs" in config:
            if isinstance(config["sector_etfs"], list):
                self.sector_etfs.setText(", ".join(config["sector_etfs"]))
            else:
                self.sector_etfs.setText(str(config["sector_etfs"]))
                
        if "sector_weight_threshold" in config:
            self.sector_threshold.setValue(config["sector_weight_threshold"])
            print(f"[DEBUG] Set sector threshold spinner to: {config['sector_weight_threshold']}")
            
        if "sector_weights" in config:
            weights = config["sector_weights"]
            if "XLK" in weights:
                self.xlk_weight.setValue(weights["XLK"])
            if "XLF" in weights:
                self.xlf_weight.setValue(weights["XLF"])
            if "XLV" in weights:
                self.xlv_weight.setValue(weights["XLV"])
            if "XLY" in weights:
                self.xly_weight.setValue(weights["XLY"])

        
        # Set Mag7 settings
        if "mag7_stocks" in config:
            if isinstance(config["mag7_stocks"], list):
                self.mag7_stocks.setText(", ".join(config["mag7_stocks"]))
            else:
                self.mag7_stocks.setText(str(config["mag7_stocks"]))
        
        if "mag7_threshold" in config:
            self.mag7_threshold.setValue(config["mag7_threshold"])
        
        if "mag7_price_change_threshold" in config:
            self.mag7_price_change.setValue(config["mag7_price_change_threshold"])
        
        if "mag7_min_aligned" in config:
            self.mag7_min_aligned.setValue(config["mag7_min_aligned"])


    def create_section(self, title, content_widget):
        """Create a styled section with title and content"""
        section = QGroupBox(title)
        section.setStyleSheet("""
            QGroupBox {
                background-color: white;
                border: 1px solid #ecf0f1;
                border-radius: 5px;
                margin-top: 15px;
                font-weight: bold;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: #3498db;
            }
        """)
        
        layout = QVBoxLayout()
        layout.addWidget(content_widget)
        section.setLayout(layout)
        
        return section
        
    def create_basic_settings(self):
        """Create widget for basic settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
            QSpinBox::up-button, QDoubleSpinBox::up-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-bottom: 1px solid #bdc3c7;
                border-top-right-radius: 3px;
                background-color: #ecf0f1;
            }
            QSpinBox::down-button, QDoubleSpinBox::down-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-top: 1px solid #bdc3c7;
                border-bottom-right-radius: 3px;
                background-color: #ecf0f1;
            }
            QComboBox::drop-down {
                border-left: 1px solid #bdc3c7;
                width: 20px;
            }
        """
        
        # Ticker list
        ticker_label = QLabel("Ticker List:")
        ticker_label.setStyleSheet(label_style)
        
        ticker_layout = QHBoxLayout()
        self.ticker_list = QLineEdit("SPY, QQQ, TSLA, AAPL")
        self.ticker_list.setStyleSheet(input_style)
        ticker_info = self.create_info_button("Comma-separated list of stock symbols to trade. These are the underlying stocks for which the bot will trade options.")
        ticker_layout.addWidget(self.ticker_list)
        ticker_layout.addWidget(ticker_info)
        form.addRow(ticker_label, ticker_layout)
        
        # Contracts per trade
        contracts_label = QLabel("Contracts per Trade:")
        contracts_label.setStyleSheet(label_style)
        
        contracts_layout = QHBoxLayout()
        self.contracts = QSpinBox()
        self.contracts.setRange(1, 100)
        self.contracts.setValue(1)
        self.contracts.setStyleSheet(input_style)
        contracts_info = self.create_info_button("Number of option contracts to trade per position. Start with 1 contract to manage risk.")
        contracts_layout.addWidget(self.contracts)
        contracts_layout.addWidget(contracts_info)
        form.addRow(contracts_label, contracts_layout)
        
        # Trailing stop method
        trailing_label = QLabel("Trailing Stop Method:")
        trailing_label.setStyleSheet(label_style)
        
        trailing_layout = QHBoxLayout()
        self.trailing_stop = QComboBox()
        self.trailing_stop.addItems([
            "Heiken Ashi Candle Trail (1-3 candle lookback)",
            "EMA Trail (e.g., EMA(9) trailing stop)",
            "% Price Trail (e.g., 1.5% below current price)",
            "ATR-Based Trail (1.5x ATR)",
            "Fixed Tick/Point Trail (custom value)"
        ])
        self.trailing_stop.setStyleSheet(input_style)
        trailing_info = self.create_info_button("Method for adjusting stop loss as the trade moves in your favor. Heiken Ashi is recommended for trending markets.")
        trailing_layout.addWidget(self.trailing_stop)
        trailing_layout.addWidget(trailing_info)
        form.addRow(trailing_label, trailing_layout)

        # Auto-trading checkbox
        auto_trading_label = QLabel("Enable Auto-Trading:")
        auto_trading_label.setStyleSheet(label_style)

        auto_trading_layout = QHBoxLayout()
        self.auto_trading = QCheckBox("Automatically place real trades")
        self.auto_trading.setChecked(False)  # Default to disabled for safety
        self.auto_trading.setStyleSheet("""
            QCheckBox {
                font-size: 14px;
                color: #e74c3c;
                font-weight: bold;
            }
        """)
        auto_trading_info = self.create_info_button("⚠️ WARNING: When enabled, the bot will place REAL trades with REAL money! Only enable this after thorough testing.")
        auto_trading_layout.addWidget(self.auto_trading)
        auto_trading_layout.addWidget(auto_trading_info)
        form.addRow(auto_trading_label, auto_trading_layout)
        
        warning_label = QLabel("⚠️ WARNING: When enabled, the bot will place REAL trades with REAL money!")
        warning_label.setStyleSheet("""
            QLabel {
                color: #e74c3c;
                font-weight: bold;
                font-size: 12px;
                padding: 5px;
                background-color: #ffeeee;
                border: 1px solid #e74c3c;
                border-radius: 3px;
            }
        """)
        warning_label.setWordWrap(True)
        form.addRow("", warning_label)

        widget.setLayout(form)
        return widget
        
    def create_time_settings(self):
        """Create widget for time-related settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QTimeEdit, QSpinBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QTimeEdit:focus, QSpinBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
            QSpinBox::up-button, QTimeEdit::up-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-bottom: 1px solid #bdc3c7;
                border-top-right-radius: 3px;
                background-color: #ecf0f1;
            }
            QSpinBox::down-button, QTimeEdit::down-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-top: 1px solid #bdc3c7;
                border-bottom-right-radius: 3px;
                background-color: #ecf0f1;
            }
        """
        
        # No-trade window
        no_trade_label = QLabel("No-Trade Window (minutes):")
        no_trade_label.setStyleSheet(label_style)
        
        no_trade_layout = QHBoxLayout()
        self.no_trade_minutes = QSpinBox()
        self.no_trade_minutes.setRange(0, 60)
        self.no_trade_minutes.setValue(3)
        self.no_trade_minutes.setStyleSheet(input_style)
        no_trade_info = self.create_info_button("Number of minutes after market open to skip trading. Helps avoid volatile opening period.")
        no_trade_layout.addWidget(self.no_trade_minutes)
        no_trade_layout.addWidget(no_trade_info)
        form.addRow(no_trade_label, no_trade_layout)
        
        # Auto-close setting
        auto_close_label = QLabel("Auto-Close (minutes):")
        auto_close_label.setStyleSheet(label_style)
        
        auto_close_layout = QHBoxLayout()
        self.auto_close_minutes = QSpinBox()
        self.auto_close_minutes.setRange(1, 60)
        self.auto_close_minutes.setValue(15)
        self.auto_close_minutes.setStyleSheet(input_style)
        auto_close_info = self.create_info_button("Minutes before market close to automatically exit all positions. Prevents overnight risk.")
        auto_close_layout.addWidget(self.auto_close_minutes)
        auto_close_layout.addWidget(auto_close_info)
        form.addRow(auto_close_label, auto_close_layout)
        
        # Cutoff time for new entries
        cutoff_label = QLabel("Cutoff Time for New Entries:")
        cutoff_label.setStyleSheet(label_style)
        
        cutoff_layout = QHBoxLayout()
        self.cutoff_time = QTimeEdit()
        self.cutoff_time.setTime(QTime(15, 15))  # 3:15 PM
        self.cutoff_time.setStyleSheet(input_style)
        cutoff_info = self.create_info_button("No new trades will be opened after this time (Eastern Time). Default is 3:15 PM ET.")
        cutoff_layout.addWidget(self.cutoff_time)
        cutoff_layout.addWidget(cutoff_info)
        form.addRow(cutoff_label, cutoff_layout)
        
        # Time-based exit failsafe
        failsafe_label = QLabel("Time-Based Exit Failsafe (min):")
        failsafe_label.setStyleSheet(label_style)
        
        failsafe_layout = QHBoxLayout()
        self.failsafe_minutes = QSpinBox()
        self.failsafe_minutes.setRange(1, 120)
        self.failsafe_minutes.setValue(20)
        self.failsafe_minutes.setStyleSheet(input_style)
        failsafe_info = self.create_info_button("Maximum time to hold a position. Position will be closed after this duration regardless of P&L.")
        failsafe_layout.addWidget(self.failsafe_minutes)
        failsafe_layout.addWidget(failsafe_info)
        form.addRow(failsafe_label, failsafe_layout)
        
        widget.setLayout(form)
        return widget
        
    def create_indicator_settings(self):
        """Create widget for technical indicator settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QSpinBox, QDoubleSpinBox, QCheckBox {
                padding: 8px;
                font-size: 14px;
                min-width: 200px;
            }
            QSpinBox, QDoubleSpinBox {
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
            }
        """
        
        # EMA settings
        ema_label = QLabel("EMA Period:")
        ema_label.setStyleSheet(label_style)
        
        ema_layout = QHBoxLayout()
        self.ema_value = QSpinBox()
        self.ema_value.setRange(5, 200)
        self.ema_value.setValue(15)
        self.ema_value.setStyleSheet(input_style)
        ema_info = self.create_info_button("Exponential Moving Average period for trend confirmation. Lower values are more responsive to price changes.")
        ema_layout.addWidget(self.ema_value)
        ema_layout.addWidget(ema_info)
        form.addRow(ema_label, ema_layout)
        
        # ADX filter
        adx_filter_label = QLabel("ADX Filter:")
        adx_filter_label.setStyleSheet(label_style)
        
        adx_filter_layout = QHBoxLayout()
        self.adx_filter = QCheckBox("Use ADX Filter")
        self.adx_filter.setChecked(True)
        self.adx_filter.setStyleSheet(input_style)
        adx_filter_info = self.create_info_button("Enable ADX (Average Directional Index) filter to avoid trading in choppy markets.")
        adx_filter_layout.addWidget(self.adx_filter)
        adx_filter_layout.addWidget(adx_filter_info)
        form.addRow(adx_filter_label, adx_filter_layout)
        
        # ADX value
        adx_value_label = QLabel("ADX Minimum Value:")
        adx_value_label.setStyleSheet(label_style)
        
        adx_value_layout = QHBoxLayout()
        self.adx_value = QSpinBox()
        self.adx_value.setRange(10, 50)
        self.adx_value.setValue(20)
        self.adx_value.setStyleSheet(input_style)
        adx_value_info = self.create_info_button("Minimum ADX value required for trading. Values > 20 indicate trending market.")
        adx_value_layout.addWidget(self.adx_value)
        adx_value_layout.addWidget(adx_value_info)
        form.addRow(adx_value_label, adx_value_layout)
        
        # News filter
        news_filter_label = QLabel("News Filter:")
        news_filter_label.setStyleSheet(label_style)
        
        news_filter_layout = QHBoxLayout()
        self.news_filter = QCheckBox("Block Trades During News Events")
        self.news_filter.setStyleSheet(input_style)
        news_filter_info = self.create_info_button("When enabled, prevents trading during scheduled economic news events (requires news API).")
        news_filter_layout.addWidget(self.news_filter)
        news_filter_layout.addWidget(news_filter_info)
        form.addRow(news_filter_label, news_filter_layout)
        
        widget.setLayout(form)
        return widget
        
    def create_compression_settings(self):
        """Create widget for compression detection settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QDoubleSpinBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QDoubleSpinBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
            QDoubleSpinBox::up-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-bottom: 1px solid #bdc3c7;
                border-top-right-radius: 3px;
                background-color: #ecf0f1;
            }
            QDoubleSpinBox::down-button {
                width: 16px;
                border-left: 1px solid #bdc3c7;
                border-top: 1px solid #bdc3c7;
                border-bottom-right-radius: 3px;
                background-color: #ecf0f1;
            }
        """
        
        # Bollinger Band Width
        bb_width_label = QLabel("BB Width Threshold:")
        bb_width_label.setStyleSheet(label_style)
        
        bb_width_layout = QHBoxLayout()
        self.bb_width = QDoubleSpinBox()
        self.bb_width.setRange(0.01, 0.2)
        self.bb_width.setSingleStep(0.01)
        self.bb_width.setValue(0.05)
        self.bb_width.setDecimals(3)
        self.bb_width.setStyleSheet(input_style)
        bb_width_info = self.create_info_button("Bollinger Band width threshold for compression detection. Lower values indicate tighter compression.")
        bb_width_layout.addWidget(self.bb_width)
        bb_width_layout.addWidget(bb_width_info)
        form.addRow(bb_width_label, bb_width_layout)
        
        # Donchian Channel Contraction
        donchian_label = QLabel("Donchian Contraction:")
        donchian_label.setStyleSheet(label_style)
        
        donchian_layout = QHBoxLayout()
        self.donchian_threshold = QDoubleSpinBox()
        self.donchian_threshold.setRange(0.1, 1.0)
        self.donchian_threshold.setSingleStep(0.1)
        self.donchian_threshold.setValue(0.6)
        self.donchian_threshold.setDecimals(1)
        self.donchian_threshold.setStyleSheet(input_style)
        donchian_info = self.create_info_button("Donchian channel contraction threshold. Range must be less than this % of average range.")
        donchian_layout.addWidget(self.donchian_threshold)
        donchian_layout.addWidget(donchian_info)
        form.addRow(donchian_label, donchian_layout)
        
        # Volume Squeeze
        volume_squeeze_label = QLabel("Volume Squeeze:")
        volume_squeeze_label.setStyleSheet(label_style)
        
        volume_squeeze_layout = QHBoxLayout()
        self.volume_squeeze = QDoubleSpinBox()
        self.volume_squeeze.setRange(0.1, 1.0)
        self.volume_squeeze.setSingleStep(0.1)
        self.volume_squeeze.setValue(0.3)
        self.volume_squeeze.setDecimals(1)
        self.volume_squeeze.setStyleSheet(input_style)
        volume_squeeze_info = self.create_info_button("Volume squeeze threshold. Recent volume must be less than this % of average volume.")
        volume_squeeze_layout.addWidget(self.volume_squeeze)
        volume_squeeze_layout.addWidget(volume_squeeze_info)
        form.addRow(volume_squeeze_label, volume_squeeze_layout)
        
        widget.setLayout(form)
        return widget

    def create_stop_loss_settings(self):
        """Create widget for stop loss settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QComboBox, QDoubleSpinBox, QCheckBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QComboBox:focus, QDoubleSpinBox:focus, QCheckBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
        """
        
        # Stop loss method selection
        sl_method_label = QLabel("Stop Loss Method:")
        sl_method_label.setStyleSheet(label_style)
        
        sl_method_layout = QHBoxLayout()
        self.sl_method = QComboBox()
        self.sl_method.addItems([
            "ATR Multiple",
            "Structure-based",
            "Fixed Percentage"
        ])
        self.sl_method.setStyleSheet(input_style)
        self.sl_method.currentIndexChanged.connect(self.on_sl_method_changed)
        sl_method_info = self.create_info_button("Method for setting initial stop loss. ATR adapts to volatility, Structure uses swing points, Fixed uses percentage.")
        sl_method_layout.addWidget(self.sl_method)
        sl_method_layout.addWidget(sl_method_info)
        form.addRow(sl_method_label, sl_method_layout)
        
        # ATR multiple input
        atr_multiple_label = QLabel("ATR Multiple:")
        atr_multiple_label.setStyleSheet(label_style)
        
        atr_multiple_layout = QHBoxLayout()
        self.atr_multiple = QDoubleSpinBox()
        self.atr_multiple.setRange(0.5, 5.0)
        self.atr_multiple.setSingleStep(0.1)
        self.atr_multiple.setValue(1.5)
        self.atr_multiple.setStyleSheet(input_style)
        atr_multiple_info = self.create_info_button("Multiplier for ATR-based stop loss. Higher values give wider stops. 1.5 is recommended.")
        atr_multiple_layout.addWidget(self.atr_multiple)
        atr_multiple_layout.addWidget(atr_multiple_info)
        form.addRow(atr_multiple_label, atr_multiple_layout)
        
        # Fixed percentage input
        fixed_pct_label = QLabel("Fixed Stop Loss (%):")
        fixed_pct_label.setStyleSheet(label_style)
        
        fixed_pct_layout = QHBoxLayout()
        self.fixed_percentage = QDoubleSpinBox()
        self.fixed_percentage.setRange(0.1, 10.0)
        self.fixed_percentage.setSingleStep(0.1)
        self.fixed_percentage.setValue(1.0)
        self.fixed_percentage.setSuffix("%")
        self.fixed_percentage.setStyleSheet(input_style)
        fixed_pct_info = self.create_info_button("Fixed percentage stop loss from entry price. 1% is conservative, 2-3% for volatile stocks.")
        fixed_pct_layout.addWidget(self.fixed_percentage)
        fixed_pct_layout.addWidget(fixed_pct_info)
        form.addRow(fixed_pct_label, fixed_pct_layout)
        
        # Initially disable the one that's not selected
        self.on_sl_method_changed()
        
        widget.setLayout(form)
        return widget
    


    def create_sector_settings(self):
        """Create widget for sector/strategy configuration with dynamic switching"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QLineEdit, QSpinBox, QComboBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
        """
        
        # Strategy Selection
        strategy_label = QLabel("Trading Strategy:")
        strategy_label.setStyleSheet(label_style + " font-weight: bold;")
        
        strategy_layout = QHBoxLayout()
        self.strategy_selector = QComboBox()
        self.strategy_selector.addItems(["Sector Alignment Strategy", "Magnificent 7 (Mag7) Strategy"])
        self.strategy_selector.setStyleSheet(input_style)
        self.strategy_selector.currentTextChanged.connect(self.on_strategy_changed)
        
        strategy_info = self.create_info_button("Select between Sector Alignment (tracks sector ETFs) or Mag7 (tracks top 7 tech stocks) strategy")
        strategy_layout.addWidget(self.strategy_selector)
        strategy_layout.addWidget(strategy_info)
        form.addRow(strategy_label, strategy_layout)
        
        # Add separator
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        separator.setStyleSheet("margin: 10px 0;")
        form.addRow(separator)
        
        # Create stacked widget for dynamic content
        self.strategy_stack = QStackedWidget()
        
        # === SECTOR ALIGNMENT SETTINGS ===
        sector_widget = QWidget()
        sector_form = QFormLayout(sector_widget)
        sector_form.setContentsMargins(0, 0, 0, 0)
        sector_form.setSpacing(15)
        
        # Sector ETFs
        sector_etfs_label = QLabel("Sector ETFs:")
        sector_etfs_label.setStyleSheet(label_style)
        
        sector_etfs_layout = QHBoxLayout()
        self.sector_etfs = QLineEdit("XLK, XLF, XLV, XLY")
        self.sector_etfs.setStyleSheet(input_style)
        sector_etfs_info = self.create_info_button("Sector ETFs to monitor for alignment. Default are Tech, Financials, Health, Consumer.")
        sector_etfs_layout.addWidget(self.sector_etfs)
        sector_etfs_layout.addWidget(sector_etfs_info)
        sector_form.addRow(sector_etfs_label, sector_etfs_layout)
        
        # Sector weight threshold
        sector_threshold_label = QLabel("Sector Threshold (%):")
        sector_threshold_label.setStyleSheet(label_style)
        
        sector_threshold_layout = QHBoxLayout()
        self.sector_threshold = QSpinBox()
        self.sector_threshold.setRange(20, 60)
        self.sector_threshold.setValue(43)
        self.sector_threshold.setStyleSheet(input_style)
        sector_threshold_info = self.create_info_button("Minimum combined weight of aligned sectors required for trading. 43% ensures majority alignment.")
        sector_threshold_layout.addWidget(self.sector_threshold)
        sector_threshold_layout.addWidget(sector_threshold_info)
        sector_form.addRow(sector_threshold_label, sector_threshold_layout)
        
        # Individual sector weights
        sector_weights_label = QLabel("Sector Weights:")
        sector_weights_label.setStyleSheet(label_style + " font-weight: bold;")
        sector_form.addRow(sector_weights_label, QLabel(""))
        
        # XLK weight
        xlk_label = QLabel("  XLK (Tech) Weight:")
        xlk_label.setStyleSheet(label_style)
        
        xlk_layout = QHBoxLayout()
        self.xlk_weight = QSpinBox()
        self.xlk_weight.setRange(0, 100)
        self.xlk_weight.setValue(32)
        self.xlk_weight.setSuffix("%")
        self.xlk_weight.setStyleSheet(input_style)
        xlk_info = self.create_info_button("Technology sector weight in S&P 500. Default is 32%.")
        xlk_layout.addWidget(self.xlk_weight)
        xlk_layout.addWidget(xlk_info)
        sector_form.addRow(xlk_label, xlk_layout)
        
        # XLF weight
        xlf_label = QLabel("  XLF (Financials) Weight:")
        xlf_label.setStyleSheet(label_style)
        
        xlf_layout = QHBoxLayout()
        self.xlf_weight = QSpinBox()
        self.xlf_weight.setRange(0, 100)
        self.xlf_weight.setValue(14)
        self.xlf_weight.setSuffix("%")
        self.xlf_weight.setStyleSheet(input_style)
        xlf_info = self.create_info_button("Financials sector weight in S&P 500. Default is 14%.")
        xlf_layout.addWidget(self.xlf_weight)
        xlf_layout.addWidget(xlf_info)
        sector_form.addRow(xlf_label, xlf_layout)
        
        # XLV weight
        xlv_label = QLabel("  XLV (Healthcare) Weight:")
        xlv_label.setStyleSheet(label_style)
        
        xlv_layout = QHBoxLayout()
        self.xlv_weight = QSpinBox()
        self.xlv_weight.setRange(0, 100)
        self.xlv_weight.setValue(11)
        self.xlv_weight.setSuffix("%")
        self.xlv_weight.setStyleSheet(input_style)
        xlv_info = self.create_info_button("Healthcare sector weight in S&P 500. Default is 11%.")
        xlv_layout.addWidget(self.xlv_weight)
        xlv_layout.addWidget(xlv_info)
        sector_form.addRow(xlv_label, xlv_layout)
        
        # XLY weight
        xly_label = QLabel("  XLY (Consumer) Weight:")
        xly_label.setStyleSheet(label_style)
        
        xly_layout = QHBoxLayout()
        self.xly_weight = QSpinBox()
        self.xly_weight.setRange(0, 100)
        self.xly_weight.setValue(11)
        self.xly_weight.setSuffix("%")
        self.xly_weight.setStyleSheet(input_style)
        xly_info = self.create_info_button("Consumer Discretionary sector weight in S&P 500. Default is 11%.")
        xly_layout.addWidget(self.xly_weight)
        xly_layout.addWidget(xly_info)
        sector_form.addRow(xly_label, xly_layout)
        
        # === MAG7 SETTINGS ===
        mag7_widget = QWidget()
        mag7_form = QFormLayout(mag7_widget)
        mag7_form.setContentsMargins(0, 0, 0, 0)
        mag7_form.setSpacing(15)
        
        # Mag7 stocks list
        mag7_stocks_label = QLabel("Mag7 Stocks:")
        mag7_stocks_label.setStyleSheet(label_style)
        
        mag7_stocks_layout = QHBoxLayout()
        self.mag7_stocks = QLineEdit("AAPL, MSFT, AMZN, NVDA, GOOG, TSLA, META")
        self.mag7_stocks.setStyleSheet(input_style)
        mag7_stocks_info = self.create_info_button("The Magnificent 7 stocks to monitor. You can customize this list.")
        mag7_stocks_layout.addWidget(self.mag7_stocks)
        mag7_stocks_layout.addWidget(mag7_stocks_info)
        mag7_form.addRow(mag7_stocks_label, mag7_stocks_layout)
        
        # Mag7 threshold
        mag7_threshold_label = QLabel("Mag7 Alignment Threshold (%):")
        mag7_threshold_label.setStyleSheet(label_style)
        
        mag7_threshold_layout = QHBoxLayout()
        self.mag7_threshold = QSpinBox()
        self.mag7_threshold.setRange(40, 80)
        self.mag7_threshold.setValue(60)
        self.mag7_threshold.setSuffix("%")
        self.mag7_threshold.setStyleSheet(input_style)
        mag7_threshold_info = self.create_info_button("Percentage of Mag7 stocks that must be aligned (bullish/bearish) to trigger trades. Default is 60%.")
        mag7_threshold_layout.addWidget(self.mag7_threshold)
        mag7_threshold_layout.addWidget(mag7_threshold_info)
        mag7_form.addRow(mag7_threshold_label, mag7_threshold_layout)
        
        # Individual stock analysis settings
        mag7_analysis_label = QLabel("Analysis Settings:")
        mag7_analysis_label.setStyleSheet(label_style + " font-weight: bold;")
        mag7_form.addRow(mag7_analysis_label, QLabel(""))
        
        # Price change threshold
        price_change_label = QLabel("  Price Change Threshold (%):")
        price_change_label.setStyleSheet(label_style)
        
        price_change_layout = QHBoxLayout()
        self.mag7_price_change = QDoubleSpinBox()
        self.mag7_price_change.setRange(0.05, 1.0)
        self.mag7_price_change.setSingleStep(0.05)
        self.mag7_price_change.setValue(0.1)
        self.mag7_price_change.setSuffix("%")
        self.mag7_price_change.setDecimals(2)
        self.mag7_price_change.setStyleSheet(input_style)
        price_change_info = self.create_info_button("Minimum price change to consider a stock bullish/bearish. Default is 0.1%.")
        price_change_layout.addWidget(self.mag7_price_change)
        price_change_layout.addWidget(price_change_info)
        mag7_form.addRow(price_change_label, price_change_layout)
        
        # Add minimum aligned stocks
        min_aligned_label = QLabel("  Minimum Aligned Stocks:")
        min_aligned_label.setStyleSheet(label_style)
        
        min_aligned_layout = QHBoxLayout()
        self.mag7_min_aligned = QSpinBox()
        self.mag7_min_aligned.setRange(3, 7)
        self.mag7_min_aligned.setValue(5)
        self.mag7_min_aligned.setStyleSheet(input_style)
        min_aligned_info = self.create_info_button("Minimum number of Mag7 stocks that must be aligned. Default is 5 out of 7.")
        min_aligned_layout.addWidget(self.mag7_min_aligned)
        min_aligned_layout.addWidget(min_aligned_info)
        mag7_form.addRow(min_aligned_label, min_aligned_layout)
        
        # Add widgets to stack
        self.strategy_stack.addWidget(sector_widget)
        self.strategy_stack.addWidget(mag7_widget)
        
        # Add stack to form
        form.addRow(self.strategy_stack)
        
        widget.setLayout(form)
        
        # Set initial state
        self.on_strategy_changed(self.strategy_selector.currentText())
        
        return widget

    def on_strategy_changed(self, strategy_text):
        """Handle strategy selection change"""
        if "Sector" in strategy_text:
            self.strategy_stack.setCurrentIndex(0)
            # Update any related UI elements if needed
            dashboard = self.window().get_dashboard() if hasattr(self.window(), 'get_dashboard') else None
            if dashboard:
                dashboard.update_log(f"Switched to Sector Alignment Strategy")
        else:  # Mag7
            self.strategy_stack.setCurrentIndex(1)
            # Update any related UI elements if needed
            dashboard = self.window().get_dashboard() if hasattr(self.window(), 'get_dashboard') else None
            if dashboard:
                dashboard.update_log(f"Switched to Magnificent 7 Strategy")


    def create_stochastic_settings(self):
        """Create widget for stochastic oscillator settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QSpinBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QSpinBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
        """
        
        # Stochastic K period
        stoch_k_label = QLabel("Stochastic K Period:")
        stoch_k_label.setStyleSheet(label_style)
        
        stoch_k_layout = QHBoxLayout()
        self.stoch_k = QSpinBox()
        self.stoch_k.setRange(1, 20)
        self.stoch_k.setValue(5)
        self.stoch_k.setStyleSheet(input_style)
        stoch_k_info = self.create_info_button("K period for Stochastic Oscillator. Barry Burns method uses 5.")
        stoch_k_layout.addWidget(self.stoch_k)
        stoch_k_layout.addWidget(stoch_k_info)
        form.addRow(stoch_k_label, stoch_k_layout)
        
        # Stochastic D period
        stoch_d_label = QLabel("Stochastic D Period:")
        stoch_d_label.setStyleSheet(label_style)
        
        stoch_d_layout = QHBoxLayout()
        self.stoch_d = QSpinBox()
        self.stoch_d.setRange(1, 20)
        self.stoch_d.setValue(3)
        self.stoch_d.setStyleSheet(input_style)
        stoch_d_info = self.create_info_button("D period for Stochastic Oscillator (signal line). Barry Burns method uses 3.")
        stoch_d_layout.addWidget(self.stoch_d)
        stoch_d_layout.addWidget(stoch_d_info)
        form.addRow(stoch_d_label, stoch_d_layout)
        
        # Stochastic Smooth
        stoch_smooth_label = QLabel("Stochastic Smooth:")
        stoch_smooth_label.setStyleSheet(label_style)
        
        stoch_smooth_layout = QHBoxLayout()
        self.stoch_smooth = QSpinBox()
        self.stoch_smooth.setRange(1, 10)
        self.stoch_smooth.setValue(2)
        self.stoch_smooth.setStyleSheet(input_style)
        stoch_smooth_info = self.create_info_button("Smoothing factor for Stochastic Oscillator. Barry Burns method uses 2.")
        stoch_smooth_layout.addWidget(self.stoch_smooth)
        stoch_smooth_layout.addWidget(stoch_smooth_info)
        form.addRow(stoch_smooth_label, stoch_smooth_layout)
        
        widget.setLayout(form)
        return widget
    
    def create_liquidity_settings(self):
        """Create widget for liquidity filter settings"""
        widget = QWidget()
        form = QFormLayout()
        form.setContentsMargins(10, 10, 10, 10)
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignRight)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QSpinBox, QDoubleSpinBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 200px;
            }
            QSpinBox:focus, QDoubleSpinBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
        """
        
        # Volume Spike Threshold
        vol_spike_label = QLabel("Volume Spike Threshold:")
        vol_spike_label.setStyleSheet(label_style)
        
        vol_spike_layout = QHBoxLayout()
        self.vol_spike = QDoubleSpinBox()
        self.vol_spike.setRange(1.1, 5.0)
        self.vol_spike.setSingleStep(0.1)
        self.vol_spike.setValue(1.5)
        self.vol_spike.setDecimals(1)
        self.vol_spike.setStyleSheet(input_style)
        vol_spike_info = self.create_info_button("Multiple of average volume required for breakout confirmation. 1.5x means 50% above average.")
        vol_spike_layout.addWidget(self.vol_spike)
        vol_spike_layout.addWidget(vol_spike_info)
        form.addRow(vol_spike_label, vol_spike_layout)
        
        # Liquidity Settings
        liquidity_min_vol_label = QLabel("Min Daily Volume:")
        liquidity_min_vol_label.setStyleSheet(label_style)
        
        liquidity_min_vol_layout = QHBoxLayout()
        self.liquidity_min_vol = QSpinBox()
        self.liquidity_min_vol.setRange(100000, 10000000)
        self.liquidity_min_vol.setSingleStep(100000)
        self.liquidity_min_vol.setValue(1000000)  # 1M
        self.liquidity_min_vol.setStyleSheet(input_style)
        liquidity_min_vol_info = self.create_info_button("Minimum daily volume required for underlying stock. Ensures sufficient liquidity.")
        liquidity_min_vol_layout.addWidget(self.liquidity_min_vol)
        liquidity_min_vol_layout.addWidget(liquidity_min_vol_info)
        form.addRow(liquidity_min_vol_label, liquidity_min_vol_layout)
        
        # Min Open Interest
        min_oi_label = QLabel("Min Open Interest:")
        min_oi_label.setStyleSheet(label_style)
        
        min_oi_layout = QHBoxLayout()
        self.min_oi = QSpinBox()
        self.min_oi.setRange(50, 5000)
        self.min_oi.setSingleStep(50)
        self.min_oi.setValue(500)
        self.min_oi.setStyleSheet(input_style)
        min_oi_info = self.create_info_button("Minimum open interest for option contracts. Higher values ensure better liquidity.")
        min_oi_layout.addWidget(self.min_oi)
        min_oi_layout.addWidget(min_oi_info)
        form.addRow(min_oi_label, min_oi_layout)
        
        # Max Bid/Ask Spread
        max_spread_label = QLabel("Max Bid/Ask Spread ($):")
        max_spread_label.setStyleSheet(label_style)
        
        max_spread_layout = QHBoxLayout()
        self.max_spread = QDoubleSpinBox()
        self.max_spread.setRange(0.01, 1.0)
        self.max_spread.setSingleStep(0.01)
        self.max_spread.setValue(0.10)
        self.max_spread.setDecimals(2)
        self.max_spread.setStyleSheet(input_style)
        max_spread_info = self.create_info_button("Maximum allowed bid-ask spread for option contracts. Lower is better for fills.")
        max_spread_layout.addWidget(self.max_spread)
        max_spread_layout.addWidget(max_spread_info)
        form.addRow(max_spread_label, max_spread_layout)
        
        widget.setLayout(form)
        return widget

    def on_sl_method_changed(self):
        """Handle stop loss method change"""
        current_method = self.sl_method.currentText()
        
        # Enable/disable appropriate inputs based on selection
        self.atr_multiple.setEnabled(current_method == "ATR Multiple")
        self.fixed_percentage.setEnabled(current_method == "Fixed Percentage")



class BacktestWidget(QWidget):
    """Widget for running backtests with enhanced styling"""
    run_backtest_requested = pyqtSignal(dict)
    
    def __init__(self):
        super().__init__()
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#f5f5f5"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)
        self.initUI()
        
    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # Header section
        header_layout = QVBoxLayout()
        title = QLabel("Backtest Your Strategy")
        title.setStyleSheet("""
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        """)
        description = QLabel("Test your trading strategy against historical data")
        description.setStyleSheet("""
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 15px;
        """)
        
        header_layout.addWidget(title)
        header_layout.addWidget(description)
        layout.addLayout(header_layout)
        
        # Parameters panel
        params_card = QFrame()
        params_card.setStyleSheet("""
            QFrame {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #ddd;
            }
        """)
        params_layout = QVBoxLayout()
        params_layout.setContentsMargins(20, 20, 20, 20)
        
        # Parameters form
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        form.setSpacing(15)
        
        # Style for labels and input widgets
        label_style = """
            QLabel {
                font-size: 14px;
                color: #2c3e50;
                padding-right: 10px;
            }
        """
        
        input_style = """
            QLineEdit, QComboBox {
                padding: 8px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #f9f9f9;
                selection-background-color: #3498db;
                font-size: 14px;
                min-width: 250px;
            }
            QLineEdit:focus, QComboBox:focus {
                border: 1px solid #3498db;
                background-color: white;
            }
            QComboBox::drop-down {
                border-left: 1px solid #bdc3c7;
                width: 20px;
            }
        """
        
        # Data source selector
        source_label = QLabel("Data Source:")
        source_label.setStyleSheet(label_style)

        self.data_source = QComboBox()
        self.data_source.addItems(["TradeStation", "TastyTrade", "YFinance"])
        self.data_source.setCurrentText("TradeStation")  # Set TradeStation as default
        self.data_source.setStyleSheet(input_style)
        self.data_source.setToolTip(
            "TradeStation (Default):\n"
            "• 1m data: Up to 40 days\n"
            "• 5m data: Up to 6 months\n"
            "• 15m data: Up to 1 year\n"
            "• 30m data: Up to 2 years\n"
            "• 1h data: Up to 3 years\n"
            "• 1d data: Up to 10 years\n"
            "• Best for professional backtesting\n\n"
            "TastyTrade:\n"
            "• Requires active API connection\n"
            "• Good for options data\n"
            "• Live market data\n\n"
            "YFinance:\n"
            "• 1m data: Only 7 days\n"
            "• 5m/15m data: Only 60 days\n"
            "• Free but limited"
        )
        form.addRow(source_label, self.data_source)
        
        # Connect data source change
        self.data_source.currentTextChanged.connect(self.on_data_source_changed)
        
        # Date range
        start_date_label = QLabel("Start Date:")
        start_date_label.setStyleSheet(label_style)
        
        self.start_date = QLineEdit(datetime.now().replace(month=1, day=1).strftime("%Y-%m-%d"))
        self.start_date.setStyleSheet(input_style)
        form.addRow(start_date_label, self.start_date)
        
        end_date_label = QLabel("End Date:")
        end_date_label.setStyleSheet(label_style)
        
        self.end_date = QLineEdit(datetime.now().strftime("%Y-%m-%d"))
        self.end_date.setStyleSheet(input_style)
        form.addRow(end_date_label, self.end_date)
        
        # Tickers
        tickers_label = QLabel("Tickers:")
        tickers_label.setStyleSheet(label_style)
        
        self.backtest_tickers = QLineEdit("AAPL, NVDA")
        self.backtest_tickers.setStyleSheet(input_style)
        form.addRow(tickers_label, self.backtest_tickers)
        
        # Timeframes
        timeframe_label = QLabel("Timeframe:")
        timeframe_label.setStyleSheet(label_style)
        
        self.timeframes = QComboBox()
        self.timeframes.addItems(["1m", "5m", "15m", "All"])
        self.timeframes.setStyleSheet(input_style)
        form.addRow(timeframe_label, self.timeframes)
        
        params_layout.addLayout(form)
        
        # Button container with proper centering
        button_container = QWidget()
        button_container.setStyleSheet("""
            QWidget {
                background-color: white;
                padding: 10px;
            }
        """)
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 10, 0, 0)
        button_layout.setSpacing(15)
        
        # Add stretch before buttons to center them
        button_layout.addStretch()
        
        # Run backtest button
        self.run_button = QPushButton("Run Backtest")
        self.run_button.setStyleSheet("""
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-weight: bold;
                font-size: 14px;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
            QPushButton:pressed {
                background-color: #1a5276;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.run_button.setCursor(Qt.PointingHandCursor)
        self.run_button.clicked.connect(self.on_run_clicked)
        
        # Export button
        self.export_button = QPushButton("Export Results")
        self.export_button.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 12px 24px;
                font-weight: bold;
                font-size: 14px;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #2ecc71;
            }
            QPushButton:pressed {
                background-color: #1e8449;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.export_button.setCursor(Qt.PointingHandCursor)
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)  # Disabled until results are available
        
        # Add buttons to layout
        button_layout.addWidget(self.run_button)
        button_layout.addWidget(self.export_button)
        
        # Add stretch after buttons to center them
        button_layout.addStretch()
        
        # Add button container to params layout
        params_layout.addWidget(button_container)
        
        params_card.setLayout(params_layout)
        layout.addWidget(params_card)

        
        # Results section with enhanced display
        results_card = QFrame()
        results_card.setStyleSheet("""
            QFrame {
                background-color: white;
                border-radius: 10px;
                border: 1px solid #ddd;
                margin-top: 15px;
            }
        """)
        results_layout = QVBoxLayout()
        results_layout.setContentsMargins(20, 20, 20, 20)
        
        results_header = QLabel("Backtest Results")
        results_header.setStyleSheet("""
            font-size: 18px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 10px;
        """)
        results_layout.addWidget(results_header)
        
        # Create a scroll area for results
        self.results_scroll = QScrollArea()
        self.results_scroll.setWidgetResizable(True)
        self.results_scroll.setStyleSheet("""
            QScrollArea {
                background-color: #f9f9f9;
                border: 1px solid #ecf0f1;
                border-radius: 5px;
            }
        """)
        
        # Results display widget
        self.results_display = QWidget()
        self.results_display_layout = QVBoxLayout(self.results_display)
        self.results_display_layout.setContentsMargins(10, 10, 10, 10)
        self.results_display_layout.setSpacing(20)  # Add spacing between sections

        log_label = QLabel("Backtest Progress Log:")
        log_label.setStyleSheet("""
            font-size: 14px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        """)
        self.results_display_layout.addWidget(log_label)
        
        # Progress text (for live updates)
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMinimumHeight(300)  # Add this line - sets minimum height
        self.results_text.setMaximumHeight(400)
        self.results_text.setStyleSheet("""
            QTextEdit {
                background-color: #2c3e50;
                color: #ecf0f1;
                border: none;
                border-radius: 5px;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 12px;
                padding: 10px;
            }
        """)
        
        # Summary table with better styling and spacing
        self.summary_table = QTableWidget()
        self.summary_table.setStyleSheet("""
            QTableWidget {
                border: none;
                gridline-color: #ecf0f1;
                background-color: white;
            }
            QHeaderView::section {
                background-color: #3498db;
                color: white;
                font-weight: bold;
                padding: 10px 8px;  /* Increased vertical padding */
                border: none;
                min-height: 35px;   /* Set minimum height for headers */
            }
            QTableWidget::item {
                padding: 10px 8px;  /* Increased vertical padding */
                border-bottom: 1px solid #ecf0f1;
                min-height: 30px;   /* Set minimum height for rows */
            }
            QTableWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
        """)
        self.summary_table.setMinimumHeight(300)
        self.summary_table.setMaximumHeight(400)
        self.summary_table.verticalHeader().setDefaultSectionSize(40)  # Set row height
        self.summary_table.horizontalHeader().setMinimumHeight(40)     # Set header height
        
        # Trades table with better styling and spacing
        self.trades_table = QTableWidget()
        self.trades_table.setStyleSheet("""
            QTableWidget {
                border: none;
                gridline-color: #ecf0f1;
                background-color: white;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                font-weight: bold;
                padding: 10px 8px;  /* Increased vertical padding */
                border: none;
                min-height: 35px;   /* Set minimum height for headers */
            }
            QTableWidget::item {
                padding: 10px 8px;  /* Increased vertical padding */
                border-bottom: 1px solid #ecf0f1;
                min-height: 30px;   /* Set minimum height for rows */
            }
            QTableWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
        """)
        self.trades_table.setMinimumHeight(300)
        self.trades_table.setMaximumHeight(400)
        self.trades_table.verticalHeader().setDefaultSectionSize(40)  # Set row height
        self.trades_table.horizontalHeader().setMinimumHeight(40)     # Set header height
        
        # Add widgets to results display with labels
        self.results_display_layout.addWidget(self.results_text)
        
        # Add label for summary
        summary_label = QLabel("Summary Results:")
        summary_label.setStyleSheet("""
            font-size: 16px;
            font-weight: bold;
            color: #2c3e50;
            margin-top: 10px;
            margin-bottom: 5px;
        """)
        self.results_display_layout.addWidget(summary_label)
        self.results_display_layout.addWidget(self.summary_table)
        
        # Add label for trades
        trades_label = QLabel("Trade Details:")
        trades_label.setStyleSheet("""
            font-size: 16px;
            font-weight: bold;
            color: #2c3e50;
            margin-top: 15px;
            margin-bottom: 5px;
        """)
        self.results_display_layout.addWidget(trades_label)
        self.results_display_layout.addWidget(self.trades_table)
        
        self.results_scroll.setWidget(self.results_display)
        results_layout.addWidget(self.results_scroll)
        
        results_card.setLayout(results_layout)
        layout.addWidget(results_card)
        
        self.setLayout(layout)

    def export_results(self):
        """Export backtest results to CSV"""
        try:
            if not hasattr(self, 'current_results') or not self.current_results:
                QMessageBox.warning(self, "No Results", "No backtest results to export.")
                return
                
            file_path, _ = QFileDialog.getSaveFileName(
                self, 
                "Export Backtest Results", 
                f"backtest_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "CSV Files (*.csv)"
            )
            
            if file_path:
                # Create a list to hold all results
                all_results = []
                
                # Process each ticker/period result
                for ticker_period, result in self.current_results.items():
                    if isinstance(result, dict) and "Win Rate" in result:
                        row_data = {
                            "Ticker/Period": ticker_period,
                            "Win Rate (%)": result.get("Win Rate", 0),
                            "Profit Factor": result.get("Profit Factor", 0),
                            "Max Drawdown (%)": result.get("Max Drawdown", 0),
                            "Total Trades": result.get("Total Trades", 0),
                            "Winning Trades": result.get("Winning Trades", 0),
                            "Losing Trades": result.get("Losing Trades", 0),
                            "Gross Profit ($)": result.get("Gross Profit", 0),
                            "Gross Loss ($)": result.get("Gross Loss", 0),
                            "Final Equity ($)": result.get("Final Equity", 10000),
                            "Optimal Trailing Method": result.get("Optimal Trailing Method", "Unknown")
                        }
                        all_results.append(row_data)
                
                # Convert to DataFrame and save
                import pandas as pd
                df = pd.DataFrame(all_results)
                df.to_csv(file_path, index=False)
                
                # Show success message
                self.results_text.append(f"""
                    <div style='background-color: #27ae60; color: white; padding: 8px; border-radius: 5px; margin-top: 10px;'>
                        <b>✓ Results exported successfully!</b><br>
                        File saved to: {file_path}
                    </div>
                """)
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export results: {str(e)}")
            self.results_text.append(f"<div style='color: red;'>Export failed: {str(e)}</div>")
    
    def on_data_source_changed(self, source):
        """Handle data source selection change"""
        try:
            # Update UI based on selected data source
            if source == "TradeStation":
                # Show info about TradeStation data
                info_text = """
                <div style='background-color: #2980b9; color: white; padding: 8px; border-radius: 5px; margin: 5px 0;'>
                    <b>TradeStation Data Selected (Recommended)</b><br>
                    • Professional-grade historical market data<br>
                    • Up to 10 years of daily data<br>
                    • Up to 40 days of 1-minute data<br>
                    • Best accuracy for backtesting<br>
                    • No authentication required for historical data
                </div>
                """
            elif source == "TastyTrade":
                # Show info about TastyTrade data
                info_text = """
                <div style='background-color: #3498db; color: white; padding: 8px; border-radius: 5px; margin: 5px 0;'>
                    <b>TastyTrade Data Selected</b><br>
                    • Uses live market data from TastyTrade API<br>
                    • Requires active TastyTrade connection<br>
                    • Provides accurate historical options data<br>
                    • Good for options-specific backtesting
                </div>
                """
            else:  # YFinance
                info_text = """
                <div style='background-color: #f39c12; color: white; padding: 8px; border-radius: 5px; margin: 5px 0;'>
                    <b>YFinance Data Selected</b><br>
                    • Free data source from Yahoo Finance<br>
                    • No authentication required<br>
                    • Limited to stock data (options simulated)<br>
                    • Good for quick testing
                </div>
                """
            
            # Display info in results text area
            self.results_text.clear()
            self.results_text.append(info_text)
            
            # Additional warnings for data limitations
            if source == "YFinance" and self.timeframes.currentText() == "1m":
                # YFinance only provides 7 days of 1-minute data
                warning_text = """
                <div style='background-color: #e74c3c; color: white; padding: 8px; border-radius: 5px; margin: 5px 0;'>
                    <b>⚠️ Warning:</b> YFinance only provides 7 days of 1-minute data.<br>
                    Consider using TradeStation or 5m/higher timeframes for longer date ranges.
                </div>
                """
                self.results_text.append(warning_text)
            elif source == "TradeStation" and self.timeframes.currentText() == "1m":
                # TradeStation provides 40 days of 1-minute data
                info_text = """
                <div style='background-color: #27ae60; color: white; padding: 8px; border-radius: 5px; margin: 5px 0;'>
                    <b>✓ TradeStation 1-minute data:</b> Up to 40 days available<br>
                    Ensure your date range doesn't exceed this limit for best results.
                </div>
                """
                self.results_text.append(info_text)
                
        except Exception as e:
            self.logger.error(f"Error in data source change handler: {e}")
    
    def on_run_clicked(self):
        """Handle run backtest button click"""
        # Get backtest parameters
        tickers = [t.strip() for t in self.backtest_tickers.text().split(',')]
        params = {
            'tickers': tickers,
            'timeframe': self.timeframes.currentText(),
            'start_date': self.start_date.text(),
            'end_date': self.end_date.text(),
            'data_source': self.data_source.currentText()
        }
        
        # Disable run button while backtest is running
        self.run_button.setEnabled(False)
        self.run_button.setText("Processing...")
        
        # Clear results
        self.results_text.clear()
        
        # Display settings
        self.results_text.append("<h3>Backtest Settings:</h3>")
        self.results_text.append(f"<b>Data Source:</b> {params['data_source']}")
        self.results_text.append(f"<b>Tickers:</b> {', '.join(params['tickers'])}")
        self.results_text.append(f"<b>Timeframe:</b> {params['timeframe']}")
        self.results_text.append(f"<b>Date Range:</b> {params['start_date']} to {params['end_date']}")
        
        # Show progress message
        self.results_text.append("\n<div style='background-color: #3498db; color: white; padding: 10px; border-radius: 5px;'><b>Running backtest...</b> This may take a few moments.</div>")
        
        # Emit signal to run backtest
        self.run_backtest_requested.emit(params)

        
    def display_results(self, results):
        """
        Display backtest results in formatted tables
        
        Args:
            results (dict): Dictionary of backtest results
        """
        try:
            # Clear previous results
            self.summary_table.clear()
            self.trades_table.clear()
            
            # Setup summary table
            metrics = [
                "Ticker/Period", "Win Rate (%)", "Profit Factor", "Max Drawdown (%)",
                "Total Trades", "Winning Trades", "Losing Trades",
                "Gross Profit ($)", "Gross Loss ($)", "Final Equity ($)",
                "Optimal Trailing Method"
            ]
            
            self.summary_table.setColumnCount(len(metrics))
            self.summary_table.setHorizontalHeaderLabels(metrics)
            
            # Populate summary table
            if isinstance(results, dict) and "All Methods" in results:
                # Single ticker result with all methods
                self._display_single_ticker_results(results)
            else:
                # Multiple ticker results
                self._display_multiple_ticker_results(results)
            
            # Auto-resize columns
            self.summary_table.resizeColumnsToContents()
            
            # Display success message
            self.results_text.append("""
                <div style='background-color: #27ae60; color: white; padding: 10px; border-radius: 5px; margin-top: 10px;'>
                    <b>✓ Backtest Completed Successfully!</b><br>
                    Results saved to Backtest_Data directory
                </div>
            """)

            # Display data source used
            data_source_info = f"""
                <div style='background-color: #34495e; color: white; padding: 8px; border-radius: 5px; margin-top: 5px;'>
                    <b>Data Source:</b> {self.data_source.currentText()} - 
                    {"Professional-grade historical data" if self.data_source.currentText() == "TradeStation" else 
                    "Live market data API" if self.data_source.currentText() == "TastyTrade" else 
                    "Free limited data"}
                </div>
            """
            self.results_text.append(data_source_info)
            
        except Exception as e:
            self.results_text.append(f"<div style='color: red;'>Error displaying results: {str(e)}</div>")
            print(f"[✗] Error displaying backtest results: {e}")

    
    def _display_single_ticker_results(self, results):
        """Display results for a single ticker with method comparison"""
        all_methods = results.get("All Methods", {})
        
        # Create rows for each method
        self.summary_table.setRowCount(len(all_methods))
        
        row = 0
        for method, stats in all_methods.items():
            # Method name
            self.summary_table.setItem(row, 0, QTableWidgetItem(method))
            
            # Metrics
            self._set_colored_item(row, 1, f"{stats.get('win_rate', 0):.1f}", stats.get('win_rate', 0) > 50)
            self._set_colored_item(row, 2, f"{stats.get('profit_factor', 0):.2f}", stats.get('profit_factor', 0) > 1)
            self._set_colored_item(row, 3, f"{stats.get('max_drawdown', 0):.1f}", stats.get('max_drawdown', 0) < 10, inverse=True)
            
            # Trade counts
            self.summary_table.setItem(row, 4, QTableWidgetItem(str(stats.get('total_trades', 0))))
            self.summary_table.setItem(row, 5, QTableWidgetItem(str(stats.get('winning_trades', 0))))
            self.summary_table.setItem(row, 6, QTableWidgetItem(str(stats.get('losing_trades', 0))))
            
            # Financial metrics
            self._set_colored_item(row, 7, f"{stats.get('total_profit', 0):.2f}", True)
            self._set_colored_item(row, 8, f"{stats.get('total_loss', 0):.2f}", False)
            self._set_colored_item(row, 9, f"{stats.get('final_equity', 10000):.2f}", 
                                 stats.get('final_equity', 10000) > 10000)
            
            # Optimal method (highlight if it's the best)
            item = QTableWidgetItem(method)
            if method == results.get("Optimal Trailing Method"):
                item.setBackground(QColor("#f1c40f"))  # Gold color for best method
                item.setForeground(QColor("#000000"))
            self.summary_table.setItem(row, 10, item)
            
            row += 1
        
        # Display trades if available
        if "Trades" in results and results["Trades"]:
            self._display_trades(results["Trades"])

    
    def _display_multiple_ticker_results(self, results):
        """Display results for multiple tickers"""
        try:
            if not results:
                return
                
            # Count rows needed (one per ticker/timeframe combination)
            row_count = 0
            for key in results:
                if isinstance(results[key], dict) and "Win Rate" in results[key]:
                    row_count += 1
                    
            if row_count == 0:
                self.results_text.append("<div style='color: orange;'>No valid results to display</div>")
                return
                
            self.summary_table.setRowCount(row_count)
            
            # Track best and worst performers
            best_profit_factor = 0
            best_ticker = ""
            worst_drawdown = 0
            worst_ticker = ""
            total_trades = 0
            total_wins = 0
            
            # Populate table
            row = 0
            for ticker_period, result in results.items():
                if not isinstance(result, dict) or "Win Rate" not in result:
                    continue
                    
                # Ticker/Period
                ticker_item = QTableWidgetItem(ticker_period)
                ticker_item.setFont(QFont("Arial", 10, QFont.Bold))
                self.summary_table.setItem(row, 0, ticker_item)
                
                # Win Rate
                win_rate = result.get("Win Rate", 0)
                self._set_colored_item(row, 1, f"{win_rate:.1f}", win_rate > 50)
                
                # Profit Factor
                profit_factor = result.get("Profit Factor", 0)
                self._set_colored_item(row, 2, f"{profit_factor:.2f}", profit_factor > 1)
                
                # Track best profit factor
                if profit_factor > best_profit_factor:
                    best_profit_factor = profit_factor
                    best_ticker = ticker_period
                
                # Max Drawdown
                max_dd = result.get("Max Drawdown", 0)
                self._set_colored_item(row, 3, f"{max_dd:.1f}", max_dd < 10, inverse=True)
                
                # Track worst drawdown
                if max_dd > worst_drawdown:
                    worst_drawdown = max_dd
                    worst_ticker = ticker_period
                
                # Trade counts
                trades = result.get("Total Trades", 0)
                wins = result.get("Winning Trades", 0)
                losses = result.get("Losing Trades", 0)
                
                self.summary_table.setItem(row, 4, QTableWidgetItem(str(trades)))
                self.summary_table.setItem(row, 5, QTableWidgetItem(str(wins)))
                self.summary_table.setItem(row, 6, QTableWidgetItem(str(losses)))
                
                total_trades += trades
                total_wins += wins
                
                # Financial metrics
                gross_profit = result.get("Gross Profit", 0)
                gross_loss = result.get("Gross Loss", 0)
                final_equity = result.get("Final Equity", 10000)
                
                self._set_colored_item(row, 7, f"{gross_profit:.2f}", gross_profit > 0)
                self._set_colored_item(row, 8, f"{gross_loss:.2f}", False)
                self._set_colored_item(row, 9, f"{final_equity:.2f}", final_equity > 10000)
                
                # Optimal method
                method = result.get("Optimal Trailing Method", "Unknown")
                method_item = QTableWidgetItem(method[:30] + "..." if len(method) > 30 else method)
                method_item.setToolTip(method)  # Full text on hover
                self.summary_table.setItem(row, 10, method_item)
                
                row += 1
            
            # Auto-resize columns
            self.summary_table.resizeColumnsToContents()
            
            # Calculate overall statistics
            overall_win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
            
            # Display summary statistics
            summary_html = f"""
            <div style='background-color: #2c3e50; color: white; padding: 15px; border-radius: 8px; margin: 10px 0;'>
                <h3 style='margin: 0 0 10px 0;'>Overall Backtest Summary</h3>
                <div style='display: flex; justify-content: space-around;'>
                    <div>
                        <b>Total Symbols Tested:</b> {row_count}<br>
                        <b>Total Trades:</b> {total_trades}<br>
                        <b>Overall Win Rate:</b> <span style='color: {"#2ecc71" if overall_win_rate > 50 else "#e74c3c"}'>{overall_win_rate:.1f}%</span>
                    </div>
                    <div>
                        <b>Best Performer:</b> <span style='color: #2ecc71'>{best_ticker}</span><br>
                        <b>Profit Factor:</b> {best_profit_factor:.2f}<br>
                        <b>Worst Drawdown:</b> <span style='color: #e74c3c'>{worst_ticker} ({worst_drawdown:.1f}%)</span>
                    </div>
                </div>
            </div>
            """
            
            self.results_text.append(summary_html)
            
            # Display trades for each ticker if available
            if any("Trades" in result for result in results.values() if isinstance(result, dict)):
                # Combine all trades
                all_trades = []
                for ticker_period, result in results.items():
                    if isinstance(result, dict) and "Trades" in result:
                        trades = result["Trades"]
                        # Add ticker info to each trade
                        for trade in trades:
                            trade["ticker_period"] = ticker_period
                        all_trades.extend(trades)
                
                if all_trades:
                    # Sort trades by entry time
                    all_trades.sort(key=lambda x: x.get("entry_time", ""))
                    
                    # Display combined trades
                    self._display_combined_trades(all_trades)
            
            # Enable export button
            if hasattr(self, 'export_button'):
                self.export_button.setEnabled(True)
                self.current_results = results  # Store for export
                
        except Exception as e:
            self.results_text.append(f"<div style='color: red;'>Error displaying multiple ticker results: {str(e)}</div>")
            import traceback
            traceback.print_exc()

    def _display_combined_trades(self, trades):
        """Display trades from multiple tickers in a single table"""
        if not trades:
            return
            
        # Setup trades table with ticker column
        trade_columns = [
            "Ticker/Period", "Entry Time", "Exit Time", "Direction", 
            "Entry Price", "Exit Price", "Contract Price", 
            "P&L ($)", "P&L (%)", "Exit Reason"
        ]
        
        self.trades_table.setColumnCount(len(trade_columns))
        self.trades_table.setHorizontalHeaderLabels(trade_columns)
        self.trades_table.setRowCount(len(trades))
        
        # Track statistics
        winning_trades = 0
        total_pnl = 0
        
        # Populate trades
        for row, trade in enumerate(trades):
            # Ticker/Period
            ticker_item = QTableWidgetItem(trade.get('ticker_period', 'Unknown'))
            ticker_item.setFont(QFont("Arial", 9, QFont.Bold))
            self.trades_table.setItem(row, 0, ticker_item)
            
            # Time stamps
            self.trades_table.setItem(row, 1, QTableWidgetItem(str(trade.get('entry_time', ''))))
            self.trades_table.setItem(row, 2, QTableWidgetItem(str(trade.get('exit_time', ''))))
            
            # Direction with color
            direction = trade.get('direction', '')
            dir_item = QTableWidgetItem(direction.capitalize())
            if direction == 'bullish':
                dir_item.setForeground(QColor("#27ae60"))
            else:
                dir_item.setForeground(QColor("#e74c3c"))
            self.trades_table.setItem(row, 3, dir_item)
            
            # Prices
            self.trades_table.setItem(row, 4, QTableWidgetItem(f"${trade.get('entry_price', 0):.2f}"))
            self.trades_table.setItem(row, 5, QTableWidgetItem(f"${trade.get('exit_price', 0):.2f}"))
            self.trades_table.setItem(row, 6, QTableWidgetItem(f"${trade.get('contract_price', 0):.2f}"))
            
            # P&L with color
            pnl_dollars = trade.get('pnl_dollars', 0)
            pnl_pct = trade.get('pnl_pct', 0)
            
            self._set_colored_trade_item(row, 7, f"${pnl_dollars:.2f}", pnl_dollars > 0)
            self._set_colored_trade_item(row, 8, f"{pnl_pct:.1f}%", pnl_pct > 0)
            
            # Track statistics
            if pnl_dollars > 0:
                winning_trades += 1
            total_pnl += pnl_dollars
            
            # Exit reason
            self.trades_table.setItem(row, 9, QTableWidgetItem(trade.get('exit_reason', '')))
        
        self.trades_table.resizeColumnsToContents()
        
        # Display trade statistics
        trade_stats_html = f"""
        <div style='background-color: #34495e; color: white; padding: 10px; border-radius: 5px; margin: 10px 0;'>
            <b>Trade Statistics:</b>
            Total P&L: <span style='color: {"#2ecc71" if total_pnl > 0 else "#e74c3c"}'>${total_pnl:.2f}</span> | 
            Win Rate: {(winning_trades/len(trades)*100):.1f}% | 
            Avg Trade: ${(total_pnl/len(trades)):.2f}
        </div>
        """
        self.results_text.append(trade_stats_html)


    def _set_colored_item(self, row, col, text, is_positive, inverse=False):
        """Set a table item with color based on value"""
        item = QTableWidgetItem(text)
        
        if inverse:
            is_positive = not is_positive
            
        if is_positive:
            item.setBackground(QColor("#d4edda"))  # Light green
            item.setForeground(QColor("#155724"))  # Dark green
        else:
            item.setBackground(QColor("#f8d7da"))  # Light red
            item.setForeground(QColor("#721c24"))  # Dark red
            
        self.summary_table.setItem(row, col, item)


    def _display_trades(self, trades):
        """Display individual trades in the trades table"""
        if not trades:
            return
            
        # Setup trades table
        trade_columns = [
            "Entry Time", "Exit Time", "Direction", "Entry Price", 
            "Exit Price", "Contract Price", "P&L ($)", "P&L (%)", 
            "Method", "Exit Reason"
        ]
        
        self.trades_table.setColumnCount(len(trade_columns))
        self.trades_table.setHorizontalHeaderLabels(trade_columns)
        self.trades_table.setRowCount(len(trades))
        
        # Populate trades
        for row, trade in enumerate(trades):
            # Time stamps
            self.trades_table.setItem(row, 0, QTableWidgetItem(str(trade.get('entry_time', ''))))
            self.trades_table.setItem(row, 1, QTableWidgetItem(str(trade.get('exit_time', ''))))
            
            # Direction with color
            direction = trade.get('direction', '')
            dir_item = QTableWidgetItem(direction.capitalize())
            if direction == 'bullish':
                dir_item.setForeground(QColor("#27ae60"))
            else:
                dir_item.setForeground(QColor("#e74c3c"))
            self.trades_table.setItem(row, 2, dir_item)
            
            # Prices
            self.trades_table.setItem(row, 3, QTableWidgetItem(f"${trade.get('entry_price', 0):.2f}"))
            self.trades_table.setItem(row, 4, QTableWidgetItem(f"${trade.get('exit_price', 0):.2f}"))
            self.trades_table.setItem(row, 5, QTableWidgetItem(f"${trade.get('contract_price', 0):.2f}"))
            
            # P&L with color
            pnl_dollars = trade.get('pnl_dollars', 0)
            pnl_pct = trade.get('pnl_pct', 0)
            
            self._set_colored_trade_item(row, 6, f"${pnl_dollars:.2f}", pnl_dollars > 0)
            self._set_colored_trade_item(row, 7, f"{pnl_pct:.1f}%", pnl_pct > 0)
            
            # Method and exit reason
            self.trades_table.setItem(row, 8, QTableWidgetItem(trade.get('method', '')))
            self.trades_table.setItem(row, 9, QTableWidgetItem(trade.get('exit_reason', '')))
        
        self.trades_table.resizeColumnsToContents()


    def _set_colored_trade_item(self, row, col, text, is_positive):
        """Set a trade table item with color"""
        item = QTableWidgetItem(text)
        
        if is_positive:
            item.setForeground(QColor("#27ae60"))  # Green
            item.setFont(QFont("Arial", 10, QFont.Bold))
        else:
            item.setForeground(QColor("#e74c3c"))  # Red
            item.setFont(QFont("Arial", 10, QFont.Bold))
            
        self.trades_table.setItem(row, col, item)


    def update_result_display(self, ticker, results):
        """Update display with results for a single ticker (live update during backtest)"""
        # Create a formatted summary card for this ticker
        summary_html = f"""
        <div style='background-color: white; padding: 10px; margin: 5px; border-radius: 5px; border: 1px solid #ddd;'>
            <h4 style='color: #2c3e50; margin: 0;'>{ticker} Results:</h4>
            <table style='width: 100%; margin-top: 10px;'>
                <tr>
                    <td style='padding: 5px;'><b>Win Rate:</b></td>
                    <td style='padding: 5px; color: {"#27ae60" if results.get("Win Rate", 0) > 50 else "#e74c3c"};'>{results.get("Win Rate", 0)}%</td>
                    <td style='padding: 5px;'><b>Profit Factor:</b></td>
                    <td style='padding: 5px; color: {"#27ae60" if results.get("Profit Factor", 0) > 1 else "#e74c3c"};'>{results.get("Profit Factor", 0)}</td>
                </tr>
                <tr>
                    <td style='padding: 5px;'><b>Total Trades:</b></td>
                    <td style='padding: 5px;'>{results.get("Total Trades", 0)}</td>
                    <td style='padding: 5px;'><b>Max Drawdown:</b></td>
                    <td style='padding: 5px; color: #e74c3c;'>{results.get("Max Drawdown", 0)}%</td>
                </tr>
                <tr>
                    <td style='padding: 5px;'><b>Winning Trades:</b></td>
                    <td style='padding: 5px; color: #27ae60;'>{results.get("Winning Trades", 0)}</td>
                    <td style='padding: 5px;'><b>Losing Trades:</b></td>
                    <td style='padding: 5px; color: #e74c3c;'>{results.get("Losing Trades", 0)}</td>
                </tr>
                <tr>
                    <td style='padding: 5px;'><b>Final Equity:</b></td>
                    <td style='padding: 5px; color: {"#27ae60" if results.get("Final Equity", 10000) > 10000 else "#e74c3c"};'>${results.get("Final Equity", 10000):,.2f}</td>
                    <td style='padding: 5px;'><b>Best Method:</b></td>
                    <td style='padding: 5px; font-size: 11px;'>{results.get("Optimal Trailing Method", "N/A")[:20]}...</td>
                </tr>
            </table>
        </div>
        """
        
        self.results_text.append(summary_html)
        
        # Ensure the UI updates
        QApplication.processEvents()

class JigsawFlowApp(QMainWindow):
    """Main application window with enhanced styling"""
    def __init__(self):
        super().__init__()
        self.login_widget = None
        self.dashboard = None
        self.config_widget = None
        self.backtest_widget = None
        self.mongodb_widget = None
        self.tabs = None
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle("Jigsaw Flow - Stock Options Intraday Trading Bot")
        self.setGeometry(100, 100, 1200, 800)
        
        # Set application-wide styles
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QTabWidget::pane {
                border: 1px solid #bdc3c7;
                border-radius: 5px;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #ecf0f1;
                color: #7f8c8d;
                border: 1px solid #bdc3c7;
                border-bottom: none;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                padding: 10px 15px;
                min-width: 120px;
                font-weight: bold;
            }
            QTabBar::tab:selected {
                background-color: white;
                color: #2980b9;
                border-bottom: 2px solid #2980b9;
            }
            QTabBar::tab:hover {
                background-color: #f5f5f5;
                color: #3498db;
            }
            QScrollBar:vertical {
                border: none;
                background: #f5f5f5;
                width: 10px;
                margin: 0px;
            }
            QScrollBar::handle:vertical {
                background: #bdc3c7;
                min-height: 30px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical:hover {
                background: #95a5a6;
            }
            QToolTip {
                border: 1px solid #bdc3c7;
                background-color: #f9f9f9;
                color: #2c3e50;
                padding: 5px;
                border-radius: 3px;
            }
        """)
        
        # Start with login widget
        self.login_widget = LoginWidget()
        self.setCentralWidget(self.login_widget)
        
        # Set application icon - placeholder
        # self.setWindowIcon(QIcon("assets/logo.png"))
        
    def show_main_interface(self):
        """Show main interface with tabs and transition animation"""
        # Create main tab widget with enhanced styling
        self.tabs = QTabWidget()
        
        # Trading dashboard with enhanced UI
        self.dashboard = TradingDashboardWidget()
        self.tabs.addTab(self.dashboard, "Trading Dashboard")
        
        # Configuration with enhanced UI
        self.config_widget = ConfigurationWidget()
        self.tabs.addTab(self.config_widget, "Configuration")
        
        # Backtesting with enhanced UI
        self.backtest_widget = BacktestWidget()
        self.tabs.addTab(self.backtest_widget, "Backtesting")
        
        # MongoDB Manager with enhanced UI
        self.mongodb_widget = MongoDBManagerWidget()
        self.tabs.addTab(self.mongodb_widget, "MongoDB Manager")
        
        # Set as central widget with effect
        self.setCentralWidget(self.tabs)
        
    def get_login_widget(self):
        """Get login widget"""
        return self.login_widget
        
    def get_dashboard(self):
        """Get dashboard widget"""
        return self.dashboard
        
    def get_config_widget(self):
        """Get configuration widget"""
        return self.config_widget
        
    def get_backtest_widget(self):
        """Get backtest widget"""
        return self.backtest_widget
        
    def get_mongodb_widget(self):
        """Get MongoDB manager widget"""
        return self.mongodb_widget