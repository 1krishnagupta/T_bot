"""
MongoDB handler for the trading bot.
This module handles MongoDB connection, database operations, and auto-installation.
"""

import os
import sys
import subprocess
import platform
import logging
import time
import json
import requests
import zipfile
import tarfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple

# Setup logging
today = datetime.now().strftime("%Y-%m-%d")
log_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"mongodb_handler_{today}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class MongoDBHandler:
    """
    MongoDB database handler for the trading bot.
    Handles auto-installation, connection, and database operations.
    """
    
    def __init__(self, auto_start: bool = True, port: int = 27017, db_name: str = "trading_bot"):
        """
        Initialize the MongoDB handler.
        
        Args:
            auto_start: Whether to auto-start MongoDB if not running
            port: MongoDB port to use
            db_name: Database name to use
        """
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self.mongo_path = self._get_mongo_install_path()
        
        # Auto check and install MongoDB if needed
        if not self._is_mongodb_installed():
            self._install_mongodb()
        
        # Auto start MongoDB if requested
        if auto_start and not self._is_mongodb_running():
            self._start_mongodb()
            
        # Import pymongo after ensuring MongoDB is installed
        try:
            import pymongo
            self.pymongo = pymongo
        except ImportError:
            self._install_pymongo()
            import pymongo
            self.pymongo = pymongo
            
        # Connect to MongoDB
        self._connect()
    
    def _get_mongo_install_path(self) -> str:
        """
        Get the MongoDB installation path based on OS.
        
        Returns:
            str: Path to MongoDB installation directory
        """
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'mongodb'))
        return base_dir
        
    def _is_mongodb_installed(self) -> bool:
        """
        Check if MongoDB is installed.
        
        Returns:
            bool: True if MongoDB is installed, False otherwise
        """
        # Check for MongoDB installation in the project directory
        mongo_binary = self._get_mongo_binary_path()
        if os.path.exists(mongo_binary):
            logger.info(f"MongoDB found at {mongo_binary}")
            return True
        
        # Also check if MongoDB is installed system-wide
        try:
            if platform.system() == "Windows":
                # Check if mongod is in PATH
                result = subprocess.run(["where", "mongod"], 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE,
                                        text=True)
                return result.returncode == 0
            else:
                # For Mac/Linux
                result = subprocess.run(["which", "mongod"], 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE,
                                        text=True)
                return result.returncode == 0
        except Exception as e:
            logger.error(f"Error checking for system MongoDB: {str(e)}")
            return False
    
    def _get_mongo_binary_path(self) -> str:
        """
        Get the path to the MongoDB binary based on the OS.
        
        Returns:
            str: Path to MongoDB binary
        """
        system = platform.system()
        if system == "Windows":
            return os.path.join(self.mongo_path, "bin", "mongod.exe")
        else:
            return os.path.join(self.mongo_path, "bin", "mongod")
    
    def _install_mongodb(self) -> None:
        """
        Download and install MongoDB in the project directory.
        """
        logger.info("Installing MongoDB...")
        
        # Create directory for MongoDB installation if it doesn't exist
        os.makedirs(self.mongo_path, exist_ok=True)
        
        # Determine MongoDB download URL based on platform
        system = platform.system()
        machine = platform.machine()
        download_url = self._get_mongodb_download_url(system, machine)
        
        if not download_url:
            logger.error(f"Unsupported platform: {system} {machine}")
            print(f"[✗] Unsupported platform: {system} {machine}")
            print("[✗] Please install MongoDB manually.")
            sys.exit(1)
        
        # Download MongoDB
        try:
            print("[*] Downloading MongoDB...")
            archive_path = os.path.join(self.mongo_path, "mongodb_archive")
            
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            
            with open(archive_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        
            print("[✓] MongoDB downloaded successfully.")
            
            # Extract MongoDB
            print("[*] Extracting MongoDB...")
            if download_url.endswith(".zip"):
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(self.mongo_path)
            else:
                with tarfile.open(archive_path) as tar_ref:
                    tar_ref.extractall(self.mongo_path)
            
            # Rename extracted directory to standardize path
            extracted_dir = None
            for item in os.listdir(self.mongo_path):
                item_path = os.path.join(self.mongo_path, item)
                if os.path.isdir(item_path) and item.startswith("mongodb-"):
                    extracted_dir = item_path
                    break
            
            if extracted_dir:
                temp_dir = os.path.join(self.mongo_path, "temp")
                os.makedirs(temp_dir, exist_ok=True)
                
                # Move files from extracted directory to temp directory
                for item in os.listdir(extracted_dir):
                    src = os.path.join(extracted_dir, item)
                    dst = os.path.join(temp_dir, item)
                    shutil.move(src, dst)
                
                # Remove extracted directory
                shutil.rmtree(extracted_dir)
                
                # Move files from temp directory to MongoDB installation directory
                for item in os.listdir(temp_dir):
                    src = os.path.join(temp_dir, item)
                    dst = os.path.join(self.mongo_path, item)
                    shutil.move(src, dst)
                
                # Remove temp directory
                shutil.rmtree(temp_dir)
            
            # Create data directory
            data_dir = os.path.join(self.mongo_path, "data")
            os.makedirs(data_dir, exist_ok=True)
            
            # Create logs directory
            logs_dir = os.path.join(self.mongo_path, "logs")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Clean up the archive
            os.remove(archive_path)
            
            print("[✓] MongoDB installed successfully.")
            logger.info("MongoDB installed successfully.")
        except Exception as e:
            logger.error(f"Error installing MongoDB: {str(e)}")
            print(f"[✗] Error installing MongoDB: {str(e)}")
            sys.exit(1)
    
    def _get_mongodb_download_url(self, system: str, machine: str) -> Optional[str]:
        """
        Get the MongoDB download URL for the current platform.
        
        Args:
            system: Operating system name
            machine: Machine architecture
            
        Returns:
            str: MongoDB download URL
        """
        # MongoDB Community Server 6.0.8 (latest stable version that works on most systems)
        base_url = "https://fastdl.mongodb.org"
        
        if system == "Windows":
            if machine == "AMD64" or machine == "x86_64":
                return f"{base_url}/windows/mongodb-windows-x86_64-6.0.8.zip"
            else:
                return None
        elif system == "Darwin":  # macOS
            if machine == "x86_64":
                return f"{base_url}/osx/mongodb-macos-x86_64-6.0.8.tgz"
            elif machine == "arm64":
                return f"{base_url}/osx/mongodb-macos-arm64-6.0.8.tgz"
            else:
                return None
        elif system == "Linux":
            if machine == "x86_64":
                return f"{base_url}/linux/mongodb-linux-x86_64-ubuntu2204-6.0.8.tgz"
            elif machine == "aarch64":
                return f"{base_url}/linux/mongodb-linux-aarch64-ubuntu2204-6.0.8.tgz"
            else:
                return None
        else:
            return None
    
    def _is_mongodb_running(self) -> bool:
        """
        Check if MongoDB is running.
        
        Returns:
            bool: True if MongoDB is running, False otherwise
        """
        try:
            # Try to connect to MongoDB
            import pymongo
            client = pymongo.MongoClient(f"mongodb://localhost:{self.port}/", serverSelectionTimeoutMS=2000)
            client.server_info()  # Will raise an exception if server is not running
            client.close()
            return True
        except Exception:
            return False
    
    def _start_mongodb(self) -> None:
        """
        Start MongoDB server.
        """
        mongo_binary = self._get_mongo_binary_path()
        data_dir = os.path.join(self.mongo_path, "data")
        log_file = os.path.join(self.mongo_path, "logs", "mongodb.log")
        
        try:
            # Create data and logs directories if they don't exist
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            # Command to start MongoDB
            cmd = [
                mongo_binary,
                "--dbpath", data_dir,
                "--port", str(self.port),
                "--logpath", log_file,
                "--fork",  # Run in background
            ]
            
            # On Windows, --fork is not supported
            if platform.system() == "Windows":
                # Remove --fork option for Windows
                cmd.remove("--fork")
                
                # Start MongoDB as a background process on Windows
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    startupinfo=startupinfo
                )
            else:
                # Start MongoDB on Mac/Linux
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                
                if result.returncode != 0:
                    logger.error(f"Error starting MongoDB: {result.stderr}")
                    print(f"[✗] Error starting MongoDB: {result.stderr}")
                    sys.exit(1)
            
            # Wait for MongoDB to start
            max_attempts = 5
            for attempt in range(max_attempts):
                if self._is_mongodb_running():
                    print("[✓] MongoDB started successfully.")
                    logger.info("MongoDB started successfully.")
                    return
                time.sleep(2)
                
            logger.error("Failed to start MongoDB after multiple attempts.")
            print("[✗] Failed to start MongoDB after multiple attempts.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error starting MongoDB: {str(e)}")
            print(f"[✗] Error starting MongoDB: {str(e)}")
            sys.exit(1)
    
    def _install_pymongo(self) -> None:
        """
        Install pymongo library if not already installed.
        """
        try:
            print("[*] Installing pymongo...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])
            print("[✓] pymongo installed successfully.")
        except Exception as e:
            logger.error(f"Error installing pymongo: {str(e)}")
            print(f"[✗] Error installing pymongo: {str(e)}")
            sys.exit(1)
    
    def _connect(self) -> None:
        """
        Connect to MongoDB.
        """
        try:
            # Connect to MongoDB
            self.client = self.pymongo.MongoClient(f"mongodb://localhost:{self.port}/", serverSelectionTimeoutMS=5000)
            self.db = self.client[self.db_name]
            
            # Test connection
            self.client.server_info()
            
            logger.info(f"Connected to MongoDB on port {self.port}.")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            print(f"[✗] Error connecting to MongoDB: {str(e)}")
            sys.exit(1)
    
    def create_collection(self, collection_name: str) -> None:
        """
        Create a collection in the MongoDB database.
        
        Args:
            collection_name: Name of the collection
        """
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
                logger.info(f"Created collection: {collection_name}")
        except Exception as e:
            logger.error(f"Error creating collection {collection_name}: {str(e)}")
            print(f"[✗] Error creating collection {collection_name}: {str(e)}")
    
    def insert_one(self, collection_name: str, document: Dict) -> str:
        """
        Insert a document into a collection.
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            
        Returns:
            str: ID of the inserted document
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error inserting document into {collection_name}: {str(e)}")
            print(f"[✗] Error inserting document: {str(e)}")
            return None
    
    def insert_many(self, collection_name: str, documents: List[Dict]) -> List[str]:
        """
        Insert multiple documents into a collection.
        
        Args:
            collection_name: Name of the collection
            documents: List of documents to insert
            
        Returns:
            List[str]: IDs of the inserted documents
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Error inserting documents into {collection_name}: {str(e)}")
            print(f"[✗] Error inserting documents: {str(e)}")
            return []
    
    def find_one(self, collection_name: str, query: Dict) -> Dict:
        """
        Find a document in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            
        Returns:
            Dict: Found document or None
        """
        try:
            collection = self.db[collection_name]
            return collection.find_one(query)
        except Exception as e:
            logger.error(f"Error finding document in {collection_name}: {str(e)}")
            print(f"[✗] Error finding document: {str(e)}")
            return None
    
    def find_many(self, collection_name: str, query: Dict, limit: int = 0) -> List[Dict]:
        """
        Find multiple documents in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            limit: Maximum number of documents to return (0 = no limit)
            
        Returns:
            List[Dict]: Found documents
        """
        try:
            collection = self.db[collection_name]
            return list(collection.find(query).limit(limit) if limit > 0 else collection.find(query))
        except Exception as e:
            logger.error(f"Error finding documents in {collection_name}: {str(e)}")
            print(f"[✗] Error finding documents: {str(e)}")
            return []
    
    def update_one(self, collection_name: str, query: Dict, update: Dict) -> bool:
        """
        Update a document in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            update: Update to apply
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, {"$set": update})
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating document in {collection_name}: {str(e)}")
            print(f"[✗] Error updating document: {str(e)}")
            return False
    
    def update_many(self, collection_name: str, query: Dict, update: Dict) -> int:
        """
        Update multiple documents in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            update: Update to apply
            
        Returns:
            int: Number of documents updated
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_many(query, {"$set": update})
            return result.modified_count
        except Exception as e:
            logger.error(f"Error updating documents in {collection_name}: {str(e)}")
            print(f"[✗] Error updating documents: {str(e)}")
            return 0
    
    def delete_one(self, collection_name: str, query: Dict) -> bool:
        """
        Delete a document in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting document in {collection_name}: {str(e)}")
            print(f"[✗] Error deleting document: {str(e)}")
            return False
    
    def delete_many(self, collection_name: str, query: Dict) -> int:
        """
        Delete multiple documents in a collection.
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            
        Returns:
            int: Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting documents in {collection_name}: {str(e)}")
            print(f"[✗] Error deleting documents: {str(e)}")
            return 0
    
    def close(self) -> None:
        """
        Close MongoDB connection.
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")
    
    def create_index(self, collection_name: str, keys: Union[str, List[Tuple[str, int]]], unique: bool = False) -> None:
        """
        Create an index on a collection.
        
        Args:
            collection_name: Name of the collection
            keys: Keys to index (string or list of tuples)
            unique: Whether the index should be unique
        """
        try:
            collection = self.db[collection_name]
            collection.create_index(keys, unique=unique)
            logger.info(f"Created index on {collection_name}: {keys}")
        except Exception as e:
            logger.error(f"Error creating index on {collection_name}: {str(e)}")
            print(f"[✗] Error creating index: {str(e)}")
    
    def drop_collection(self, collection_name: str) -> None:
        """
        Drop a collection from the database.
        
        Args:
            collection_name: Name of the collection
        """
        try:
            self.db.drop_collection(collection_name)
            logger.info(f"Dropped collection: {collection_name}")
        except Exception as e:
            logger.error(f"Error dropping collection {collection_name}: {str(e)}")
            print(f"[✗] Error dropping collection: {str(e)}")
    
    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            bool: True if collection exists, False otherwise
        """
        try:
            return collection_name in self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Error checking if collection {collection_name} exists: {str(e)}")
            print(f"[✗] Error checking collection: {str(e)}")
            return False
            
    def clear_collections(self, collections=None):
        """
        Clear (delete all documents) from specified collections
        
        Args:
            collections (list): List of collection names to clear, or None to clear all
            
        Returns:
            dict: Dictionary with collection names and number of documents deleted
        """
        try:
            result = {}
            
            # If no collections specified, clear all
            if collections is None:
                collections = self.db.list_collection_names()
            
            # Clear each collection
            for collection_name in collections:
                if collection_name in self.db.list_collection_names():
                    delete_result = self.db[collection_name].delete_many({})
                    result[collection_name] = delete_result.deleted_count
                    logger.info(f"Cleared {delete_result.deleted_count} documents from {collection_name}")
                else:
                    result[collection_name] = 0
                    logger.warning(f"Collection {collection_name} does not exist")
            
            return result
        except Exception as e:
            logger.error(f"Error clearing collections: {str(e)}")
            return {}
            
    def clear_all_data(self):
        """
        Clear all data from all collections
        
        Returns:
            dict: Dictionary with collection names and number of documents deleted
        """
        return self.clear_collections()
    
    def get_collection_stats(self):
        """
        Get statistics about all collections
        
        Returns:
            dict: Dictionary with collection statistics
        """
        try:
            stats = {}
            collections = self.db.list_collection_names()
            
            for collection_name in collections:
                # Get document count
                count = self.db[collection_name].count_documents({})
                
                # Calculate collection size in MB
                # Use the collStats command to get accurate size information
                collection_stats = self.db.command("collStats", collection_name)
                size_mb = round(collection_stats.get("size", 0) / (1024 * 1024), 2)  # Convert bytes to MB
                
                stats[collection_name] = {
                    "count": count,
                    "size_mb": size_mb
                }
                
            return stats
        except Exception as e:
            logger.error(f"Error getting collection stats: {str(e)}")
            return {}


    def insert_one_with_fallback(self, collection_name: str, document: Dict) -> str:
        """
        Insert a document with fallback to file storage if MongoDB fails
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            
        Returns:
            str: ID of the inserted document or None
        """
        try:
            # Try MongoDB first
            return self.insert_one(collection_name, document)
        except Exception as e:
            self.logger.error(f"MongoDB insertion failed, using file fallback: {e}")
            
            # Fallback to file storage
            fallback_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'fallback_data'))
            os.makedirs(fallback_dir, exist_ok=True)
            
            # Generate ID if not present
            if '_id' not in document:
                document['_id'] = str(int(time.time())) + '-' + str(hash(str(document)))
                
            # Append to file
            file_path = os.path.join(fallback_dir, f"{collection_name}.jsonl")
            with open(file_path, 'a') as f:
                f.write(json.dumps(document) + '\n')
                
            return document.get('_id')

    

# MongoDB collections we'll use in our trading bot
COLLECTIONS = {
    'QUOTES': 'market_quotes',                    # Real-time market quotes
    'TRADES': 'market_trades',                    # Market trade executions
    'GREEKS': 'option_greeks',                   # Option Greeks data
    'CANDLES': 'price_candles',                  # OHLC candlestick data
    'POSITIONS': 'active_positions',             # Currently open trading positions
    'ORDERS': 'trading_orders',                  # All trading orders (active & historical)
    'ACCOUNT': 'account_info',                   # Broker account information
    'POSITION_HISTORY': 'position_history',      # Historical positions (closed trades)
    'STRATEGY_STATE': 'strategy_state',          # Strategy state and settings
    'TRADE_SIGNALS': 'trade_signals',            # Trading signals generated
    'PERFORMANCE': 'performance_metrics',        # Performance tracking
}

# Singleton instance of MongoDBHandler
_instance = None

def get_mongodb_handler() -> MongoDBHandler:
    """
    Get a singleton instance of MongoDBHandler.
    
    Returns:
        MongoDBHandler: Instance of MongoDBHandler
    """
    global _instance
    if _instance is None:
        _instance = MongoDBHandler()
    return _instance