import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Configuration
    API_ID = os.getenv("API_ID")
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    # Wasabi Configuration
    WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.getenv("WASABI_BUCKET")
    WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
    
    # Set default region if not provided
    if not WASABI_REGION:
        WASABI_REGION = "us-east-1"
    WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"
    
    # Optional Telegram Channel Storage
    STORAGE_CHANNEL_ID = os.getenv("STORAGE_CHANNEL_ID")
    
    # File Configuration - Maximum Speed Optimized
    MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for maximum speed
    
    # MAXIMUM SPEED TELEGRAM OPTIMIZATION
    MAX_CONCURRENT_TRANSMISSIONS = 32  # Maximum parallel streams
    CONNECTION_POOL_SIZE = 50  # Large HTTP connection pool
    UPLOAD_WORKERS = 16  # Maximum concurrent workers
    DOWNLOAD_WORKERS = 16  # Maximum download workers
    
    # Ultra-fast chunk sizes
    TELEGRAM_CHUNK_SIZE = 32 * 1024 * 1024  # 32MB for Telegram transfers
    PROGRESS_UPDATE_INTERVAL = 16 * 1024 * 1024  # Update every 16MB
    
    # Web Interface
    WEB_HOST = "0.0.0.0"
    WEB_PORT = 5000
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required_vars = ["API_ID", "API_HASH", "BOT_TOKEN", "WASABI_ACCESS_KEY", "WASABI_SECRET_KEY", "WASABI_BUCKET"]
        missing_vars = [var for var in required_vars if not getattr(cls, var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True