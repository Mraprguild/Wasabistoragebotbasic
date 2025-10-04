import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Bot Configuration ---
class Config:
    # Telegram API Configuration
    API_ID = os.getenv("API_ID")
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    # Wasabi S3 Configuration
    WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.getenv("WASABI_BUCKET")
    WASABI_REGION = os.getenv("WASABI_REGION")
    WASABI_ENDPOINT_URL = f'https://s3.{WASABI_REGION}.wasabisys.com'
    
    # Authorization
    AUTHORIZED_USERS = [int(user_id) for user_id in os.getenv("AUTHORIZED_USERS", "").split(",") if user_id]
    
    # Rate Limiting
    MAX_REQUESTS_PER_MINUTE = 30  # Increased limit for power users
    MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB Ultra size limit
    
    # Performance Settings
    PYROGRAM_WORKERS = 50
    MAX_POOL_CONNECTIONS = 100
    MAX_CONCURRENCY = 50
    MULTIPART_THRESHOLD = 32 * 1024 * 1024  # 32MB
    MULTIPART_CHUNKSIZE = 32 * 1024 * 1024  # 32MB
    NUM_DOWNLOAD_ATTEMPTS = 10
    
    # Timeout Settings
    CONNECT_TIMEOUT = 30
    READ_TIMEOUT = 60
    
    # URLs
    WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/IMG-20250915-WA0013.jpg"
    
    # Web Server
    WEB_SERVER_PORT = int(os.environ.get('PORT', 8080))
    WEB_SERVER_HOST = '0.0.0.0'
    
    # Retry Settings
    MAX_RETRIES = 5
    RETRY_DELAY = 5  # seconds
    
    # Progress Settings
    PROGRESS_UPDATE_INTERVAL = 1.5  # seconds
    PROGRESS_BAR_LENGTH = 12

# --- Validation ---
def validate_config():
    """Validate that all required environment variables are set"""
    required_vars = [
        "API_ID", "API_HASH", "BOT_TOKEN", 
        "WASABI_ACCESS_KEY", "WASABI_SECRET_KEY", 
        "WASABI_BUCKET", "WASABI_REGION"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not getattr(Config, var, None):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file.")
        return False
    
    print("✅ All required configuration variables are set")
    return True

# --- Configuration Instances ---
# Create config instance for easy access
config = Config()

# Performance mode description
PERFORMANCE_MODE = "ULTRA_TURBO"
