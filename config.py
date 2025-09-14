import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Telegram Bot ---
API_ID = int(os.getenv("API_ID", "1234567"))
API_HASH = os.getenv("API_HASH", "your_api_hash")
BOT_TOKEN = os.getenv("BOT_TOKEN", "your_bot_token")

# --- Wasabi Storage ---
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY", "your_wasabi_access_key")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY", "your_wasabi_secret_key")
WASABI_BUCKET = os.getenv("WASABI_BUCKET", "your-bucket-name")
WASABI_REGION = os.getenv("WASABI_REGION", "ap-northeast-1")  
WASABI_ENDPOINT_URL = os.getenv("WASABI_ENDPOINT_URL", "https://s3.wasabisys.com")

# --- Optional Backup Channel ---
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "-1001234567890"))

# --- In-memory Database ---
# This will reset every time the bot restarts.
# Consider replacing with a persistent solution (SQLite, JSON file, etc.)
FILE_DATABASE = {}
