# config.py

# --- Telegram Bot ---
API_ID = 1234567  # Your API ID from my.telegram.org
API_HASH = "your_api_hash"
BOT_TOKEN = "your_bot_token"

# --- Wasabi Storage ---
WASABI_ACCESS_KEY = "your_wasabi_access_key"
WASABI_SECRET_KEY = "your_wasabi_secret_key"
WASABI_BUCKET = "your-bucket-name"
WASABI_REGION = "ap-northeast-1" # e.g., us-east-1, eu-central-1
WASABI_ENDPOINT_URL = "https://s3.wasabisys.com" # Or your region-specific endpoint

# --- Optional Backup Channel ---
STORAGE_CHANNEL_ID = -1001234567890 # Your private channel ID, e.g., -100...

# --- In-memory Database ---
# This will be reset every time the bot restarts.
# Consider replacing with a persistent solution (SQLite, JSON file, etc.)
FILE_DATABASE = {} 
