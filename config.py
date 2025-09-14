import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()

class Config:
    """
    Configuration class for the Telegram File Bot.
    Reads sensitive information and settings from environment variables.
    """
    # Telegram API Credentials
    API_ID = os.environ.get("API_ID")
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")

    # Wasabi Cloud Storage Credentials
    WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
    WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1") # Default region
    
    # Get the Wasabi endpoint URL
    WASABI_ENDPOINT_URL = f"https://s3.{WASABI_REGION}.wasabisys.com"

    # Telegram Channel for backup storage
    # Make sure the bot is an admin in this channel
    STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID")) if os.environ.get("STORAGE_CHANNEL_ID") else None
    
    # Web server port
    PORT = int(os.environ.get("PORT", 5000))

    # In-memory database to store file information
    # In a production environment, you would use a persistent database like SQLite, PostgreSQL, or Redis.
    # Format: { 'file_unique_id': {'file_name': '...', 'wasabi_url': '...', 'channel_message_id': ...} }
    FILE_DATABASE = {}

# Instantiate the config
config = Config()

# Basic validation to ensure essential variables are set
def validate_config():
    """Checks if all mandatory environment variables are set."""
    required_vars = [
        "API_ID", "API_HASH", "BOT_TOKEN", "WASABI_ACCESS_KEY",
        "WASABI_SECRET_KEY", "WASABI_BUCKET", "STORAGE_CHANNEL_ID"
    ]
    missing_vars = [var for var in required_vars if not getattr(config, var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Run validation on import
try:
    validate_config()
except ValueError as e:
    print(f"Error: {e}")
    # You might want to exit the application if config is invalid
    # import sys
    # sys.exit(1)
