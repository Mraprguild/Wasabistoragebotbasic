import os
import asyncio
import boto3
import logging
from botocore.exceptions import NoCredentialsError
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait
from dotenv import load_dotenv

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
load_dotenv()

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
WASABI_REGION = os.environ.get("WASABI_REGION")
STORAGE_CHANNEL_ID = os.environ.get("STORAGE_CHANNEL_ID") # Optional

# --- Validate Essential Configuration ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    logger.critical("!!! CRITICAL ERROR: One or more environment variables are missing. Please check your .env file.")
    exit()

# --- Initialize Telegram Bot ---
app = Client("file_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Initialize Wasabi S3 Client ---
try:
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY
    )
    logger.info("Boto3 S3 client initialized successfully.")
except Exception as e:
    logger.critical(f"Failed to initialize Boto3 client: {e}")
    exit()

# In-memory dictionary for simplicity. For production, use a database.
file_storage = {}
file_counter = 1

# --- Helper Classes and Functions ---

class ProgressCallback:
    """
    Handles progress reporting for Boto3 uploads in an async environment.
    """
    def __init__(self, message: Message, text: str, loop: asyncio.AbstractEventLoop):
        self._message = message
        self._text = text
        self._loop = loop
        self._size = 0
        self._seen_so_far = 0
        self._last_update_percentage = 0

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percentage = round((self._seen_so_far / self._size) * 100)
        
        # Update only every 5% to avoid Telegram flood limits
        if percentage > self._last_update_percentage + 4:
            self._last_update_percentage = percentage
            asyncio.run_coroutine_threadsafe(
                self._message.edit_text(f"{self._text}: {percentage}%"), self._loop
            )

    def set_size(self, size):
        self._size = size

async def run_blocking_io(func, *args):
    """Runs a blocking I/O function in a separate thread to avoid freezing the bot."""
    return await asyncio.to_thread(func, *args)

# --- Bot Commands ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    logger.info(f"Received /start command from {message.from_user.id}")
    await message.reply_text(
        "Welcome to the File Storage Bot! I'm now running on a more stable, non-blocking core.\n\n"
        "Use /help to see all available commands."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    logger.info(f"Received /help command from {message.from_user.id}")
    help_text = """
**Available Commands:**

/start - Show welcome message.
/upload - Send any file to upload it.
/download <file_id> - Get a download link for a file.
/list - List all stored files.
/stream <file_id> - Get streaming links for video/audio.
/test - Test the connection to Wasabi storage.
/help - Show this help message.
    """
    await message.reply_text(help_text)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message):
    """Handles all incoming files for upload."""
    global file_counter
    user_id = message.from_user.id
    logger.info(f"Received a file from user {user_id}.")

    file = message.document or message.video or message.audio or message.photo
    if not file:
        await message.reply_text("Could not identify the file. Please try again.")
        return

    file_name = getattr(file, 'file_name', f"upload_{file.file_unique_id}.{file.mime_type.split('/')[1]}")
    file_id = str(file_counter)
    
    progress_message = await message.reply_text("Downloading from Telegram servers...")

    try:
        file_path = await message.download()
        await progress_message.edit_text("Download complete. Preparing to upload to Wasabi...")
        
        loop = asyncio.get_running_loop()
        progress_callback = ProgressCallback(progress_message, "Uploading to Wasabi", loop)
        progress_callback.set_size(file.file_size)

        # Run the blocking S3 upload in a separate thread
        await run_blocking_io(
            s3.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=progress_callback
        )
        
        file_storage[file_id] = {'name': file_name, 'size': file.file_size}
        file_counter += 1

        await progress_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File Name:** `{file_name}`\n"
            f"**File ID:** `{file_id}`"
        )
        logger.info(f"Successfully uploaded {file_name} with ID {file_id}.")

    except NoCredentialsError:
        logger.error("Wasabi credentials not found.")
        await progress_message.edit_text("❌ **Error:** Wasabi credentials not configured correctly.")
    except Exception as e:
        logger.error(f"An error occurred during upload: {e}", exc_info=True)
        await progress_message.edit_text(f"❌ **An error occurred:**\n`{e}`")
    finally:
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_command(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_id>`")
        return
        
    file_id = message.command[1]
    if file_id not in file_storage:
        await message.reply_text("❌ File ID not found.")
        return

    file_name = file_storage[file_id]['name']
    
    msg = await message.reply_text("Generating download link...")
    try:
        params = {'Bucket': WASABI_BUCKET, 'Key': file_name}
        url = await run_blocking_io(s3.generate_presigned_url, 'get_object', Params=params, ExpiresIn=3600)
        
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Download Now", url=url)]])
        await msg.edit_text(f"Click to download: **{file_name}**", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Failed to generate download link for {file_name}: {e}")
        await msg.edit_text(f"❌ **Could not generate download link:** {e}")

@app.on_message(filters.command("list"))
async def list_files_command(client, message):
    if not file_storage:
        await message.reply_text("No files have been uploaded yet.")
        return
    file_list = "**Stored Files:**\n\n"
    for fid, info in file_storage.items():
        size_mb = info['size'] / (1024 * 1024)
        file_list += f"**ID:** `{fid}` - **Name:** `{info['name']}` ({size_mb:.2f} MB)\n"
    await message.reply_text(file_list)

@app.on_message(filters.command("stream"))
async def stream_command(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/stream <file_id>`")
        return

    file_id = message.command[1]
    if file_id not in file_storage:
        await message.reply_text("❌ File ID not found.")
        return
        
    file_name = file_storage[file_id]['name']

    msg = await message.reply_text("Generating streaming links...")
    try:
        params = {'Bucket': WASABI_BUCKET, 'Key': file_name}
        stream_url = await run_blocking_io(s3.generate_presigned_url, 'get_object', Params=params, ExpiresIn=86400)

        # Create player links
        mx_player_link = f"intent:{stream_url}#Intent;package=com.mxtech.videoplayer.ad;end"
        vlc_link = f"vlc://{stream_url}"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Open in MX Player", url=mx_player_link)],
            [InlineKeyboardButton("🟠 Open in VLC", url=vlc_link)]
        ])
        
        await msg.edit_text(f"**Streaming Links for:** `{file_name}`", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Failed to generate stream link for {file_name}: {e}")
        await msg.edit_text(f"❌ **Could not generate streaming link:** {e}")

@app.on_message(filters.command("test"))
async def test_wasabi_connection(client, message):
    try:
        await run_blocking_io(s3.list_buckets)
        await message.reply_text("✅ Wasabi connection successful!")
    except NoCredentialsError:
        await message.reply_text("❌ **Error:** Wasabi credentials not found.")
    except Exception as e:
        await message.reply_text(f"❌ **Wasabi connection failed:**\n`{e}`")

async def main():
    """Main function to start the bot."""
    await app.start()
    me = await app.get_me()
    logger.info(f"Bot started as @{me.username}. Waiting for messages...")
    await asyncio.Event().wait() # Keep the bot running indefinitely

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutting down...")
