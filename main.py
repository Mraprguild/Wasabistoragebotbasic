import os
import time
import math
import logging
import sqlite3
import asyncio
from dotenv import load_dotenv

import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration & Validation ---
# We now check for each required variable and exit if one is missing.
# This prevents the bot from starting in a broken state.
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
OWNER_ID = os.getenv("OWNER_ID")
STORAGE_CHANNEL_ID = os.getenv("STORAGE_CHANNEL_ID")

# Validate that all critical variables are set
missing_vars = [v for v in ["API_ID", "API_HASH", "BOT_TOKEN", "WASABI_ACCESS_KEY", "WASABI_SECRET_KEY", "WASABI_BUCKET", "WASABI_REGION", "OWNER_ID"] if not globals()[v]]
if missing_vars:
    raise SystemExit(f"❌ ERROR: Missing critical environment variables: {', '.join(missing_vars)}")

# Convert numeric values after checking they exist
API_ID = int(API_ID)
OWNER_ID = int(OWNER_ID)
STORAGE_CHANNEL_ID = int(STORAGE_CHANNEL_ID) if STORAGE_CHANNEL_ID else 0


# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Setup (SQLite) ---
DB_FILE = "file_bot.db"
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT UNIQUE NOT NULL,
            user_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            wasabi_key TEXT NOT NULL,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# --- Wasabi S3 Client Setup ---
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    logger.info("Wasabi S3 client initialized successfully.")
except Exception as e:
    logger.error(f"Could not initialize Wasabi S3 client: {e}")
    s3_client = None

# --- Pyrogram Bot Client ---
app = Client("file_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Helper Functions ---
def human_readable_size(size_bytes):
    if size_bytes == 0: return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# This progress callback is now fully asynchronous
async def progress_callback(current, total, message, action, start_time):
    now = time.time()
    elapsed = now - start_time
    if elapsed < 1: return # Update only once per second to avoid FloodWait

    speed = current / elapsed
    percentage = current * 100 / total
    progress_bar = "[{0}{1}]".format('█' * int(percentage / 5), ' ' * (20 - int(percentage / 5)))
    
    try:
        text = (
            f"**{action}**\n"
            f"{progress_bar} **{percentage:.1f}%**\n"
            f"**Processed:** {human_readable_size(current)} of {human_readable_size(total)}\n"
            f"**Speed:** {human_readable_size(speed)}/s"
        )
        # Check if message text is different before editing to avoid unnecessary API calls
        if message.text != text:
            await message.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.x)
    except Exception:
        pass # Ignore other errors like message not modified

# --- Bot Command Handlers ---
owner_filter = filters.user(OWNER_ID)

@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "**Welcome to the File Storage Bot!** 🗂️\n\n"
        "Send any file to me, and I will upload it to secure cloud storage, providing you with fast streaming and download links.\n\n"
        "Use /help to see all available commands."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    await message.reply_text(
        "**📚 Bot Commands Guide**\n\n"
        "**/start** - Show the welcome message.\n"
        "**/list** - List all your uploaded files.\n"
        "**/test** - (Owner Only) Test the connection to Wasabi.\n"
        "**/help** - Display this help message.\n\n"
        "To upload, simply send me any file."
    )

@app.on_message(filters.command("test"))
async def test_wasabi_command(client, message):
    if message.from_user.id != OWNER_ID:
        await message.reply_text("⛔ You are not authorized to use this command.")
        return

    if not s3_client:
        await message.reply_text("❌ **Wasabi client is not configured.** Check your logs and environment variables.")
        return
        
    test_msg = await message.reply_text("⏳ **Testing Wasabi connection...**")
    try:
        # Running the blocking boto3 call in a separate thread
        await asyncio.to_thread(s3_client.list_buckets)
        await test_msg.edit_text(f"✅ **Connection Successful!**\n\n**Bucket:** `{WASABI_BUCKET}`\n**Region:** `{WASABI_REGION}`")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        await test_msg.edit_text(f"❌ **Connection Failed!**\n\n**Error:** `{error_code}`")
    except Exception as e:
        await test_msg.edit_text(f"❌ **An unexpected error occurred:**\n\n`{str(e)}`")


@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(client, message: Message):
    if not s3_client:
        await message.reply_text("❌ **Cloud storage is not configured.** Please contact the bot owner.")
        return
        
    media = message.document or message.video or message.audio
    if not media: return

    file_name = media.file_name
    file_size = media.file_size
    
    if file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File size exceeds the 4GB limit.")
        return

    download_start_time = time.time()
    progress_msg = await message.reply_text(f"📥 **Preparing to download...**\n`{file_name}`")
    
    try:
        # The download progress callback is already async-safe with Pyrogram
        file_path = await client.download_media(
            message,
            progress=progress_callback,
            progress_args=(progress_msg, "Downloading from Telegram", download_start_time)
        )
    except Exception as e:
        logger.error(f"Failed to download from Telegram: {e}")
        await progress_msg.edit_text(f"❌ **Error downloading file:**\n`{e}`")
        return

    await progress_msg.edit_text("✅ **Download complete!**\n\n⏳ **Uploading to cloud storage...**")
    
    # --- FIXED: Non-blocking upload ---
    wasabi_key = f"{message.from_user.id}/{int(time.time())}-{file_name}"
    
    try:
        # We run the blocking s3_client.upload_file in a separate thread
        # to prevent it from freezing the whole bot.
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            wasabi_key
        )
    except Exception as e:
        logger.error(f"Failed to upload to Wasabi: {e}")
        await progress_msg.edit_text(f"❌ **Error uploading file to Wasabi:**\n`{e}`")
        os.remove(file_path)
        return
    
    # Generate pre-signed URL (this is a fast, non-blocking operation)
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_key},
        ExpiresIn=604800  # 7 days
    )
        
    # Save file info to database
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO files (file_id, user_id, file_name, file_size, wasabi_key) VALUES (?, ?, ?, ?, ?)",
            (media.file_unique_id, message.from_user.id, file_name, file_size, wasabi_key)
        )
        conn.commit()

    # Send backup to channel if configured
    if STORAGE_CHANNEL_ID != 0:
        try:
            await message.forward(STORAGE_CHANNEL_ID)
        except Exception as e:
            logger.warning(f"Could not forward file to storage channel {STORAGE_CHANNEL_ID}: {e}")

    # Send success message with links
    vlc_link = f"vlc://{presigned_url}"
    mx_player_link = f"intent:{presigned_url}#Intent;package=com.mxtech.videoplayer.ad;end"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🖥️ Direct Link", url=presigned_url)],
        [
            InlineKeyboardButton("▶️ MX Player", url=mx_player_link),
            InlineKeyboardButton("🟠 VLC Player", url=vlc_link)
        ]
    ])
    
    await progress_msg.edit_text(
        f"✅ **File Uploaded!**\n\n"
        f"**File:** `{file_name}`\n"
        f"**Size:** {human_readable_size(file_size)}\n\n"
        f"Links are valid for 7 days.",
        reply_markup=keyboard
    )
    
    os.remove(file_path)

@app.on_message(filters.command("list"))
async def list_files_command(client, message):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT file_name, file_size FROM files WHERE user_id = ? ORDER BY id DESC", (message.from_user.id,))
        files = cursor.fetchall()

    if not files:
        await message.reply_text("You haven't uploaded any files yet.")
        return

    response_text = "**Your Uploaded Files:**\n\n"
    for i, (file_name, file_size) in enumerate(files[:20], 1): # Show latest 20 files
        response_text += f"**{i}.** `{file_name}` ({human_readable_size(file_size)})\n"
    
    await message.reply_text(response_text)


async def main():
    logger.info("Initializing Database...")
    init_db()
    logger.info("Starting Bot...")
    await app.start()
    logger.info(f"Bot started as @{(await app.get_me()).username}")
    await idle() # Keep the bot running until it's stopped
    logger.info("Stopping Bot...")
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())

