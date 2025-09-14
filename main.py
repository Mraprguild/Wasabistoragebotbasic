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

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait

# --- Load Environment Variables ---
load_dotenv()

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", 0))

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
    logger.info("Database initialized successfully.")

init_db()

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
except (NoCredentialsError, PartialCredentialsError) as e:
    logger.error(f"Wasabi credentials not found or incomplete: {e}")
    s3_client = None

# --- Pyrogram Bot Client ---
app = Client("file_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Helper Functions ---
def human_readable_size(size_bytes):
    """Converts bytes to a human-readable format."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

async def progress_callback(current, total, message, action, start_time):
    """Updates the progress message."""
    now = time.time()
    elapsed = now - start_time
    if elapsed == 0:
        elapsed = 0.01  # Avoid division by zero

    speed = current / elapsed
    percentage = current * 100 / total
    
    progress_bar = "[{0}{1}]".format(
        '█' * int(percentage / 5),
        ' ' * (20 - int(percentage / 5))
    )

    try:
        await message.edit_text(
            f"**{action}**\n"
            f"{progress_bar} **{percentage:.1f}%**\n"
            f"**Processed:** {human_readable_size(current)} of {human_readable_size(total)}\n"
            f"**Speed:** {human_readable_size(speed)}/s"
        )
    except FloodWait as e:
        await asyncio.sleep(e.x)
    except Exception as e:
        logger.warning(f"Error updating progress: {e}")

# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "**Welcome to the File Storage Bot!** 🗂️\n\n"
        "I can store your files (up to 4GB) in secure Wasabi cloud storage and provide you with fast streaming and download links.\n\n"
        "**How to use:**\n"
        "1. Simply send any file to me.\n"
        "2. I will upload it and give you a shareable link.\n\n"
        "Use /help to see all available commands."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    await message.reply_text(
        "**📚 Bot Commands Guide**\n\n"
        "`/start` - Show the welcome message.\n"
        "`/upload` - You can just send any file directly.\n"
        "`/list` - List your uploaded files.\n"
        "`/test` - Test the connection to Wasabi cloud storage.\n"
        "`/setchannel <channel_id>` - (Owner only) Set a backup channel.\n"
        "`/help` - Display this help message."
    )

@app.on_message(filters.command("test") & filters.user(OWNER_ID))
async def test_wasabi_command(client, message):
    if not s3_client:
        await message.reply_text("❌ **Wasabi client is not configured.** Check your environment variables.")
        return
        
    test_msg = await message.reply_text("⏳ **Testing Wasabi connection...**")
    try:
        s3_client.list_buckets()
        await test_msg.edit_text(f"✅ **Connection Successful!**\n\n**Bucket:** `{WASABI_BUCKET}`\n**Region:** `{WASABI_REGION}`")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        await test_msg.edit_text(f"❌ **Connection Failed!**\n\n**Error:** `{error_code}`\n\nPlease check your credentials and bucket permissions.")
    except Exception as e:
        await test_msg.edit_text(f"❌ **An unexpected error occurred:**\n\n`{str(e)}`")


@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(client, message: Message):
    if not s3_client:
        await message.reply_text("❌ **Cloud storage is not configured.** Please contact the bot owner.")
        return
        
    media = message.document or message.video or message.audio
    file_name = media.file_name
    file_size = media.file_size
    
    if file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File size exceeds the 4GB limit.")
        return

    # 1. Download from Telegram
    download_start_time = time.time()
    progress_msg = await message.reply_text(f"📥 **Starting download from Telegram...**\n`{file_name}`")
    
    try:
        file_path = await client.download_media(
            message,
            progress=progress_callback,
            progress_args=(progress_msg, "Downloading from Telegram", download_start_time)
        )
    except Exception as e:
        logger.error(f"Failed to download from Telegram: {e}")
        await progress_msg.edit_text(f"❌ **Error downloading file from Telegram:**\n`{e}`")
        return

    await progress_msg.edit_text("✅ **Download complete!**\n\n⏳ **Now uploading to cloud storage...**")
    
    # 2. Upload to Wasabi
    upload_start_time = time.time()
    wasabi_key = f"{message.from_user.id}/{file_name}" # Unique key for the file in Wasabi
    
    try:
        s3_client.upload_file(
            file_path,
            WASABI_BUCKET,
            wasabi_key,
            Callback=lambda bytes_transferred: asyncio.run(
                progress_callback(bytes_transferred, file_size, progress_msg, "Uploading to Wasabi", upload_start_time)
            )
        )
    except Exception as e:
        logger.error(f"Failed to upload to Wasabi: {e}")
        await progress_msg.edit_text(f"❌ **Error uploading file to Wasabi:**\n`{e}`")
        os.remove(file_path) # Clean up downloaded file
        return
    
    # 3. Generate pre-signed URL for access
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_key},
            ExpiresIn=604800  # 7 days
        )
    except Exception as e:
        logger.error(f"Failed to generate pre-signed URL: {e}")
        await progress_msg.edit_text("❌ **Could not generate a shareable link.**")
        os.remove(file_path)
        return
        
    # 4. Save file info to database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files (file_id, user_id, file_name, file_size, wasabi_key) VALUES (?, ?, ?, ?, ?)",
        (media.file_unique_id, message.from_user.id, file_name, file_size, wasabi_key)
    )
    conn.commit()
    conn.close()

    # 5. Send file backup to channel if configured
    if STORAGE_CHANNEL_ID != 0:
        try:
            await message.forward(STORAGE_CHANNEL_ID)
        except Exception as e:
            logger.warning(f"Could not forward file to storage channel: {e}")

    # 6. Send success message with links
    # Note: MX Player intent links are tricky and might not work universally.
    # A web player link is more reliable.
    web_player_link = f"https://vjs.zencdn.net/v/oceans.mp4?url={presigned_url}" # Simple web player example
    vlc_link = f"vlc://{presigned_url}"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🖥️ Stream / Download", url=presigned_url)],
        [
            InlineKeyboardButton("▶️ MX Player", url=f"intent:{presigned_url}#Intent;package=com.mxtech.videoplayer.ad;end"),
            InlineKeyboardButton("🟠 VLC Player", url=vlc_link)
        ]
    ])
    
    await progress_msg.edit_text(
        f"✅ **File uploaded successfully!**\n\n"
        f"**File Name:** `{file_name}`\n"
        f"**File Size:** {human_readable_size(file_size)}\n\n"
        f"This link is valid for 7 days.",
        reply_markup=keyboard
    )
    
    # 7. Clean up the local file
    os.remove(file_path)

@app.on_message(filters.command("list"))
async def list_files_command(client, message):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT file_name, file_size FROM files WHERE user_id = ?", (message.from_user.id,))
    files = cursor.fetchall()
    conn.close()

    if not files:
        await message.reply_text("You haven't uploaded any files yet.")
        return

    response_text = "**Your Uploaded Files:**\n\n"
    for i, (file_name, file_size) in enumerate(files, 1):
        response_text += f"{i}. `{file_name}` - ({human_readable_size(file_size)})\n"
    
    await message.reply_text(response_text)


async def main():
    logger.info("Bot is starting...")
    await app.start()
    logger.info("Bot started successfully!")
    await asyncio.Event().wait() # Keep the bot running

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot is shutting down...")
