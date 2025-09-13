import os
import asyncio
import time
import mimetypes
from urllib.parse import quote_plus
from humanize import natural_size

# Third-party libraries
import aiofiles
from aiobotocore.session import get_session
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait

# --- Configuration ---
# Load environment variables with sanity checks.
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")

# --- Constants ---
MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB
DOWNLOAD_DIR = "./downloads/"
WASABI_ENDPOINT_URL = f"https://s3.{WASABI_REGION}.wasabisys.com" if WASABI_REGION else None
PRESIGNED_URL_EXPIRATION = 60 * 60 * 24 * 7  # 7 days in seconds

# --- Bot Initialization ---
app = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- Helper Functions ---

async def progress_callback(current: int, total: int, message: Message, start_time: float, action: str):
    """
    Updates the message with the current progress of an operation (download/upload).
    Uses a 1.5-second interval to avoid hitting Telegram API rate limits.
    """
    now = time.time()
    if now - start_time > 1.5:
        elapsed = now - start_time
        speed = current / elapsed
        percentage = current * 100 / total
        progress_str = (
            f"**{action}**\n\n"
            f"[{'●' * int(percentage / 10)}{'○' * (10 - int(percentage / 10))}] {percentage:.2f}%\n\n"
            f"**Processed:** {natural_size(current)} of {natural_size(total)}\n"
            f"**Speed:** {natural_size(speed)}/s"
        )
        try:
            await message.edit_text(progress_str)
            return now
        except FloodWait as e:
            # If we are rate-limited, wait for the specified time
            await asyncio.sleep(e.x)
        except Exception:
            # Handle cases where the message might not have changed or other issues
            pass
    return start_time

# --- Bot Command Handlers ---

@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    """Greets the user when they start the bot."""
    await message.reply_text(
        "**Welcome to the Wasabi Upload Bot!**\n\n"
        "Send me any file (up to 5GB), and I'll upload it to Wasabi storage. "
        "You'll receive a high-speed, direct link that you can use for streaming or downloading."
    )

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(client: Client, message: Message):
    """Handles incoming files, orchestrating the download, upload, and link generation."""
    media = message.document or message.video or message.audio
    if not media:
        await message.reply_text("Unsupported file type. Please send a document, video, or audio file.")
        return

    file_name = media.file_name
    file_size = media.file_size

    if file_size > MAX_FILE_SIZE:
        await message.reply_text(f"File size limit is {natural_size(MAX_FILE_SIZE)}. Your file is {natural_size(file_size)}.")
        return

    status_msg = await message.reply_text("Parsing your request...")
    download_path = os.path.join(DOWNLOAD_DIR, file_name)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # 1. Download from Telegram with Progress
    last_update_time = time.time()
    try:
        async def download_progress(current, total):
            nonlocal last_update_time
            last_update_time = await progress_callback(current, total, status_msg, last_update_time, "Downloading from Telegram...")
        
        await client.download_media(message=message, file_name=download_path, progress=download_progress)
    except Exception as e:
        await status_msg.edit_text(f"❌ **Download Failed!**\n\n**Error:** {e}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return

    # 2. Upload to Wasabi
    await status_msg.edit_text("Download complete. Now uploading to Wasabi...\n\nThis may take a while for large files.")
    
    session = get_session()
    try:
        async with session.create_client(
            's3',
            region_name=WASABI_REGION,
            endpoint_url=WASABI_ENDPOINT_URL,
            aws_access_key_id=WASABI_ACCESS_KEY,
            aws_secret_access_key=WASABI_SECRET_KEY
        ) as s3_client:
            
            mime_type, _ = mimetypes.guess_type(file_name)
            mime_type = mime_type or "application/octet-stream"

            async with aiofiles.open(download_path, 'rb') as f:
                await s3_client.put_object(
                    Bucket=WASABI_BUCKET,
                    Key=file_name,
                    Body=f,
                    ContentType=mime_type
                )
            
            # 3. Generate Pre-signed URL
            presigned_url = await s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
                ExpiresIn=PRESIGNED_URL_EXPIRATION
            )
    except Exception as e:
        await status_msg.edit_text(f"❌ **Wasabi Upload Failed!**\n\n**Error:** {e}")
        return
    finally:
        # 5. Cleanup the downloaded file
        if os.path.exists(download_path):
            os.remove(download_path)

    # 4. Send the result with streaming buttons
    encoded_file_name = quote_plus(file_name)
    # Generic intent that works well for MX Player/VLC and other Android players
    generic_player_url = f"intent:{presigned_url}#Intent;type={mime_type};scheme=https;S.title={encoded_file_name};end"
    
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔗 Direct Link", url=presigned_url)],
            [InlineKeyboardButton("▶️ Open in Media Player (Mobile)", url=generic_player_url)]
        ]
    )

    await status_msg.edit_text(
        f"✅ **Upload Successful!**\n\n"
        f"**File:** `{file_name}`\n"
        f"**Size:** {natural_size(file_size)}\n\n"
        f"_This link is valid for 7 days._",
        reply_markup=buttons,
        disable_web_page_preview=True
    )


async def main():
    """Main function to start the bot and check for configuration."""
    print("Checking environment variables...")
    required_vars = [
        "API_ID", "API_HASH", "BOT_TOKEN", "WASABI_ACCESS_KEY", 
        "WASABI_SECRET_KEY", "WASABI_BUCKET", "WASABI_REGION"
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Error: Missing required environment variables: {', '.join(missing_vars)}")
        return
    
    print("Starting bot...")
    await app.start()
    user = await app.get_me()
    print(f"Bot started as @{user.username}")
    await asyncio.Event().wait()  # Keep the bot running indefinitely

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot is stopping...")
