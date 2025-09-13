import os
import time
import math
import boto3
import asyncio
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# --- Load Environment Variables ---
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
WASABI_ENDPOINT_URL = f'https://s3.{WASABI_REGION}.wasabisys.com'

# --- Initialize Pyrogram Bot ---
app = Client(
    "wasabi_upload_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- Initialize Boto3 S3 Client for Wasabi ---
try:
    s3 = boto3.client(
        's3',
        endpoint_url=WASABI_ENDPOINT_URL,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=WASABI_REGION
    )
    print("✅ Successfully connected to Wasabi.")
except (NoCredentialsError, PartialCredentialsError) as e:
    print(f"❌ Error: Wasabi credentials not found or incomplete. Please check your .env file. Details: {e}")
    s3 = None
except Exception as e:
    print(f"❌ An unexpected error occurred during Wasabi connection: {e}")
    s3 = None


# --- Helper Functions ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0B"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


async def progress_callback(current, total, message, start_time, action, last_update_time):
    """Updates the progress message in Telegram, throttling to avoid API limits."""
    now = time.time()
    # Update only once every 2 seconds
    if now - last_update_time[0] < 2:
        return

    elapsed_time = now - start_time
    if elapsed_time == 0:
        return

    speed = current / elapsed_time
    percentage = current * 100 / total
    progress_bar = "■" * int(percentage / 5) + "□" * (20 - int(percentage / 5))
    
    time_left_seconds = (total - current) / speed if speed > 0 else 0
    time_left = time.strftime('%Hh %Mm %Ss', time.gmtime(time_left_seconds))

    progress_text = (
        f"**{action}**\n"
        f"├ `{progress_bar}`\n"
        f"├ **Progress:** {percentage:.1f}%\n"
        f"├ **Done:** {humanbytes(current)} of {humanbytes(total)}\n"
        f"├ **Speed:** {humanbytes(speed)}/s\n"
        f"└ **Time Left:** {time_left}"
    )
    
    try:
        await message.edit_text(progress_text)
        last_update_time[0] = now # Update the time of the last successful edit
    except FloodWait as e:
        print(f"FloodWait: sleeping for {e.value} seconds.")
        await asyncio.sleep(e.value)
    except Exception:
        pass


# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    """Handler for the /start command."""
    await message.reply_text("👋 Hello! Send me any file, and I will upload it to Wasabi and give you a streamable link.")

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    """Handles file downloads and uploads."""
    if not s3:
        await message.reply_text("⚠️ **Connection Error:** Could not connect to Wasabi storage. Please check the bot's configuration and logs.")
        return

    media = message.document or message.video or message.audio
    if not media:
        await message.reply_text("This message doesn't contain a file I can handle.")
        return

    file_name = media.file_name
    file_size = media.file_size
    
    # Check if file size exceeds Telegram's bot API limit (2 GB)
    if file_size > 4 * 1024 * 1024 * 1024: # Pyrogram can handle up to 4GB
        await message.reply_text("❌ **Error:** File is larger than 4 GB.")
        return

    status_message = await message.reply_text(f"📥 Starting download of `{file_name}`...")
    
    download_path = f"./downloads/{file_name}"
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    
    # --- Download from Telegram ---
    start_time = time.time()
    last_update_time = [start_time] # Use a list for mutable access in callback
    
    try:
        await app.download_media(
            message=message,
            file_name=download_path,
            progress=progress_callback,
            progress_args=(status_message, start_time, "Downloading", last_update_time)
        )
    except Exception as e:
        await status_message.edit_text(f"❌ **Download Failed:** {e}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return

    await status_message.edit_text(f"✅ Download complete! Now uploading to Wasabi...")

    # --- Upload to Wasabi ---
    start_time = time.time()
    last_update_time[0] = start_time # Reset timer for upload
    
    # This class bridges the synchronous Boto3 callback with our async progress function
    class BotoProgress:
        def __init__(self, message, total_size, start_time, last_update_time_ref):
            self._message = message
            self._total_size = total_size
            self._start_time = start_time
            self._seen_so_far = 0
            self._loop = asyncio.get_running_loop()
            self._last_update_time = last_update_time_ref

        def __call__(self, bytes_transferred):
            self._seen_so_far += bytes_transferred
            asyncio.run_coroutine_threadsafe(
                progress_callback(
                    self._seen_so_far, self._total_size, self._message,
                    self._start_time, "Uploading", self._last_update_time
                ),
                self._loop
            )

    try:
        s3.upload_file(
            Filename=download_path,
            Bucket=WASABI_BUCKET,
            Key=file_name,
            Callback=BotoProgress(status_message, file_size, start_time, last_update_time)
        )
    except Exception as e:
        await status_message.edit_text(f"❌ **Upload Failed:** {e}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return

    # --- Generate Presigned URL ---
    try:
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=604800  # Link valid for 7 days
        )
        final_message = (
            f"✅ **Upload Successful!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"🔗 **Shareable Link:**\n`{presigned_url}`\n\n"
            f"This link will expire in 7 days."
        )
        await status_message.edit_text(final_message)
    except Exception as e:
        await status_message.edit_text(f"❌ **Could not generate link:** {e}")

    # --- Cleanup ---
    if os.path.exists(download_path):
        os.remove(download_path)


# --- Run the Bot ---
if __name__ == "__main__":
    print("🚀 Bot is starting...")
    app.run()
    print("👋 Bot has stopped.")
    
