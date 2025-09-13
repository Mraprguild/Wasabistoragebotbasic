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


async def progress_callback(current, total, message, start_time, action):
    """Updates the progress message in Telegram."""
    elapsed_time = time.time() - start_time
    if elapsed_time == 0:
        return
        
    speed = current / elapsed_time
    percentage = current * 100 / total
    progress_bar = "■" * int(percentage / 5) + "□" * (20 - int(percentage / 5))
    
    progress_text = (
        f"**{action}**\n"
        f"├ `{progress_bar}`\n"
        f"├ **Progress:** {percentage:.1f}%\n"
        f"├ **Done:** {humanbytes(current)} of {humanbytes(total)}\n"
        f"├ **Speed:** {humanbytes(speed)}/s\n"
        f"└ **Time Left:** {time.strftime('%Hh %Mm %Ss', time.gmtime(total / speed - elapsed_time)) if speed > 0 else 'N/A'}"
    )
    
    try:
        # Edit message only once per second to avoid API flood waits
        if int(elapsed_time) % 2 == 0:
             await message.edit_text(progress_text)
    except Exception:
        pass


# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    """Handler for the /start command."""
    await message.reply_text("👋 Hello! Send me any file, and I will upload it to Wasabi and give you a streamable link.")

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    """Handles file uploads."""
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
    # Note: Pyrogram with a user account can handle up to 4 GB.
    if file_size > 2 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File is larger than 2 GB, which is the limit for bots.")
        return

    status_message = await message.reply_text(f"📥 Starting download of `{file_name}`...")
    
    # --- Download from Telegram ---
    download_path = f"./downloads/{file_name}"
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    start_time = time.time()
    
    try:
        await app.download_media(
            message=message,
            file_name=download_path,
            progress=progress_callback,
            progress_args=(status_message, start_time, "Downloading")
        )
    except Exception as e:
        await status_message.edit_text(f"❌ **Download Failed:** {e}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return

    await status_message.edit_text(f"✅ Download complete! Now uploading to Wasabi...")

    # --- Upload to Wasabi ---
    start_time = time.time()
    try:
        s3.upload_file(
            Filename=download_path,
            Bucket=WASABI_BUCKET,
            Key=file_name,
            Callback=lambda bytes_transferred: asyncio.run(
                progress_callback(
                    bytes_transferred,
                    file_size,
                    status_message,
                    start_time,
                    "Uploading"
                )
            )
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
            ExpiresIn=604800  # Link valid for 7 days (in seconds)
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
    if os.path.exists(d
ownload_path):
        os.remove(download_path)


# --- Run the Bot ---
if __name__ == "__main__":
    print("🚀 Bot is starting...")
    app.run()
    print("👋 Bot has stopped.")
