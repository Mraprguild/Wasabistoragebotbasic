import os
import time
import math
import boto3
import asyncio
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from botocore.exceptions import NoCredentialsError, ClientError
from pyrogram.errors import FloodWait

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- Initialize Pyrogram Client ---
# Increased workers for better performance with multiple concurrent tasks.
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=20)

# --- Initialize Boto3 Client for Wasabi ---
# This is a synchronous client, but we will use it in a non-blocking way.
wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY
)

# --- Helper Functions & Classes ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power and t_n < len(power_dict) -1:
        size /= power
        t_n += 1
    return "{:.2f}".format(size) + power_dict[t_n]

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Asynchronously reports progress of a background task."""
    while status['running']:
        percentage = (status['seen'] / total_size) * 100 if total_size > 0 else 0
        percentage = min(percentage, 100)

        elapsed_time = time.time() - start_time
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime((total_size - status['seen']) / speed)) if speed > 0 else "N/A"
        
        progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), ' ' * (10 - int(percentage / 10)))
        
        text = (
            f"**{task}...**\n"
            f"{progress_bar} {percentage:.2f}%\n"
            f"**Done:** {humanbytes(status['seen'])} of {humanbytes(total_size)}\n"
            f"**Speed:** {humanbytes(speed)}/s\n"
            f"**ETA:** {eta}"
        )
        try:
            await message.edit_text(text)
        except FloodWait as e:
            await asyncio.sleep(e.x)
        except Exception:
            pass # Ignore other edit errors
        await asyncio.sleep(3) # Update every 3 seconds

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations."""
    # This is a synchronous wrapper for the async logic, used for Pyrogram's download
    # In a real-world high-concurrency bot, this would also be made fully async.
    try:
        # A simple synchronous edit is okay here as Pyrogram handles its own loop.
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or time.time() - pyrogram_progress_callback.last_edit_time > 3:
            percentage = min((current * 100 / total), 100)
            text = f"**{task}...** {percentage:.2f}%"
            message.edit_text(text)
            pyrogram_progress_callback.last_edit_time = time.time()
    except Exception:
        pass


# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "Hello! I am a high-speed Wasabi storage bot.\n\n"
        "➡️ **To upload:** Just send me any file. I can handle files up to Telegram's limit (2GB for standard users, 4GB for Premium).\n"
        "⬅️ **To download:** Use `/download <file_name>`.\n\n"
        "Generated links are direct streamable links compatible with players like VLC & MX Player."
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi in a non-blocking way."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    file_path = None
    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        # Step 1: Download from Telegram (this is a blocking call in Pyrogram but very fast on servers)
        await status_message.edit_text("Downloading from Telegram...")
        file_path = await message.download(progress=pyrogram_progress_callback, progress_args=(status_message, time.time(), "Downloading"))
        
        # Step 2: Upload to Wasabi non-blockingly
        file_name = os.path.basename(file_path)
        status = {'running': True, 'seen': 0}
        
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, media.file_size, f"Uploading `{file_name}`", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_callback
        )
        
        status['running'] = False
        reporter_task.cancel()

        # Step 3: Generate link and send success message
        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400) # 24 hours
        
        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Streamable Link (24h expiry):**\n`{presigned_url}`\n\n"
            f"This link works with VLC, MX Player, and other media players."
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi in a non-blocking way."""
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_name_in_wasabi>`")
        return

    file_name = " ".join(message.command[1:])
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True)
    
    status_message = await message.reply_text(f"Searching for `{file_name}` in Wasabi...", quote=True)

    try:
        # Step 1: Get file metadata from Wasabi
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get('ContentLength', 0))

        # Step 2: Download from Wasabi non-blockingly
        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount
            
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading `{file_name}`", time.time())
        )
        
        await asyncio.to_thread(s3_client.download_file, WASABI_BUCKET, file_name, local_file_path, Callback=boto_callback)
        
        status['running'] = False
        reporter_task.cancel()
        
        # Step 3: Upload to Telegram
        await status_message.edit_text("Uploading to Telegram...")
        await client.send_document(
            chat_id=message.chat.id,
            document=local_file_path,
            progress=pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "Uploading")
        )
        
        await status_message.delete()

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            await status_message.edit_text(f"❌ **Error:** File not found in Wasabi: `{file_name}`")
        else:
            await status_message.edit_text(f"❌ **S3 Client Error:** {e}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with high-performance settings...")
    app.run()
    print("Bot has stopped.")
