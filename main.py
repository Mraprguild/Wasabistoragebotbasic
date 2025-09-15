import os
import time
import math
import boto3
import asyncio
import re
import signal
import atexit
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from botocore.exceptions import NoCredentialsError, ClientError
from pyrogram.errors import FloodWait
from boto3.s3.transfer import TransferConfig

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

# --- Boto3 Transfer Configuration for TURBO SPEED ---
# This enables multipart transfers and uses multiple threads for significant speed boosts.
transfer_config = TransferConfig(
    multipart_threshold=25 * 1024 * 1024,  # Start multipart for files > 25MB
    max_concurrency=20,                     # Use up to 20 parallel threads
    multipart_chunksize=8 * 1024 * 1024,    # 8MB chunks
    use_threads=True
)

# --- Initialize Boto3 Client for Wasabi ---
wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY
)

# --- Rate limiting ---
user_limits = {}
MAX_REQUESTS_PER_MINUTE = 5
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

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

def sanitize_filename(filename):
    """Remove potentially dangerous characters from filenames"""
    # Keep only alphanumeric, spaces, dots, hyphens, and underscores
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
    # Limit length to avoid issues
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    return filename

def cleanup():
    """Clean up temporary files on exit"""
    for folder in ['.', './downloads']:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                if file.endswith('.tmp') or file.startswith('pyrogram'):
                    try:
                        os.remove(os.path.join(folder, file))
                    except:
                        pass

atexit.register(cleanup)

async def check_rate_limit(user_id):
    """Check if user has exceeded rate limits"""
    current_time = time.time()
    
    if user_id not in user_limits:
        user_limits[user_id] = []
    
    # Remove requests older than 1 minute
    user_limits[user_id] = [t for t in user_limits[user_id] if current_time - t < 60]
    
    if len(user_limits[user_id]) >= MAX_REQUESTS_PER_MINUTE:
        return False
    
    user_limits[user_id].append(current_time)
    return True

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Asynchronously reports progress of a background task."""
    while status['running']:
        percentage = (status['seen'] / total_size) * 100 if total_size > 0 else 0
        percentage = min(percentage, 100)

        elapsed_time = time.time() - start_time
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime((total_size - status['seen']) / speed)) if speed > 0 else "N/A"
        
        progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), ' ' * (10 - int(percentage / 10)))
        
        # Escape markdown characters to prevent parsing errors
        escaped_task = task.replace('*', '×').replace('_', '＿').replace('`', '´')
        
        text = (
            f"**{escaped_task}**\n"
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
            pass  # Ignore other edit errors
        await asyncio.sleep(3)  # Update every 3 seconds

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations."""
    try:
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or time.time() - pyrogram_progress_callback.last_edit_time > 3:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            
            # Escape markdown characters to prevent parsing errors
            escaped_task = task.replace('*', '×').replace('_', '＿').replace('`', '´')
            
            text = f"**{escaped_task}** {percentage:.2f}%"
            message.edit_text(text)
            pyrogram_progress_callback.last_edit_time = time.time()
    except Exception:
        pass


# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "Hello! I am a **Turbo-Speed** Wasabi storage bot.\n\n"
        "I use parallel processing to make transfers incredibly fast.\n\n"
        "➡️ **To upload:** Just send me any file.\n"
        "⬅️ **To download:** Use `/download <file_name>`.\n"
        "📋 **To list files:** Use `/list`.\n\n"
        "Generated links are direct streamable links compatible with players like VLC & MX Player."
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers."""
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    # Check file size limit
    if hasattr(media, 'file_size') and media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
        return

    file_path = None
    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        await status_message.edit_text("Downloading from Telegram...")
        file_path = await message.download(progress=pyrogram_progress_callback, progress_args=(status_message, time.time(), "Downloading"))
        
        file_name = sanitize_filename(os.path.basename(file_path))
        status = {'running': True, 'seen': 0}
        
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, media.file_size, f"Uploading {file_name} (Turbo)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_callback,
            Config=transfer_config  # <-- TURBO SPEED ENABLED
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400) # 24 hours
        
        # Escape markdown in the URL display
        display_url = presigned_url.replace('_', '＿').replace('*', '×').replace('`', '´')
        
        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Streamable Link (24h expiry):**\n`{display_url}`"
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using multipart transfers."""
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_name_in_wasabi>`")
        return

    file_name = " ".join(message.command[1:])
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True)
    
    status_message = await message.reply_text(f"Searching for `{file_name}`...", quote=True)

    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get('ContentLength', 0))

        # Check file size limit
        if total_size > MAX_FILE_SIZE:
            await status_message.edit_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
            return

        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount
            
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading {file_name} (Turbo)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            file_name,
            local_file_path,
            Callback=boto_callback,
            Config=transfer_config  # <-- TURBO SPEED ENABLED
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()
        
        await status_message.edit_text("Uploading to Telegram...")
        await message.reply_document(
            document=local_file_path,
            caption=f"✅ **Download Complete:** `{file_name}`",
            progress=pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "Uploading to Telegram")
        )
        
        await status_message.delete()

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            await status_message.edit_text(f"❌ **Error:** File not found in Wasabi: `{file_name}`")
        elif error_code == '403':
            await status_message.edit_text("❌ **Error:** Access denied. Check your Wasabi credentials.")
        elif error_code == 'NoSuchBucket':
            await status_message.edit_text("❌ **Error:** Bucket does not exist.")
        else:
            await status_message.edit_text(f"❌ **S3 Error:** {error_code} - {str(e)}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in the Wasabi bucket"""
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return
        
    try:
        response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET)
        if 'Contents' not in response:
            await message.reply_text("No files found in the bucket.")
            return
        
        files = [obj['Key'] for obj in response['Contents']]
        files_list = "\n".join([f"• `{file}`" for file in files[:20]])  # Show first 20 files
        
        if len(files) > 20:
            files_list += f"\n\n...and {len(files) - 20} more files"
        
        await message.reply_text(f"**Files in bucket:**\n\n{files_list}")
    
    except Exception as e:
        await message.reply_text(f"❌ Error listing files: {str(e)}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with TURBO-SPEED settings...")
    app.run()
    print("Bot has stopped.")
