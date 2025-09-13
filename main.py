import os
import time
import math
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# --- Configuration ---
# Load environment variables from your system
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# Wasabi S3-Compatible Storage Configuration
WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY", "")
WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY", "")
WASABI_BUCKET = os.environ.get("WASABI_BUCKET", "")
WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1") # Default to us-east-1 if not set

# Check for missing essential variables
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET]):
    print("ERROR: Missing one or more required environment variables.")
    exit(1)

# Construct the Wasabi endpoint URL
WASABI_ENDPOINT_URL = f'https://s3.{WASABI_REGION}.wasabisys.com'

# --- Bot Initialization ---
# Initialize the Pyrogram Client
app = Client(
    "wasabi_storage_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Initialize the Boto3 S3 Client for Wasabi
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=WASABI_ENDPOINT_URL,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=WASABI_REGION
    )
    print("Boto3 S3 client initialized successfully.")
except (NoCredentialsError, PartialCredentialsError) as e:
    print(f"Error initializing Boto3 client: {e}. Please check your Wasabi credentials.")
    exit(1)


# --- Helper Functions ---
def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB, etc.)."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# A dictionary to prevent spamming Telegram with progress updates
last_update_time = {}

async def progress_callback(current, total, message: Message, action: str):
    """
    Handles progress updates for both downloads and uploads.
    Edits the message to show the current progress.
    """
    user_id = message.chat.id
    now = time.time()

    # Update only once per second to avoid hitting Telegram API limits
    if user_id in last_update_time and (now - last_update_time[user_id]) < 1:
        return
    last_update_time[user_id] = now

    percentage = current * 100 / total
    speed = current / (now - message.date.timestamp()) if (now - message.date.timestamp()) > 0 else 0
    
    # Create the progress bar string
    progress_bar = "[{0}{1}]".format(
        '█' * int(percentage / 5),
        ' ' * (20 - int(percentage / 5))
    )

    # Format the status message
    status_text = (
        f"**{action}**\n"
        f"{progress_bar} {percentage:.2f}%\n"
        f"**Done:** {format_bytes(current)}\n"
        f"**Total:** {format_bytes(total)}\n"
        f"**Speed:** {format_bytes(speed)}/s"
    )

    try:
        await message.edit_text(status_text)
    except Exception as e:
        # Handle cases where the message might not be editable
        print(f"Error updating progress: {e}")


class WasabiUploadProgress:
    """
    A callback class for Boto3 to track upload progress and update the Telegram message.
    It is called from a separate thread, so it needs to schedule the async update on the main event loop.
    """
    def __init__(self, message: Message, total_size: int, action: str, loop):
        self._message = message
        self._total_size = total_size
        self._seen_so_far = 0
        self._action = action
        self._loop = loop

    def __call__(self, bytes_amount):
        """The callback method invoked by boto3."""
        self._seen_so_far += bytes_amount
        # Schedule the async progress_callback to run on the main event loop
        asyncio.run_coroutine_threadsafe(
            progress_callback(
                self._seen_so_far, self._total_size, self._message, self._action
            ),
            self._loop
        )


# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "Hello! I'm your Telegram to Wasabi Storage bot.\n\n"
        "Send me any file (up to 5GB), and I will upload it to Wasabi and provide you with a direct, shareable link."
    )

# --- Bot File Handling Logic ---
@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    """Handles incoming files (documents, videos, audios)."""
    
    media = message.document or message.video or message.audio
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    file_name = media.file_name
    file_size = media.file_size
    
    # Check if the file size is within a reasonable limit (e.g., 5GB)
    if file_size > 5 * 1024 * 1024 * 1024:
        await message.reply_text("Sorry, the file is too large. The maximum supported size is 5 GB.")
        return
        
    status_msg = await message.reply_text("Preparing to process your file...")
    local_file_path = None

    try:
        # 1. Download from Telegram
        await status_msg.edit_text(f"Downloading `{file_name}` from Telegram...")
        
        start_time = time.time()
        
        local_file_path = await app.download_media(
            message=message,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading...")
        )
        
        download_time = time.time() - start_time
        await status_msg.edit_text(
            f"Downloaded `{file_name}` in {download_time:.2f} seconds. Now uploading to Wasabi..."
        )
        
        # 2. Upload to Wasabi Storage
        upload_start_time = time.time()
        
        # Get the current event loop to schedule callbacks from the other thread
        loop = asyncio.get_event_loop()
        
        # Create an instance of the progress callback class for boto3
        upload_progress = WasabiUploadProgress(status_msg, file_size, "Uploading...", loop)
        
        # Run the synchronous boto3 upload in a separate thread to keep it non-blocking
        await loop.run_in_executor(
            None,  # Use default executor
            # Use a lambda to pass keyword arguments correctly
            lambda: s3_client.upload_file(
                local_file_path,
                WASABI_BUCKET,
                file_name,
                Callback=upload_progress
            )
        )
        
        upload_time = time.time() - upload_start_time
        await status_msg.edit_text(f"Uploaded to Wasabi in {upload_time:.2f} seconds. Generating link...")

        # 3. Generate a pre-signed URL for sharing
        # This link will be valid for 24 hours (86400 seconds)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=86400
        )
        
        # 4. Send the final link to the user
        await status_msg.edit_text(
            f"**File Upload Complete!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** {format_bytes(file_size)}\n\n"
            f"**Your direct link (valid for 24 hours):**\n"
            f"`{presigned_url}`\n\n"
            f"This link can be used with media players like VLC and MX Player."
        )

    except Exception as e:
        print(f"An error occurred: {e}")
        await status_msg.edit_text(f"An error occurred during the process: {str(e)}")
        
    finally:
        # 5. Clean up the downloaded file
        if local_file_path and os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Cleaned up temporary file: {local_file_path}")


# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting...")
    app.run()
    print("Bot has stopped.")
