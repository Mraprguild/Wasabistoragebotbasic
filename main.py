import os
import time
import math
import boto3
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
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Initialize Boto3 Client for Wasabi ---
wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'

s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY
)

# --- Helper Functions ---
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

# --- Progress Callback ---
def progress_callback(current, total, message, start_time, task):
    """Handles progress bar updates for both uploads and downloads."""
    if total == 0:
        return
    elapsed_time = time.time() - start_time
    if elapsed_time == 0:
        return

    speed = current / elapsed_time
    percentage = current * 100 / total
    
    # Ensure percentage doesn't exceed 100
    percentage = min(percentage, 100)

    progress_bar = "[{0}{1}]".format(
        '█' * int(percentage / 10),
        ' ' * (10 - int(percentage / 10))
    )
    
    eta = "N/A"
    if speed > 0:
        time_to_complete = (total - current) / speed
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime(time_to_complete))

    try:
        # Edit the message with progress updates
        text = (
            f"**{task}...**\n"
            f"{progress_bar} {percentage:.2f}%\n"
            f"**Done:** {humanbytes(current)} of {humanbytes(total)}\n"
            f"**Speed:** {humanbytes(speed)}/s\n"
            f"**ETA:** {eta}"
        )
        # To avoid flooding, only edit the message every 2 seconds
        if not hasattr(message, 'last_edit_time') or time.time() - message.last_edit_time > 2:
            message.edit_text(text)
            message.last_edit_time = time.time()
    except FloodWait as e:
        time.sleep(e.x)
    except Exception:
        pass


# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "Hello! I'm a bot that can upload and download files from Wasabi storage.\n\n"
        "➡️ **To upload:** Just send me any file.\n"
        "⬅️ **To download:** Use the command `/download <file_name>`."
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    file_path = None
    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        # Download the file from Telegram
        start_time = time.time()
        file_path = await message.download(
            progress=progress_callback,
            progress_args=(status_message, start_time, "Downloading from Telegram")
        )
        
        await status_message.edit_text("Download complete. Now uploading to Wasabi...")

        # Upload the file to Wasabi
        file_name = os.path.basename(file_path)
        start_upload_time = time.time()

        class ProgressUpdater:
            def __init__(self, total_size):
                self._size = total_size
                self._seen_so_far = 0

            def __call__(self, bytes_amount):
                self._seen_so_far += bytes_amount
                progress_callback(self._seen_so_far, self._size, status_message, start_upload_time, "Uploading to Wasabi")

        s3_client.upload_file(
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=ProgressUpdater(media.file_size)
        )
        
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=3600
        )
        
        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File Name:** `{file_name}`\n"
            f"**Shareable Link (expires in 1 hour):**\n"
            f"`{presigned_url}`"
        )

    except NoCredentialsError:
        await status_message.edit_text("❌ Wasabi credentials not found. Please check your configuration.")
    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi."""
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_name_in_wasabi>`")
        return

    file_name = " ".join(message.command[1:]) # To handle filenames with spaces
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True) # Ensure download directory exists
    
    status_message = await message.reply_text(f"Attempting to download `{file_name}` from Wasabi...", quote=True)

    try:
        meta = s3_client.head_object(Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get('ContentLength', 0))

        start_download_time = time.time()
        
        class ProgressUpdater:
            def __init__(self, total_size):
                self._size = total_size
                self._seen_so_far = 0

            def __call__(self, bytes_amount):
                self._seen_so_far += bytes_amount
                progress_callback(self._seen_so_far, self._size, status_message, start_download_time, "Downloading from Wasabi")

        s3_client.download_file(
            WASABI_BUCKET, 
            file_name, 
            local_file_path,
            Callback=ProgressUpdater(total_size)
        )

        await status_message.edit_text("Download from Wasabi complete. Now uploading to Telegram...")

        start_upload_time = time.time()
        await client.send_document(
            chat_id=message.chat.id,
            document=local_file_path,
            progress=progress_callback,
            progress_args=(status_message, start_upload_time, "Uploading to Telegram")
        )
        
        await status_message.delete()

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            await status_message.edit_text(f"❌ **Error:** File not found in Wasabi bucket: `{file_name}`")
        else:
            await status_message.edit_text(f"❌ **An S3 Client Error occurred:** {e}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)


# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting...")
    app.run()
    print("Bot has stopped.")
