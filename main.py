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
PORT = int(os.environ.get("PORT", 8080))  # Render provides PORT environment variable
WELCOME_IMAGE_URL = "https://i.ibb.co/yY5W2kF/Extreme-Speed-Wasabi-Storage-Bot.png"

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- Initialize Pyrogram Client ---
# Added in_memory=True and adjusted for Render compatibility
app = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=20,
    in_memory=True  # Recommended for server environments
)

# --- Boto3 Transfer Configuration for EXTREME SPEED ---
transfer_config = TransferConfig(
    multipart_threshold=25 * 1024 * 1024,
    max_concurrency=40,
    multipart_chunksize=16 * 1024 * 1024,
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
    try:
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or time.time() - pyrogram_progress_callback.last_edit_time > 3:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            text = f"**{task}...** {percentage:.2f}%"
            message.edit_text(text)
            pyrogram_progress_callback.last_edit_time = time.time()
    except Exception:
        pass

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption=(
            "Hello! I am an **Extreme-Speed** Wasabi storage bot.\n\n"
            "I use aggressive parallel processing to make transfers incredibly fast.\n\n"
            "➡️ **To upload:** Just send me any file.\n"
            "⬅️ **To download:** Use `/download <file_name>`.\n"
            "📂 **To list files:** Use `/list`.\n\n"
            "Generated links are direct streamable links compatible with players like VLC & MX Player."
        )
    )

@app.on_message(filters.command("list"))
async def list_files_handler(client, message: Message):
    """Handles the /list command to show files in the Wasabi bucket."""
    status_message = await message.reply_text("🔎 Fetching file list from Wasabi...", quote=True)
    try:
        response = await asyncio.to_thread(
            s3_client.list_objects_v2, Bucket=WASABI_BUCKET
        )

        if 'Contents' in response:
            files = response['Contents']
            # Sort files by last modified date, newest first
            sorted_files = sorted(files, key=lambda x: x['LastModified'], reverse=True)

            file_list_text = "**Files in your Wasabi Bucket:**\n\n"
            for file in sorted_files:
                file_line = f"📄 `{file['Key']}` ({humanbytes(file['Size'])})\n"
                if len(file_list_text) + len(file_line) > 4096:
                    file_list_text += "\n...and more. List truncated."
                    break
                file_list_text += file_line

            await status_message.edit_text(file_list_text)
        else:
            await status_message.edit_text("✅ Your Wasabi bucket is empty.")

    except ClientError as e:
        await status_message.edit_text(f"❌ **S3 Client Error:** Could not list files. Check bucket name and permissions. Details: {e}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    file_path = None
    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        await status_message.edit_text("Downloading from Telegram...")
        file_path = await message.download(progress=pyrogram_progress_callback, progress_args=(status_message, time.time(), "Downloading"))

        file_name = os.path.basename(file_path)
        status = {'running': True, 'seen': 0}

        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, media.file_size, f"Uploading `{file_name}` (Extreme Speed)", time.time())
        )

        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_callback,
            Config=transfer_config
        )

        status['running'] = False
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400)

        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Streamable Link (24h expiry):**\n`{presigned_url}`"
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using multipart transfers."""
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

        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading `{file_name}` (Extreme Speed)", time.time())
        )

        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            file_name,
            local_file_path,
            Callback=boto_callback,
            Config=transfer_config
        )

        status['running'] = False
        reporter_task.cancel()

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

# --- Health Check Endpoint for Render ---
from aiohttp import web

async def health_check(request):
    return web.Response(text="OK")

# --- Main Execution with Render Support ---
if __name__ == "__main__":
    print("Bot is starting with EXTREME-SPEED settings...")

    # Start a simple web server for health checks (required by Render)
    async def start_web_server():
        app_web = web.Application()
        app_web.router.add_get('/health', health_check)
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        print(f"Health check server started on port {PORT}")

    # Start both the Telegram bot and the health check server
    loop = asyncio.get_event_loop()
    loop.create_task(start_web_server())

    # Run the Pyrogram client
    app.run()
    print("Bot has stopped.")
