#!/usr/bin/env python3
import os
import io
import time
import math
import uuid
import tempfile
import traceback
import boto3
import asyncio
import psutil
from datetime import datetime
from dotenv import load_dotenv
from pyrogram import Client, filters, __version__ as pyrogram_version
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from botocore.exceptions import NoCredentialsError, ClientError
from pyrogram.errors import FloodWait
from boto3.s3.transfer import TransferConfig
from aiohttp import web

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
PORT = int(os.environ.get("PORT", 5000))  # Render uses port 5000

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit(1)

# --- Bot Startup Time ---
BOT_START_TIME = time.time()

# --- Initialize Pyrogram Client ---
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=20)

# --- Boto3 Transfer Configuration for TURBO SPEED ---
transfer_config = TransferConfig(
    multipart_threshold=25 * 1024 * 1024,  # Start multipart for files > 25MB
    max_concurrency=20,
    multipart_chunksize=8 * 1024 * 1024,
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

# --- Templates ---
TEMPLATES = {
    "start": (
        "🚀 **Turbo-Speed Wasabi Storage Bot**\n\n"
        "I use parallel processing to make transfers blazing fast!\n\n"
        "➡️ **To upload:** Just send me any file\n"
        "⬅️ **To download:** Use `/download <file_name>`\n"
        "📊 **Check status:** Use `/status`\n\n"
        "Generated links are direct streamable links compatible with players like VLC & MX Player."
    ),
    "upload_success": (
        "✅ **Upload Successful!**\n\n"
        "**File:** `{file_name}`\n"
        "**Size:** {file_size}\n"
        "**Streamable Link (24h expiry):**\n`{presigned_url}`"
    ),
    "error": "❌ **Error:** {error}",
    "processing": "⏳ Processing your request...",
    "downloading": "📥 Downloading from Telegram...",
    "uploading": "📤 Uploading to Wasabi...",
    "searching": "🔍 Searching for `{file_name}`...",
    "file_not_found": "❌ **Error:** File not found in Wasabi: `{file_name}`",
    "status": (
        "🤖 **Bot Status**\n\n"
        "**Uptime:** {uptime}\n"
        "**CPU Usage:** {cpu_usage}%\n"
        "**Memory Usage:** {memory_usage}\n"
        "**Python Version:** {python_version}\n"
        "**Pyrogram Version:** {pyrogram_version}\n\n"
        "**Wasabi Connection:** {wasabi_status}\n"
        "**Telegram Connection:** {telegram_status}\n\n"
        "**Total Files Processed:** {files_processed}\n"
        "**Total Data Transferred:** {data_transferred}"
    )
}

# --- Bot Statistics ---
class BotStats:
    def __init__(self):
        self.files_processed = 0
        self.data_transferred = 0
        self.start_time = time.time()
    
    def add_file(self, size):
        self.files_processed += 1
        self.data_transferred += size
    
    def get_uptime(self):
        uptime_seconds = int(time.time() - self.start_time)
        days, remainder = divmod(uptime_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        else:
            return f"{minutes}m {seconds}s"

# Initialize bot statistics
bot_stats = BotStats()

# --- Utilities ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power and t_n < len(power_dict) - 1:
        size /= power
        t_n += 1
    return "{:.2f}".format(size) + power_dict[t_n]

async def safe_edit(message: Message, text: str):
    """Safely edit a message, handling FloodWait and other transient errors."""
    try:
        await message.edit_text(text)
    except FloodWait as e:
        # wait the required duration, then try once more
        await asyncio.sleep(e.x)
        try:
            await message.edit_text(text)
        except Exception:
            pass
    except Exception:
        # ignore other edit errors (message deleted, not enough rights etc.)
        pass

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Asynchronously reports progress of a background task (upload/download)."""
    try:
        while status.get('running', False):
            seen = status.get('seen', 0)
            percentage = (seen / total_size) * 100 if total_size > 0 else 0
            percentage = min(percentage, 100)

            elapsed_time = max(1e-6, time.time() - start_time)
            speed = seen / elapsed_time
            remaining = max(0, total_size - seen)
            eta_seconds = int(remaining / speed) if speed > 0 else None
            eta = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds)) if eta_seconds is not None else "N/A"

            filled = int(percentage / 10)
            progress_bar = "[{0}{1}]".format('█' * filled, ' ' * (10 - filled))

            text = (
                f"**{task}**\n"
                f"{progress_bar} {percentage:.2f}%\n"
                f"**Done:** {humanbytes(seen)} of {humanbytes(total_size)}\n"
                f"**Speed:** {humanbytes(speed)}/s\n"
                f"**ETA:** {eta}"
            )

            # Use safe_edit coroutine non-blocking
            asyncio.create_task(safe_edit(message, text))
            await asyncio.sleep(3)  # update every 3 seconds
    except asyncio.CancelledError:
        # Task cancelled — exit cleanly
        return
    except Exception:
        return

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations. Uses asyncio task to edit messages safely."""
    try:
        now = time.time()
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or (now - pyrogram_progress_callback.last_edit_time) > 3:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            text = f"**{task}...** {percentage:.2f}%"
            # Create a task to edit the message so we can await FloodWait there
            asyncio.create_task(safe_edit(message, text))
            pyrogram_progress_callback.last_edit_time = now
    except Exception:
        pass

# --- Connection Status Checkers ---
async def check_wasabi_connection():
    """Check if we can connect to Wasabi"""
    try:
        await asyncio.to_thread(s3_client.head_bucket, Bucket=WASABI_BUCKET)
        return "✅ Connected"
    except Exception as e:
        return f"❌ Error: {str(e)}"

async def check_telegram_connection():
    """Check if we're connected to Telegram"""
    try:
        # Simple check - if we can get our own bot info
        me = await app.get_me()
        return f"✅ Connected as @{me.username}"
    except Exception as e:
        return f"❌ Error: {str(e)}"

# --- Hybrid helpers (memory vs temp-file) ---
# Bytes threshold for in-memory streaming (200 MB)
IN_MEMORY_THRESHOLD = 200 * 1024 * 1024

def use_memory_stream(size):
    return size is not None and size <= IN_MEMORY_THRESHOLD

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Status", callback_data="status"),
        InlineKeyboardButton("❓ Help", callback_data="help")]
    ])
    
    await message.reply_text(
        TEMPLATES["start"],
        reply_markup=keyboard
    )

@app.on_message(filters.command("status"))
async def status_command(client, message: Message):
    """Handles the /status command."""
    # Get system stats
    cpu_usage = psutil.cpu_percent()
    memory_usage = humanbytes(psutil.virtual_memory().used)
    
    # Check connection status
    wasabi_status = await check_wasabi_connection()
    telegram_status = await check_telegram_connection()
    
    status_message = TEMPLATES["status"].format(
        uptime=bot_stats.get_uptime(),
        cpu_usage=cpu_usage,
        memory_usage=memory_usage,
        python_version=os.sys.version.split()[0],
        pyrogram_version=pyrogram_version,
        wasabi_status=wasabi_status,
        telegram_status=telegram_status,
        files_processed=bot_stats.files_processed,
        data_transferred=humanbytes(bot_stats.data_transferred)
    )
    
    await message.reply_text(status_message)

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    """Handles the /help command."""
    help_text = (
        "🤖 **Wasabi Storage Bot Help**\n\n"
        "**Commands:**\n"
        "• /start - Start the bot\n"
        "• /status - Check bot status and connections\n"
        "• /help - Show this help message\n"
        "• /download <filename> - Download a file from Wasabi\n\n"
        "**Usage:**\n"
        "• Just send any file to upload it to Wasabi\n"
        "• Use /download with the filename to retrieve files\n\n"
        "**Features:**\n"
        "• Turbo-speed transfers with parallel processing\n"
        "• Streamable links for media files\n"
        "• Progress tracking for all operations\n"
        "• Connection status monitoring"
    )
    await message.reply_text(help_text)

@app.on_callback_query()
async def callback_handler(client, callback_query):
    """Handle inline button callbacks"""
    data = callback_query.data
    
    if data == "status":
        # Get system stats
        cpu_usage = psutil.cpu_percent()
        memory_usage = humanbytes(psutil.virtual_memory().used)
        
        # Check connection status
        wasabi_status = await check_wasabi_connection()
        telegram_status = await check_telegram_connection()
        
        status_message = TEMPLATES["status"].format(
            uptime=bot_stats.get_uptime(),
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            python_version=os.sys.version.split()[0],
            pyrogram_version=pyrogram_version,
            wasabi_status=wasabi_status,
            telegram_status=telegram_status,
            files_processed=bot_stats.files_processed,
            data_transferred=humanbytes(bot_stats.data_transferred)
        )
        
        await callback_query.message.edit_text(status_message)
    
    elif data == "help":
        help_text = (
            "🤖 **Wasabi Storage Bot Help**\n\n"
            "**Commands:**\n"
            "• /start - Start the bot\n"
            "• /status - Check bot status and connections\n"
            "• /help - Show this help message\n"
            "• /download <filename> - Download a file from Wasabi\n\n"
            "**Usage:**\n"
            "• Just send any file to upload it to Wasabi\n"
            "• Use /download with the filename to retrieve files\n\n"
            "**Features:**\n"
            "• Turbo-speed transfers with parallel processing\n"
            "• Streamable links for media files\n"
            "• Progress tracking for all operations\n"
            "• Connection status monitoring"
        )
        await callback_query.message.edit_text(help_text)
    
    await callback_query.answer()

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers. Hybrid streaming (memory for small files)."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    # Telegram provides file_size on media objects; fall back to 0 if missing
    file_size = getattr(media, "file_size", 0) or 0
    status_message = await message.reply_text(TEMPLATES["processing"], quote=True)

    try:
        await safe_edit(status_message, TEMPLATES["downloading"])
        start_dl = time.time()
        # Download into memory if small, otherwise to temp file
        if use_memory_stream(file_size):
            # small file: download into memory
            file_bytes = await message.download(in_memory=True)
            if isinstance(file_bytes, bytes):
                file_obj = io.BytesIO(file_bytes)
            else:
                # safety: if library returns path, open it and read then remove
                with open(file_bytes, "rb") as f:
                    file_obj = io.BytesIO(f.read())
                os.remove(file_bytes)
            file_obj.seek(0)
            file_name = getattr(media, "file_name", f"{uuid.uuid4().hex}") or f"{uuid.uuid4().hex}"
            total_size = len(file_obj.getbuffer())
            status = {'running': True, 'seen': 0}

            def boto_callback(bytes_amount):
                status['seen'] += bytes_amount

            reporter_task = asyncio.create_task(
                progress_reporter(status_message, status, total_size, f"Uploading `{file_name}` (Turbo)", time.time())
            )

            # upload_fileobj is blocking; run in thread
            await asyncio.to_thread(
                s3_client.upload_fileobj,
                Fileobj=file_obj,
                Bucket=WASABI_BUCKET,
                Key=file_name,
                Callback=boto_callback,
                Config=transfer_config
            )

            status['running'] = False
            try:
                reporter_task.cancel()
                await reporter_task
            except asyncio.CancelledError:
                pass

        else:
            # large file: download to temp file
            tmp_fd, tmp_path = tempfile.mkstemp(prefix="tg_dl_")
            os.close(tmp_fd)
            # message.download returns path when not in_memory
            downloaded_path = await message.download(file_name=tmp_path)
            file_name = os.path.basename(downloaded_path)
            total_size = os.path.getsize(downloaded_path)
            status = {'running': True, 'seen': 0}

            def boto_callback(bytes_amount):
                status['seen'] += bytes_amount

            reporter_task = asyncio.create_task(
                progress_reporter(status_message, status, total_size, f"Uploading `{file_name}` (Turbo)", time.time())
            )

            await asyncio.to_thread(
                s3_client.upload_file,
                downloaded_path,
                WASABI_BUCKET,
                file_name,
                Callback=boto_callback,
                Config=transfer_config
            )

            status['running'] = False
            try:
                reporter_task.cancel()
                await reporter_task
            except asyncio.CancelledError:
                pass

            # remove temp file
            if os.path.exists(downloaded_path):
                os.remove(downloaded_path)

        # Update statistics
        bot_stats.add_file(total_size)

        # Build presigned URL (24 hours)
        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400)

        await safe_edit(status_message, TEMPLATES["upload_success"].format(
            file_name=file_name,
            file_size=humanbytes(total_size),
            presigned_url=presigned_url
        ))
    except Exception as e:
        err = "".join(traceback.format_exception_only(type(e), e)).strip()
        await safe_edit(status_message, TEMPLATES["error"].format(error=err))
    finally:
        # ensure any local temp cleaned (safety)
        # no-op here because downloaded_path handled above
        pass

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using multipart transfers. Hybrid streaming to Telegram."""
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_name_in_wasabi>`")
        return

    file_name = " ".join(message.command[1:])
    os.makedirs("./downloads", exist_ok=True)
    status_message = await message.reply_text(
        TEMPLATES["searching"].format(file_name=file_name), 
        quote=True
    )

    try:
        # Check object metadata
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get('ContentLength', 0))

        status = {'running': True, 'seen': 0}

        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading `{file_name}` (Turbo)", time.time())
        )

        if use_memory_stream(total_size):
            # stream into memory and send directly
            file_obj = io.BytesIO()
            await asyncio.to_thread(
                s3_client.download_fileobj,
                Bucket=WASABI_BUCKET,
                Key=file_name,
                Fileobj=file_obj,
                Callback=boto_callback,
                Config=transfer_config
            )
            status['running'] = False
            try:
                reporter_task.cancel()
                await reporter_task
            except asyncio.CancelledError:
                pass

            file_obj.seek(0)
            await safe_edit(status_message, "Uploading to Telegram...")
            # Pyrogram accepts file-like objects for send_document
            await client.send_document(
                chat_id=message.chat.id,
                document=file_obj,
                filename=file_name,
                progress=pyrogram_progress_callback,
                progress_args=(status_message, time.time(), "Uploading")
            )
            await status_message.delete()

        else:
            # large file: stream to temp file then send
            tmp_fd, tmp_path = tempfile.mkstemp(prefix="s3_dl_")
            os.close(tmp_fd)

            await asyncio.to_thread(
                s3_client.download_file,
                WASABI_BUCKET,
                file_name,
                tmp_path,
                Callback=boto_callback,
                Config=transfer_config
            )

            status['running'] = False
            try:
                reporter_task.cancel()
                await reporter_task
            except asyncio.CancelledError:
                pass

            await safe_edit(status_message, "Uploading to Telegram...")
            await client.send_document(
                chat_id=message.chat.id,
                document=tmp_path,
                progress=pyrogram_progress_callback,
                progress_args=(status_message, time.time(), "Uploading")
            )
            await status_message.delete()

            # cleanup
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        # Update statistics
        bot_stats.add_file(total_size)

    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            await safe_edit(status_message, TEMPLATES["file_not_found"].format(file_name=file_name))
        else:
            err = "".join(traceback.format_exception_only(type(e), e)).strip()
            await safe_edit(status_message, TEMPLATES["error"].format(error=err))
    except Exception as e:
        err = "".join(traceback.format_exception_only(type(e), e)).strip()
        await safe_edit(status_message, TEMPLATES["error"].format(error=err))
    finally:
        # ensure downloads dir is cleaned if necessary (we used temp files)
        pass

# --- Health check endpoint for Render ---
async def health_check(request):
    # Check if services are connected
    wasabi_status = await check_wasabi_connection()
    telegram_status = await check_telegram_connection()
    
    status = {
        "status": "ok",
        "uptime": bot_stats.get_uptime(),
        "wasabi": wasabi_status,
        "telegram": telegram_status,
        "files_processed": bot_stats.files_processed,
        "data_transferred": humanbytes(bot_stats.data_transferred)
    }
    
    return web.json_response(status)

async def start_web_server():
    """Start a simple web server for health checks on Render"""
    app_web = web.Application()
    app_web.router.add_get('/', health_check)
    app_web.router.add_get('/health', health_check)
    app_web.router.add_get('/status', health_check)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"Web server started on port {PORT}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with TURBO-SPEED settings...")
    
    # Create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Start web server
        loop.create_task(start_web_server())
        
        # Start the bot
        print("Starting Telegram bot...")
        app.run()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print("Fatal error while running the bot:", e)
        traceback.print_exc()
    finally:
        print("Bot has stopped.")