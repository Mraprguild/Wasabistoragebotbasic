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
    exit(1)

# --- Initialize Pyrogram Client with port 5000 ---
app = Client(
    "wasabi_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN, 
    workers=20,
    port=5000  # Added port 5000 configuration
)

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

# --- Hybrid helpers (memory vs temp-file) ---
# Bytes threshold for in-memory streaming (200 MB)
IN_MEMORY_THRESHOLD = 200 * 1024 * 1024

def use_memory_stream(size):
    return size is not None and size <= IN_MEMORY_THRESHOLD

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "Hello! I am a **Turbo-Speed** Wasabi storage bot.\n\n"
        "I use parallel processing to make transfers fast.\n\n"
        "➡️ **To upload:** Just send me any file.\n"
        "⬅️ **To download:** Use `/download <file_name>`.\n\n"
        "Generated links are direct streamable links compatible with players like VLC & MX Player."
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers. Hybrid streaming (memory for small files)."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    # Telegram provides file_size on media objects; fall back to 0 if missing
    file_size = getattr(media, "file_size", 0) or 0
    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        await status_message.edit_text("Downloading from Telegram...")
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

        # Build presigned URL (24 hours)
        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400)

        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** {humanbytes(total_size)}\n"
            f"**Streamable Link (24h expiry):**\n`{presigned_url}`"
        )
    except Exception as e:
        err = "".join(traceback.format_exception_only(type(e), e)).strip()
        await status_message.edit_text(f"❌ An error occurred: `{err}`")
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
    status_message = await message.reply_text(f"Searching for `{file_name}`...", quote=True)

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
            await status_message.edit_text("Uploading to Telegram...")
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

            await status_message.edit_text("Uploading to Telegram...")
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

    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            await status_message.edit_text(f"❌ **Error:** File not found in Wasabi: `{file_name}`")
        else:
            err = "".join(traceback.format_exception_only(type(e), e)).strip()
            await status_message.edit_text(f"❌ **S3 Client Error:** `{err}`")
    except Exception as e:
        err = "".join(traceback.format_exception_only(type(e), e)).strip()
        await status_message.edit_text(f"❌ **An unexpected error occurred:** `{err}`")
    finally:
        # ensure downloads dir is cleaned if necessary (we used temp files)
        pass

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with TURBO-SPEED settings...")
    try:
        app.run()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print("Fatal error while running the bot:", e)
    finally:
        print("Bot has stopped.")
