#!/usr/bin/env python3
"""
Extreme-Speed Wasabi <-> Telegram Bot
- Aggressive multipart / concurrency for Wasabi
- Large worker pool for Pyrogram
- /tmp usage for fast I/O
- Health endpoint (aiohttp) for Render/containers (binds to PORT or 5000)
- Safer task cancellation & reduced edit-frequency to maximize bandwidth
"""
import os
import time
import math
import asyncio
import logging
import tempfile
import threading
from contextlib import suppress

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.s3.transfer import TransferConfig

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

from aiohttp import web

# Load env
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
PORT = int(os.getenv("PORT", 5000))

# Basic checks
REQUIRED = [API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]
if not all(REQUIRED):
    raise SystemExit("Missing required environment variables. Fill .env and restart.")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("wasabi_extreme")

# Pyrogram client with high worker count
app = Client(
    "wasabi_bot_extreme",
    api_id=int(API_ID),
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50  # high concurrency for many simultaneous tasks
)

# Botocore / boto3 config
botocore_conf = BotocoreConfig(
    retries={"max_attempts": 10, "mode": "adaptive"},
    max_pool_connections=128,
    tcp_keepalive=True
)

wasabi_endpoint_url = f"https://s3.{WASABI_REGION}.wasabisys.com"
s3_client = boto3.client(
    "s3",
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=botocore_conf
)

# TransferConfig: extreme-speed tuning (smaller chunk size, more concurrency)
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,   # 8MB
    multipart_chunksize=8 * 1024 * 1024,   # 8MB parts
    max_concurrency=64,                     # many parallel threads
    use_threads=True
)

TELEGRAM_BOT_FILE_LIMIT = 2 * 1024 * 1024 * 1024  # 2GB

# --- Utilities ---
def humanbytes(size: int) -> str:
    if not size:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.2f} {units[idx]}"

async def safe_edit(message: Message, text: str):
    """Edit text safely, handling FloodWait and other exceptions."""
    try:
        await message.edit_text(text)
    except FloodWait as e:
        log.warning("FloodWait in safe_edit: sleeping %s", e.x)
        await asyncio.sleep(e.x)
        with suppress(Exception):
            await message.edit_text(text)
    except Exception as e:
        # Silently ignore edit errors (too frequent edits cause errors; we prefer transfer speed)
        log.debug("Ignored edit error: %s", e)

# --- Progress reporter (async) ---
async def progress_reporter(message: Message, status: dict, total_size: int, task_name: str, start_time: float, interval: float = 5.0):
    """
    Reports progress every `interval` seconds (default 5s) to reduce edit overhead.
    status: dict with keys 'running' (bool) and 'seen' (int)
    """
    try:
        while status.get("running", False):
            seen = status.get("seen", 0)
            pct = (seen / total_size * 100) if total_size > 0 else 0
            pct = min(pct, 100)
            elapsed = max(time.time() - start_time, 0.0001)
            speed = seen / elapsed
            eta_seconds = int((total_size - seen) / speed) if speed > 0 else 0
            eta = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds)) if eta_seconds > 0 else "N/A"
            bar_units = int(pct // 10)
            progress_bar = "[" + "█" * bar_units + " " * (10 - bar_units) + "]"
            text = (
                f"**{task_name}**\n"
                f"{progress_bar} {pct:.2f}%\n"
                f"**Done:** {humanbytes(seen)} / {humanbytes(total_size)}\n"
                f"**Speed:** {humanbytes(speed)}/s\n"
                f"**ETA:** {eta}"
            )
            await safe_edit(message, text)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.debug("progress_reporter cancelled for %s", task_name)
    except Exception as e:
        log.exception("progress_reporter error: %s", e)

# Pyrogram-compatible progress callback (synchronous)
def pyrogram_progress_callback(current, total, message: Message, start_time: float, task: str):
    """
    Called by Pyrogram with (current, total, *progress_args)
    progress_args we pass: (status_message, start_time, task)
    """
    try:
        # Throttle edits: only allow edits every ~5 seconds
        now = time.time()
        last = getattr(pyrogram_progress_callback, "_last", 0)
        if now - last < 5.0:
            return
        pyrogram_progress_callback._last = now

        pct = (current * 100 / total) if total > 0 else 0
        text = f"**{task}** {pct:.2f}% — {humanbytes(current)} / {humanbytes(total)}"
        # schedule coroutine edit
        loop = asyncio.get_event_loop()
        loop.create_task(safe_edit(message, text))
    except Exception:
        pass

# --- Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    await message.reply_text(
        "Hello! I am an **Extreme-Speed** Wasabi storage bot.\n\n"
        "➡️ Send any file to upload to Wasabi.\n"
        "⬅️ Use `/download <file_name>` to fetch from Wasabi (then bot will send file)\n"
        "📂 Use `/list` to list recent files.\n\n"
        "Note: Telegram bots cannot send files >2GB. Wasabi can store bigger files."
    )

@app.on_message(filters.command("list"))
async def list_files_handler(client, message: Message):
    status_message = await message.reply_text("🔎 Fetching file list from Wasabi...", quote=True)
    try:
        resp = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET)
        if 'Contents' in resp and resp['Contents']:
            files = sorted(resp['Contents'], key=lambda x: x['LastModified'], reverse=True)
            txt = "**Files in your Wasabi Bucket (newest first):**\n\n"
            for file in files:
                line = f"📄 `{file['Key']}` — {humanbytes(file['Size'])}\n"
                if len(txt) + len(line) > 4000:
                    txt += "\n...and more (list truncated)."
                    break
                txt += line
            await status_message.edit_text(txt)
        else:
            await status_message.edit_text("✅ Your Wasabi bucket appears empty.")
    except ClientError as e:
        await status_message.edit_text(f"❌ S3 Client Error: {str(e)}")
    except Exception as e:
        await status_message.edit_text(f"❌ Unexpected error: {str(e)}")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported or empty media.")
        return

    status_message = await message.reply_text("Preparing upload...", quote=True)
    tmp_dir = "/tmp" if os.path.isdir("/tmp") else tempfile.gettempdir()
    file_path = None
    try:
        await status_message.edit_text("Downloading from Telegram to fast storage...")
        # Use /tmp for faster I/O
        file_path = await message.download(file_name=os.path.join(tmp_dir, media.file_name or f"tg_{int(time.time())}"),
                                           progress=pyrogram_progress_callback,
                                           progress_args=(status_message, time.time(), "Downloading from Telegram"))
        file_name = os.path.basename(file_path)
        file_size = media.file_size or (os.path.getsize(file_path) if os.path.exists(file_path) else 0)

        status = {"running": True, "seen": 0}
        start_time = time.time()

        def boto_cb(bytes_amount):
            # called in threads; safe because it's a primitive update
            status["seen"] += bytes_amount

        # start reporter (less frequent to reduce edit overhead)
        reporter = asyncio.create_task(progress_reporter(status_message, status, file_size, f"Uploading `{file_name}`", start_time, interval=5.0))

        # run blocking upload in threadpool
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_cb,
            Config=transfer_config
        )

        status["running"] = False
        # cancel reporter gracefully
        if reporter:
            reporter.cancel()
            with suppress(asyncio.CancelledError):
                await reporter

        # create presigned URL (24h)
        presigned = await asyncio.to_thread(
            s3_client.generate_presigned_url,
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=86400
        )

        await status_message.edit_text(
            f"✅ Upload successful!\n\n**File:** `{file_name}`\n**Size:** {humanbytes(file_size)}\n"
            f"**Streamable (24h):**\n`{presigned}`"
        )
    except Exception as e:
        log.exception("Upload error")
        await status_message.edit_text(f"❌ Upload failed: {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            with suppress(Exception):
                os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_name_in_wasabi>`")
        return

    file_name = " ".join(message.command[1:])
    tmp_dir = "/tmp" if os.path.isdir("/tmp") else tempfile.gettempdir()
    local_path = os.path.join(tmp_dir, file_name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    status_message = await message.reply_text(f"Searching Wasabi for `{file_name}`...", quote=True)
    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get("ContentLength", 0))

        # Prevent trying to send >2GB to Telegram
        if total_size > TELEGRAM_BOT_FILE_LIMIT:
            await status_message.edit_text(
                f"❌ File is {humanbytes(total_size)} which exceeds Telegram bot limit (2GB).\n"
                "You can download directly using the presigned URL instead."
            )
            # Provide presigned link
            presigned = await asyncio.to_thread(
                s3_client.generate_presigned_url,
                'get_object',
                Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
                ExpiresIn=86400
            )
            await message.reply_text(f"Direct download (24h):\n`{presigned}`")
            return

        status = {"running": True, "seen": 0}
        start_time = time.time()

        def boto_cb(bytes_amount):
            status["seen"] += bytes_amount

        reporter = asyncio.create_task(progress_reporter(status_message, status, total_size, f"Downloading `{file_name}`", start_time, interval=5.0))

        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            file_name,
            local_path,
            Callback=boto_cb,
            Config=transfer_config
        )

        status["running"] = False
        if reporter:
            reporter.cancel()
            with suppress(asyncio.CancelledError):
                await reporter

        await status_message.edit_text("Uploading file to Telegram...")
        # Upload to Telegram (progress disabled to reduce overhead)
        await client.send_document(
            chat_id=message.chat.id,
            document=local_path,
            force_document=True,
            # you can re-enable progress, but it will cause more edits -- we skip for speed
            # progress=pyrogram_progress_callback,
            # progress_args=(status_message, time.time(), "Uploading to Telegram")
        )
        with suppress(Exception):
            await status_message.delete()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey"):
            await status_message.edit_text(f"❌ File not found in Wasabi: `{file_name}`")
        else:
            await status_message.edit_text(f"❌ S3 Client Error: {str(e)}")
    except Exception as e:
        log.exception("Download error")
        await status_message.edit_text(f"❌ Unexpected error: {str(e)}")
    finally:
        if os.path.exists(local_path):
            with suppress(Exception):
                os.remove(local_path)

# --- lightweight HTTP server for healthchecks (binds to PORT) ---
async def health(request):
    return web.Response(text="OK - Wasabi Extreme Bot is running")

def run_health_server(host="0.0.0.0", port=PORT):
    app_web = web.Application()
    app_web.router.add_get("/", health)
    log.info("Starting health server on %s:%s", host, port)
    web.run_app(app_web, host=host, port=port)

# --- Main ---
if __name__ == "__main__":
    # Start health server in background thread so Pyrogram can run in main thread
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()
    log.info("Starting bot (extreme speed config)...")
    try:
        app.run()  # blocking call, runs until Ctrl+C
    except KeyboardInterrupt:
        log.info("Shutting down by KeyboardInterrupt")
    except Exception as e:
        log.exception("Top-level exception: %s", e)
    finally:
        log.info("Bot stopped.")
                            
