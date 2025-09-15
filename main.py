#!/usr/bin/env python3
"""
Telegram File Bot + Wasabi Storage
- Upload up to 5GB files
- Extreme speed (parallel chunk transfers)
- Direct streamable links (VLC/MX)
- Render compatible (port 5000 web server)
- Bot runs in POLLING mode
"""

import os
import time
import math
import asyncio
import logging
import threading
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters
from pyrogram.types import Message
from aiohttp import web
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION", "ap-northeast-1")
WASABI_ENDPOINT_URL = f"https://s3.{WASABI_REGION}.wasabisys.com"

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Boto3 client ---
s3_client = boto3.client(
    "s3",
    endpoint_url=WASABI_ENDPOINT_URL,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=boto3.session.Config(
        retries={"max_attempts": 10, "mode": "adaptive"},
        max_pool_connections=64,
    ),
)

# --- TransferConfig for high speed ---
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=64,
    use_threads=True,
)

# --- Pyrogram bot (polling) ---
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

# --- Progress Helper ---
async def progress_callback(current, total, message: Message, start, action):
    now = time.time()
    diff = now - start
    if diff == 0:
        return
    speed = current / diff
    percent = current * 100 / total
    eta = (total - current) / speed if speed > 0 else 0
    try:
        await message.edit_text(
            f"{action}\n"
            f"{percent:.2f}% of {math.ceil(total/1024/1024)} MB\n"
            f"⚡ {math.ceil(speed/1024/1024)} MB/s\n"
            f"⏳ {math.ceil(eta)}s left"
        )
    except:
        pass

# --- Upload handler ---
@app.on_message(filters.document | filters.video | filters.audio)
async def upload_to_wasabi(client, message: Message):
    status = await message.reply("⬇️ Downloading from Telegram...")
    file_path = await message.download(
        file_name=f"/tmp/{message.document.file_name}",
        progress=progress_callback,
        progress_args=(status, time.time(), "⬇️ Downloading"),
    )
    file_name = os.path.basename(file_path)

    await status.edit("⬆️ Uploading to Wasabi...")
    await asyncio.to_thread(
        s3_client.upload_file,
        file_path,
        WASABI_BUCKET,
        file_name,
        Config=transfer_config,
    )

    url = f"{WASABI_ENDPOINT_URL}/{WASABI_BUCKET}/{quote(file_name)}"
    vlc = f"vlc://{url}"
    mx = f"intent:{url}#Intent;package=com.mxtech.videoplayer.ad;end"

    await status.edit(
        f"✅ Uploaded!\n\n"
        f"🔗 [Direct Link]({url})\n"
        f"🎬 [VLC Link]({vlc})\n"
        f"📱 [MX Player Link]({mx})"
    )

    os.remove(file_path)

# --- Download handler ---
@app.on_message(filters.command("get"))
async def download_from_wasabi(client, message: Message):
    if len(message.command) < 2:
        return await message.reply("Usage: `/get filename`")

    file_name = message.command[1]
    status = await message.reply("⬇️ Downloading from Wasabi...")

    tmp_path = f"/tmp/{file_name}"
    await asyncio.to_thread(
        s3_client.download_file,
        WASABI_BUCKET,
        file_name,
        tmp_path,
        Config=transfer_config,
    )

    await message.reply_document(
        tmp_path,
        caption=f"📥 {file_name}",
        progress=progress_callback,
        progress_args=(status, time.time(), "⬆️ Uploading to Telegram"),
    )

    os.remove(tmp_path)
    await status.delete()

# --- Web server for Render ---
async def handle(request):
    return web.Response(text="✅ Bot is running in polling mode with webserver!")

def start_web():
    async def runner():
        app_web = web.Application()
        app_web.router.add_get("/", handle)
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 5000)))
        await site.start()
        while True:
            await asyncio.sleep(3600)
    asyncio.run(runner())

# --- Entry ---
if __name__ == "__main__":
    # Start webserver in background
    threading.Thread(target=start_web, daemon=True).start()
    # Run bot (polling mode)
    app.run()
    
