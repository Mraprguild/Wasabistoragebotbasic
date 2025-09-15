#!/usr/bin/env python3
"""
Telegram File Bot + Wasabi Storage
- Upload up to 5GB files
- Super-fast upload/download (parallel chunks)
- MX Player / VLC streaming links
- Render.com compatible (port 5000 web server)
"""

import os
import time
import math
import asyncio
import mimetypes
import logging
from urllib.parse import quote

import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters, idle
from pyrogram.types import Message
from aiohttp import web
from dotenv import load_dotenv

# --- Load Environment Variables ---
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

# --- Wasabi Client ---
s3_client = boto3.client(
    "s3",
    endpoint_url=WASABI_ENDPOINT_URL,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=boto3.session.Config(
        retries={"max_attempts": 10, "mode": "adaptive"},
        tcp_keepalive=True,
        max_pool_connections=64,
    ),
)

# --- TransferConfig for Speed ---
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,  # Start multipart uploads at 8MB
    multipart_chunksize=8 * 1024 * 1024,  # Each chunk = 8MB
    max_concurrency=64,                   # High concurrency for speed
    use_threads=True,
)

# --- Pyrogram Bot ---
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

# --- Progress Bar ---
async def progress_callback(current, total, message: Message, start, action):
    now = time.time()
    diff = now - start
    if diff == 0:
        return
    speed = current / diff
    percent = current * 100 / total
    eta = (total - current) / speed
    try:
        await message.edit_text(
            f"{action}\n"
            f"{percent:.2f}% of {math.ceil(total/1024/1024)} MB\n"
            f"⚡ Speed: {math.ceil(speed/1024/1024)} MB/s\n"
            f"⏳ ETA: {math.ceil(eta)}s"
        )
    except:
        pass

# --- Upload Handler ---
@app.on_message(filters.document | filters.video | filters.audio)
async def upload_to_wasabi(client, message: Message):
    status = await message.reply("⬇️ Downloading from Telegram...")

    file_path = await message.download(
        file_name=f"/tmp/{message.document.file_name}",
        progress=progress_callback,
        progress_args=(status, time.time(), "⬇️ Downloading from Telegram"),
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
        f"✅ Uploaded to Wasabi!\n\n"
        f"🔗 [Direct Link]({url})\n"
        f"🎬 [VLC Player Link]({vlc})\n"
        f"📱 [MX Player Link]({mx})"
    )

    os.remove(file_path)

# --- Download Handler ---
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

# --- Web Server for Render ---
async def handle(request):
    return web.Response(text="✅ Bot is running on Render!")

async def run_web():
    app_web = web.Application()
    app_web.router.add_get("/", handle)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 5000)))
    await site.start()
    while True:
        await asyncio.sleep(3600)

# --- Main ---
async def main():
    await app.start()
    asyncio.create_task(run_web())  # Run web server in background
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
           
