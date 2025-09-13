#!/usr/bin/env python3
"""
Telegram File Bot with Wasabi Cloud Storage
- Upload & download up to 5GB files
- Wasabi S3-compatible backend via boto3/aioboto3
- Real-time progress updates
- Streaming support (FastAPI proxy, MX Player / VLC compatible)
"""

import os
import asyncio
import tempfile
import logging
import time
from pathlib import Path
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import Message
import boto3
from fastapi import FastAPI, Response, Request
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# Environment variables
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
PRESIGN_EXPIRY = int(os.getenv("PRESIGN_EXPIRY", 86400))  # default 24h
PROXY_PUBLIC_HOST = os.getenv("PROXY_PUBLIC_HOST")
API_PORT = int(os.getenv("API_PORT", 5000))

if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET]):
    raise SystemExit("❌ Missing one or more required environment variables.")

# Pyrogram bot client
bot = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    in_memory=True,
)

# FastAPI app for streaming
app = FastAPI(title="Wasabi Streaming Proxy")

# Boto3 client for presigned URL generation
s3_client = boto3.client(
    "s3",
    endpoint_url=f"https://s3.{WASABI_REGION}.wasabisys.com",
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=WASABI_REGION,
)

# ---------- Helpers ---------- #
async def safe_edit_message(chat_id: int, message_id: int, text: str):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except Exception:
        pass

def make_progress_updater(title: str, chat_id: int, message_id: int):
    last_edit = {"time": 0}

    def progress(current, total):
        now = time.time()
        if now - last_edit["time"] < 1 and current != total:
            return
        last_edit["time"] = now
        pct = (current / total * 100) if total else 0
        text = f"{title}\n{current}/{total} bytes ({pct:.1f}%)"
        asyncio.get_event_loop().create_task(
            safe_edit_message(chat_id, message_id, text)
        )

    return progress

async def upload_file_to_wasabi(local_path: Path, key: str, progress_callback=None):
    session = boto3.Session()
    async with session.client(
        "s3",
        endpoint_url=f"https://s3.{WASABI_REGION}.wasabisys.com",
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=WASABI_REGION,
    ) as client:
        total = os.path.getsize(local_path)
        part_size = 8 * 1024 * 1024
        mpu = await client.create_multipart_upload(Bucket=WASABI_BUCKET, Key=key)
        upload_id = mpu["UploadId"]
        parts = []
        try:
            async with await asyncio.to_thread(open, local_path, "rb") as f:
                part_number = 1
                while True:
                    chunk = await asyncio.to_thread(f.read, part_size)
                    if not chunk:
                        break
                    resp = await client.upload_part(
                        Bucket=WASABI_BUCKET,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                    if progress_callback:
                        sent = part_number * part_size
                        progress_callback(min(sent, total), total)
                    part_number += 1
            await client.complete_multipart_upload(
                Bucket=WASABI_BUCKET,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception as e:
            await client.abort_multipart_upload(
                Bucket=WASABI_BUCKET, Key=key, UploadId=upload_id
            )
            raise e

# ---------- Bot Handlers ---------- #
@bot.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_file(c: Client, m: Message):
    file_name = m.document.file_name if m.document else (
        m.video.file_name if m.video else (
            m.audio.file_name if m.audio else f"photo_{m.date}.jpg"
        )
    )
    chat_id = m.chat.id

    tmpdir = Path(tempfile.gettempdir())
    local_path = tmpdir / f"{int(time.time())}_{file_name}"
    key = f"uploads/{local_path.name}"

    status_msg = await m.reply_text(f"Downloading: {file_name}")
    download_progress = make_progress_updater("Downloading: " + file_name, chat_id, status_msg.message_id)
    await c.download_media(m, file_name=str(local_path), progress=download_progress)

    upload_msg = await m.reply_text(f"Uploading: {file_name}")
    upload_progress = make_progress_updater("Uploading: " + file_name, chat_id, upload_msg.message_id)
    await upload_file_to_wasabi(local_path, key, progress_callback=upload_progress)

    os.remove(local_path)

    presigned = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": WASABI_BUCKET, "Key": key},
        ExpiresIn=PRESIGN_EXPIRY,
    )

    if PROXY_PUBLIC_HOST:
        stream_url = f"{PROXY_PUBLIC_HOST}/stream/{key}"
        await m.reply_text(f"✅ Uploaded!\nDirect: {presigned}\nStream: {stream_url}")
    else:
        await m.reply_text(f"✅ Uploaded!\nDirect: {presigned}")

# ---------- FastAPI Streaming ---------- #
@app.get("/stream/{key:path}")
async def stream_file(key: str, request: Request):
    loop = asyncio.get_event_loop()
    obj = await loop.run_in_executor(
        None,
        lambda: s3_client.get_object(Bucket=WASABI_BUCKET, Key=key)
    )
    body = obj["Body"]
    async def iterator():
        while True:
            chunk = await loop.run_in_executor(None, body.read, 1024*1024)
            if not chunk:
                break
            yield chunk
    return Response(content=iterator(), media_type="application/octet-stream")

# ---------- Entrypoint ---------- #
async def main():
    loop = asyncio.get_event_loop()
    loop.create_task(
        uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="info")
    )
    await bot.start()
    logger.info("🤖 Bot started.")
    await idle()

from pyrogram import idle

if __name__ == "__main__":
    asyncio.run(main())
    
