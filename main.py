#!/usr/bin/env python3
"""
Telegram Wasabi File Bot — Single-file, production-friendly example
- Pyrogram (bot) receives files and uploads to Wasabi (S3-compatible) using aioboto3 multipart
- Generates presigned download URLs (boto3)
- Optional FastAPI streaming proxy that supports Range requests (useful for MX Player / VLC)
- Progress reporting via message edits (works for large files)

Requirements
------------
Python 3.9+
pip install pyrogram tgcrypto aioboto3 aiofiles boto3 fastapi uvicorn python-multipart

Environment variables (required):
- API_ID
- API_HASH
- BOT_TOKEN
- WASABI_ACCESS_KEY
- WASABI_SECRET_KEY
- WASABI_BUCKET
- WASABI_REGION

Optional:
- PROXY_PUBLIC_HOST -> e.g. https://example.com (public URL pointing to the FastAPI service)
- PRESIGN_EXPIRY (seconds, default 3600)
- API_PORT (FastAPI port, default 8000)
- TMP_DIR (where to store temporary downloads)

Notes
-----
- This file purposefully keeps local buffering to a single temporary file before multipart upload to simplify reliability.
- For very large files and limited disk, consider implementing a streaming pipeline (Telegram -> chunk -> multipart upload) which is more complex.

"""

import os
import sys
import asyncio
import tempfile
import logging
import time
from pathlib import Path
from urllib.parse import quote
from typing import Optional

import aiofiles
import aioboto3
import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from pyrogram import Client, filters
from pyrogram.types import Message

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import StreamingResponse, RedirectResponse
import uvicorn

# ------- Logging -------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ------- Configuration -------
try:
    API_ID = int(os.environ["API_ID"])
    API_HASH = os.environ["API_HASH"]
    BOT_TOKEN = os.environ["BOT_TOKEN"]
    WASABI_ACCESS_KEY = os.environ["WASABI_ACCESS_KEY"]
    WASABI_SECRET_KEY = os.environ["WASABI_SECRET_KEY"]
    WASABI_BUCKET = os.environ["WASABI_BUCKET"]
    WASABI_REGION = os.environ["WASABI_REGION"]
except KeyError as e:
    missing = e.args[0]
    logger.error("Missing required environment variable: %s", missing)
    sys.exit(1)

PROXY_PUBLIC_HOST = os.environ.get("PROXY_PUBLIC_HOST")
PRESIGN_EXPIRY = int(os.environ.get("PRESIGN_EXPIRY", "3600"))
API_PORT = int(os.environ.get("API_PORT", "8000"))
TMP_DIR = Path(os.environ.get("TMP_DIR", tempfile.gettempdir())) / "tg_wasabi_bot"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Wasabi endpoint
WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"

# ------- S3 helpers -------

def make_boto3_client():
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name=WASABI_REGION,
        endpoint_url=WASABI_ENDPOINT,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def make_aioboto3_client():
    session = aioboto3.Session()
    return session.client(
        "s3",
        region_name=WASABI_REGION,
        endpoint_url=WASABI_ENDPOINT,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


async def upload_file_to_wasabi(local_path: Path, key: str, progress_callback=None) -> None:
    """Upload local_path to Wasabi using multipart upload and report progress via callback.
    progress_callback(current_bytes, total_bytes) -> None
    """
    total_size = local_path.stat().st_size
    logger.info("Uploading local=%s size=%d to wasabi key=%s", local_path, total_size, key)

    async with make_aioboto3_client() as client:
        mp = await client.create_multipart_upload(Bucket=WASABI_BUCKET, Key=key)
        upload_id = mp["UploadId"]
        parts = []
        part_number = 1
        chunk_size = 8 * 1024 * 1024  # 8 MiB
        uploaded = 0
        try:
            async with aiofiles.open(local_path, "rb") as f:
                while True:
                    data = await f.read(chunk_size)
                    if not data:
                        break
                    resp = await client.upload_part(
                        Bucket=WASABI_BUCKET,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data,
                    )
                    etag = resp.get("ETag")
                    parts.append({"ETag": etag, "PartNumber": part_number})
                    uploaded += len(data)
                    if progress_callback:
                        try:
                            progress_callback(uploaded, total_size)
                        except Exception:
                            logger.exception("Progress callback failed")
                    part_number += 1

            await client.complete_multipart_upload(
                Bucket=WASABI_BUCKET,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            logger.info("Multipart upload complete: %s", key)
        except Exception as e:
            logger.exception("Error during multipart upload, aborting: %s", e)
            try:
                await client.abort_multipart_upload(Bucket=WASABI_BUCKET, Key=key, UploadId=upload_id)
            except Exception:
                logger.exception("Failed to abort multipart upload")
            raise


def generate_presigned_url(key: str, expires_in: int = PRESIGN_EXPIRY) -> str:
    client = make_boto3_client()
    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": WASABI_BUCKET, "Key": key},
            ExpiresIn=expires_in,
        )
        return url
    except ClientError as e:
        logger.exception("Presign failed: %s", e)
        raise

# ------- FastAPI streaming proxy -------
app = FastAPI(title="Wasabi Streaming Proxy")

@app.get("/stream/{key:path}")
async def stream_key(key: str, request: Request, range: Optional[str] = Header(None)):
    # key is path param; it may be URL-encoded by the bot when producing the link
    # We will call boto3.get_object with Range header if present
    client = make_boto3_client()
    params = {"Bucket": WASABI_BUCKET, "Key": key}
    if range:
        params["Range"] = range
    try:
        resp = client.get_object(**params)
    except client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="Object not found")
    except Exception as e:
        logger.exception("Error fetching object from Wasabi: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching object")

    body = resp["Body"]

    def iter_body():
        try:
            while True:
                chunk = body.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                body.close()
            except Exception:
                pass

    headers = {}
    if "ContentLength" in resp:
        headers["content-length"] = str(resp["ContentLength"])
    if "ContentType" in resp:
        headers["content-type"] = resp["ContentType"]
    if "AcceptRanges" in resp:
        headers["accept-ranges"] = resp["AcceptRanges"]

    return StreamingResponse(iter_body(), headers=headers)

@app.get("/")
async def root():
    return RedirectResponse(url="/docs")

# ------- Pyrogram bot -------
bot = Client("wasabi_file_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir=str(TMP_DIR))


@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(c: Client, m: Message):
    await m.reply_text(
        "Hi! Send me a file (document/video/audio/photo). I will upload it to Wasabi and return a download & streaming link."
    )


@bot.on_message(filters.private & (filters.document | filters.video | filters.audio | filters.photo | filters.voice))
async def handle_file(c: Client, m: Message):
    media = m.document or m.video or m.audio or m.voice or m.photo
    file_name = getattr(media, "file_name", None) or f"file_{int(time.time())}"
    # sanitize filename a bit
    file_name = file_name.replace("/", "_")
    local_path = TMP_DIR / f"{int(time.time())}_{file_name}"

    status_msg = await m.reply_text(f"Preparing to download: {file_name}")

    # progress updater helpers
    last_edit = 0

    def download_progress(current, total):
        nonlocal last_edit
        now = time.time()
        if now - last_edit < 1 and current != total:
            return
        last_edit = now
        pct = (current / total * 100) if total else 0
        text = f"Downloading: {file_name}
{current}/{total} bytes ({pct:.1f}%)"
        # schedule edit
        asyncio.get_event_loop().create_task(
            safe_edit_message(chat_id=m.chat.id, message_id=status_msg.message_id, text=text)
        )

    async def safe_edit_message(chat_id: int, message_id: int, text: str):
        try:
            await c.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
        except Exception:
            # ignore edit failures (rate limits, message deleted, etc.)
            pass

    try:
        # Download file from Telegram to local_path
        await c.download_media(m, file_name=str(local_path), progress=download_progress)

        # Prepare key and start upload
        key = f"uploads/{int(time.time())}_{file_name}"
        upload_msg = await m.reply_text(f"Uploading to Wasabi: {file_name}")

        def upload_progress(current, total):
            nonlocal last_edit
            now = time.time()
            if now - last_edit < 1 and current != total:
                return
            last_edit = now
            pct = (current / total * 100) if total else 0
            text = f"Uploading: {file_name}
{current}/{total} bytes ({pct:.1f}%)"
            asyncio.get_event_loop().create_task(
                safe_edit_message(chat_id=m.chat.id, message_id=upload_msg.message_id, text=text)
            )

        await upload_file_to_wasabi(local_path, key, progress_callback=upload_progress)

        presigned = generate_presigned_url(key)

        result_text = ["Upload complete!
"]
        result_text.append(f"Direct presigned URL (expires in {PRESIGN_EXPIRY}s):
{presigned}
")

        if PROXY_PUBLIC_HOST:
            proxy_url = f"{PROXY_PUBLIC_HOST.rstrip('/')}/stream/{quote(key, safe='') }"
            result_text.append(f"Streaming proxy URL (for MX Player/VLC):
{proxy_url}
")
        else:
            result_text.append("No PROXY_PUBLIC_HOST set — to get a streaming URL suitable for MX Player/VLC, set PROXY_PUBLIC_HOST to your public host that forwards to this FastAPI instance.
")

        await m.reply_text("
".join(result_text))

    except Exception as e:
        logger.exception("Error handling file: %s", e)
        await m.reply_text(f"❌ Error: {e}")
    finally:
        try:
            if local_path.exists():
                local_path.unlink()
        except Exception:
            pass


async def run_services():
    # run FastAPI (uvicorn) in background thread and start bot in current event loop
    loop = asyncio.get_event_loop()

    def start_uvicorn():
        # uvicorn.run is blocking; run in thread
        uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="info")

    from threading import Thread
    t = Thread(target=start_uvicorn, daemon=True)
    t.start()

    await bot.start()
    logger.info("Bot started")
    # keep running until cancelled
    try:
        await asyncio.Event().wait()
    finally:
        await bot.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_services())
    except KeyboardInterrupt:
        logger.info("Shutting down by user request")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(1)
