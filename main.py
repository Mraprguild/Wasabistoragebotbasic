import os
import time
import math
import boto3
import asyncio
import mimetypes
from urllib.parse import quote
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait
from aiohttp import web

# --- Load Environment Variables ---
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
WASABI_ENDPOINT_URL = f'https://s3.{WASABI_REGION}.wasabisys.com'

# --- Initialize Pyrogram Bot ---
app = Client(
    "wasabi_upload_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- State Management for Cancellations ---
active_transfers = {}

# --- Initialize Boto3 S3 Client for Wasabi ---
try:
    s3 = boto3.client(
        's3',
        endpoint_url=WASABI_ENDPOINT_URL,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=WASABI_REGION
    )
    print("✅ Successfully connected to Wasabi.")
except (NoCredentialsError, PartialCredentialsError) as e:
    print(f"❌ Error: Wasabi credentials not found or incomplete. Please check your .env file. Details: {e}")
    s3 = None
except Exception as e:
    print(f"❌ An unexpected error occurred during Wasabi connection: {e}")
    s3 = None


# --- Helper Functions ---
def humanbytes(size):
    if not size: return "0B"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

async def progress_callback(current, total, message, start_time, action, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2: return
    elapsed_time = now - start_time
    if elapsed_time == 0: return

    speed = current / elapsed_time
    percentage = current * 100 / total
    filled_len = int(percentage / 5)
    progress_bar = "🚀" * filled_len + "─" * (20 - filled_len) if percentage < 100 else "✅" * 20
    eta = time.strftime('%Hh %Mm %Ss', time.gmtime((total - current) / speed)) if speed > 0 else 'N/A'

    try:
        file_media = message.reply_to_message.document or message.reply_to_message.video or message.reply_to_message.audio
        file_name_str = getattr(file_media, 'file_name', 'file')
        
        progress_text = (
            f"**{action}**\n"
            f"**File:** `{file_name_str}`\n"
            f"├ `{progress_bar}`\n"
            f"├ **Progress:** {percentage:.1f}%\n"
            f"├ **Done:** {humanbytes(current)} of {humanbytes(total)}\n"
            f"├ **Speed:** {humanbytes(speed)}/s\n"
            f"└ **ETA:** {eta}"
        )
        transfer_id = f"{message.chat.id}-{message.reply_to_message.id}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{transfer_id}")]]) if active_transfers.get(transfer_id) else None
        await message.edit_text(progress_text, reply_markup=keyboard)
        last_update_time[0] = now
    except FloodWait as e:
        print(f"FloodWait: sleeping for {e.value} seconds.")
        await asyncio.sleep(e.value)
    except Exception: pass


# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    await message.reply_text("👋 Hello! I create direct streaming links for VLC, MX Player, and web browsers. Just send me a file.")

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    if not s3:
        await message.reply_text("⚠️ **Connection Error:** Could not connect to Wasabi. Please check config.")
        return
    media = message.document or message.video or message.audio
    if not media: return
    file_name, file_size = media.file_name, media.file_size
    if file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File is larger than 4 GB.")
        return
    transfer_id = f"{message.chat.id}-{message.id}"
    active_transfers[transfer_id] = {"cancelled": False}
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{transfer_id}")]])
    status_message = await message.reply_text(f"🚀 Preparing to turbo-stream `{file_name}`...", reply_markup=keyboard)
    start_time, last_update_time = time.time(), [time.time()]
    content_type, _ = mimetypes.guess_type(file_name)
    content_type = content_type or "application/octet-stream"

    def create_upload(): return s3.create_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, ContentType=content_type, ContentDisposition='inline')
    def upload_part(uid, pn, ch): return s3.upload_part(Bucket=WASABI_BUCKET, Key=file_name, UploadId=uid, PartNumber=pn, Body=ch)
    def complete_upload(uid, p): return s3.complete_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, UploadId=uid, MultipartUpload={'Parts': p})
    def abort_upload(uid): return s3.abort_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, UploadId=uid)
    
    upload_id = None
    try:
        multi_part_upload = await asyncio.to_thread(create_upload)
        upload_id = multi_part_upload['UploadId']
        parts, part_number, seen_so_far = [], 1, 0
        MIN_PART_SIZE = 5 * 1024 * 1024 
        buffer = bytearray()
        async for chunk in app.stream_media(message):
            if active_transfers.get(transfer_id, {}).get("cancelled"):
                raise UserWarning("Transfer cancelled by user.")
            buffer.extend(chunk)
            seen_so_far += len(chunk)
            while len(buffer) >= MIN_PART_SIZE:
                part_chunk, buffer = buffer[:MIN_PART_SIZE], buffer[MIN_PART_SIZE:]
                part_response = await asyncio.to_thread(upload_part, upload_id, part_number, part_chunk)
                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                part_number += 1
            await progress_callback(seen_so_far, file_size, status_message, start_time, "⚡ Streaming to Cloud", last_update_time)
        if buffer:
            part_response = await asyncio.to_thread(upload_part, upload_id, part_number, bytes(buffer))
            parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
        await asyncio.to_thread(complete_upload, upload_id, parts)
        
        presigned_url = s3.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=604800)
        encoded_url = quote(presigned_url, safe="")
        vlc_url = f"vlc://{encoded_url}"
        mx_player_url = f"intent:{presigned_url}#Intent;action=android.intent.action.VIEW;package=com.mxtech.videoplayer.ad;end"
        duration = time.strftime('%Hh %Mm %Ss', time.gmtime(time.time() - start_time))
        final_message = (f"✅ **Upload Complete!**\n\n📄 **File:** `{file_name}`\n📦 **Size:** `{humanbytes(file_size)}`\n"
                         f"⏱️ **Duration:** `{duration}`\n\n**Copy Link:**\n`{presigned_url}`\n")
        player_buttons = [[InlineKeyboardButton("▶️ Play in VLC", url=vlc_url), InlineKeyboardButton("▶️ Play in MX Player", url=mx_player_url)],
                          [InlineKeyboardButton("🌐 Open in Browser", url=presigned_url)]]
        await status_message.edit_text(final_message, reply_markup=InlineKeyboardMarkup(player_buttons))
    except UserWarning:
        await status_message.edit_text(f"❌ **Transfer Cancelled.**\n`{file_name}`")
        if upload_id: await asyncio.to_thread(abort_upload, upload_id)
    except Exception as e:
        await status_message.edit_text(f"❌ **Stream Upload Failed:** {e}")
        if upload_id: await asyncio.to_thread(abort_upload, upload_id)
    finally:
        if transfer_id in active_transfers: del active_transfers[transfer_id]

@app.on_callback_query(filters.regex("^cancel_"))
async def cancel_handler(_, query):
    transfer_id = query.data.split("_")[1]
    if transfer_id in active_transfers:
        active_transfers[transfer_id]["cancelled"] = True
        await query.answer("Cancelling transfer...", show_alert=False)
        try: await query.message.edit_text("⏳ Cancelling the transfer, please wait...")
        except Exception: pass
    else:
        await query.answer("This transfer is already complete or has been cancelled.", show_alert=True)
        try: await query.message.edit_reply_markup(None)
        except Exception: pass


# --- Web Server for Health Checks (Required for some deployment platforms) ---
async def run_web_server():
    """Initializes and runs the aiohttp web server in the background."""
    async def health_check(request):
        return web.Response(text="OK", status=200)

    webapp = web.Application()
    webapp.router.add_get("/", health_check)
    runner = web.AppRunner(webapp)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080)) # Use 8080 as a common default
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    try:
        await site.start()
        print(f"✅ Web server is listening on port {port} for health checks.")
        await asyncio.Future() # Keep the server running indefinitely until cancelled
    finally:
        await runner.cleanup()
        print("🛑 Web server stopped.")


# --- MAIN EXECUTION BLOCK ---
async def main():
    """Starts the bot and the background web server, and handles graceful shutdown."""
    web_server_task = asyncio.create_task(run_web_server())

    try:
        print("--- Starting Telegram Bot ---")
        await app.start()
        print("✅ Telegram Bot is now online.")
        await idle()
    finally:
        print("\n--- Shutting down services... ---")
        await app.stop()
        print("🛑 Telegram Bot stopped.")
        
        web_server_task.cancel()
        try:
            await web_server_task
        except asyncio.CancelledError:
            print("Web server task cancelled successfully.")


if __name__ == "__main__":
    print("Bot starting up...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nShutdown signal received.")
