import os
import time
import math
import boto3
import asyncio
import mimetypes
from datetime import datetime
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

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
    """Converts bytes to a human-readable format."""
    if not size:
        return "0B"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


async def progress_callback(current, total, message, start_time, action, last_update_time):
    """Updates the progress message in Telegram with an advanced UI."""
    now = time.time()
    if now - last_update_time[0] < 2:  # Throttle updates to every 2 seconds
        return

    elapsed_time = now - start_time
    if elapsed_time == 0:
        return

    speed = current / elapsed_time
    percentage = current * 100 / total
    
    # Advanced Emoji Progress Bar
    filled_len = int(percentage / 5)
    if percentage == 100:
        progress_bar = "✅" * 20
    else:
        progress_bar = "🚀" * filled_len + "─" * (20 - filled_len)

    time_left_seconds = (total - current) / speed if speed > 0 else 0
    eta = time.strftime('%Hh %Mm %Ss', time.gmtime(time_left_seconds))

    progress_text = (
        f"**{action}**\n"
        f"**File:** `{message.reply_to_message.document or message.reply_to_message.video or message.reply_to_message.audio.file_name}`\n"
        f"├ `{progress_bar}`\n"
        f"├ **Progress:** {percentage:.1f}%\n"
        f"├ **Done:** {humanbytes(current)} of {humanbytes(total)}\n"
        f"├ **Speed:** {humanbytes(speed)}/s\n"
        f"└ **ETA:** {eta}"
    )
    
    try:
        # Get the cancel button associated with this transfer
        transfer_id = f"{message.chat.id}-{message.reply_to_message.id}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{transfer_id}")]]) if active_transfers.get(transfer_id) else None
        await message.edit_text(progress_text, reply_markup=keyboard)
        last_update_time[0] = now
    except FloodWait as e:
        print(f"FloodWait: sleeping for {e.value} seconds.")
        await asyncio.sleep(e.value)
    except Exception:
        pass


# --- Web Server for Render Health Check ---
async def health_check(request):
    return web.Response(text="OK", status=200)


# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    await message.reply_text("👋 Hello! Send me any file, and I will upload it to Wasabi and give you a streamable link compatible with VLC and MX Player.")

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    if not s3:
        await message.reply_text("⚠️ **Connection Error:** Could not connect to Wasabi. Please check config.")
        return

    media = message.document or message.video or message.audio
    if not media:
        return

    file_name = media.file_name
    file_size = media.file_size
    
    if file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File is larger than 4 GB.")
        return
    
    transfer_id = f"{message.chat.id}-{message.id}"
    active_transfers[transfer_id] = {"cancelled": False}
    
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{transfer_id}")]])
    status_message = await message.reply_text(f"🚀 Preparing to turbo-stream `{file_name}`...", reply_markup=keyboard)
    
    start_time = time.time()
    last_update_time = [start_time]

    content_type, _ = mimetypes.guess_type(file_name)
    content_type = content_type or "application/octet-stream"

    def create_upload():
        return s3.create_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, ContentType=content_type, ContentDisposition='inline')

    def upload_part(upload_id, part_number, chunk):
        return s3.upload_part(Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id, PartNumber=part_number, Body=chunk)

    def complete_upload(upload_id, parts):
        return s3.complete_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id, MultipartUpload={'Parts': parts})
    
    def abort_upload(upload_id):
        return s3.abort_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id)
    
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
                part_chunk = buffer[:MIN_PART_SIZE]
                buffer = buffer[MIN_PART_SIZE:]
                
                part_response = await asyncio.to_thread(upload_part, upload_id, part_number, part_chunk)
                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                part_number += 1
            
            await progress_callback(seen_so_far, file_size, status_message, start_time, "⚡ Streaming to Cloud", last_update_time)

        if buffer:
            part_response = await asyncio.to_thread(upload_part, upload_id, part_number, bytes(buffer))
            parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})

        await asyncio.to_thread(complete_upload, upload_id, parts)
        
        # Final success message
        end_time = time.time()
        duration = time.strftime('%Hh %Mm %Ss', time.gmtime(end_time - start_time))
        presigned_url = s3.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=604800)
        
        final_message = (
            f"✅ **Upload Complete!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** `{humanbytes(file_size)}`\n"
            f"⏱️ **Duration:** `{duration}`\n"
            f"🔗 **Stream & Download Link:**\n`{presigned_url}`\n\n"
            f"This link works in VLC, MX Player, and web browsers. It will expire in 7 days."
        )
        await status_message.edit_text(final_message)

    except UserWarning as e:
        await status_message.edit_text(f"❌ **Transfer Cancelled.**\n`{file_name}`")
        if upload_id:
            await asyncio.to_thread(abort_upload, upload_id)
            
    except Exception as e:
        await status_message.edit_text(f"❌ **Stream Upload Failed:** {e}")
        if upload_id:
            await asyncio.to_thread(abort_upload, upload_id)
            
    finally:
        if transfer_id in active_transfers:
            del active_transfers[transfer_id]

@app.on_callback_query(filters.regex("^cancel_"))
async def cancel_handler(_, query):
    transfer_id = query.data.split("_")[1]
    if transfer_id in active_transfers:
        active_transfers[transfer_id]["cancelled"] = True
        await query.answer("Cancelling transfer...", show_alert=False)
        try:
            await query.message.edit_text("⏳ Cancelling the transfer, please wait...")
        except Exception:
            pass
    else:
        await query.answer("This transfer is already complete or has been cancelled.", show_alert=True)
        try:
            await query.message.edit_reply_markup(None)
        except Exception:
            pass

async def main():
    """Starts the bot and the web server concurrently."""
    webapp = web.Application()
    webapp.router.add_get("/", health_check)
    runner = web.AppRunner(webapp)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 5000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    await app.start()
    await site.start()
    print(f"🚀 Bot and web server started on port {port}...")
    
    await idle()
    
    await app.stop()
    await runner.cleanup()
    print("👋 Bot and web server have stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Interrupted by user. Shutting down...")
