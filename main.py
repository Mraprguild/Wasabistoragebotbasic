import os
import time
import math
import boto3
import asyncio
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message
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
    """Updates the progress message in Telegram, throttling to avoid API limits."""
    now = time.time()
    # Update only once every 2 seconds to avoid hitting API rate limits
    if now - last_update_time[0] < 2:
        return

    elapsed_time = now - start_time
    if elapsed_time == 0:
        return

    speed = current / elapsed_time
    percentage = current * 100 / total
    progress_bar = "■" * int(percentage / 5) + "□" * (20 - int(percentage / 5))
    
    time_left_seconds = (total - current) / speed if speed > 0 else 0
    time_left = time.strftime('%Hh %Mm %Ss', time.gmtime(time_left_seconds))

    progress_text = (
        f"**{action}**\n"
        f"├ `{progress_bar}`\n"
        f"├ **Progress:** {percentage:.1f}%\n"
        f"├ **Done:** {humanbytes(current)} of {humanbytes(total)}\n"
        f"├ **Speed:** {humanbytes(speed)}/s\n"
        f"└ **Time Left:** {time_left}"
    )
    
    try:
        await message.edit_text(progress_text)
        last_update_time[0] = now # Update the time of the last successful edit
    except FloodWait as e:
        print(f"FloodWait: sleeping for {e.value} seconds.")
        await asyncio.sleep(e.value)
    except Exception:
        pass


# --- Web Server for Render Health Check ---
async def health_check(request):
    """Responds with a 200 OK for Render's health checks."""
    return web.Response(text="OK", status=200)


# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(_, message: Message):
    """Handler for the /start command."""
    await message.reply_text("👋 Hello! Send me any file, and I will upload it to Wasabi and give you a streamable link.")

@app.on_message(filters.document | filters.video | filters.audio)
async def file_handler(_, message: Message):
    """Handles file streaming from Telegram to Wasabi."""
    if not s3:
        await message.reply_text("⚠️ **Connection Error:** Could not connect to Wasabi storage. Please check the bot's configuration and logs.")
        return

    media = message.document or message.video or message.audio
    if not media:
        await message.reply_text("This message doesn't contain a file I can handle.")
        return

    file_name = media.file_name
    file_size = media.file_size
    
    if file_size > 4 * 1024 * 1024 * 1024: # Pyrogram can handle up to 4GB
        await message.reply_text("❌ **Error:** File is larger than 4 GB.")
        return

    status_message = await message.reply_text(f"🚀 Preparing to turbo-stream `{file_name}`...")
    
    start_time = time.time()
    last_update_time = [start_time]

    # --- Boto3 Multipart Upload Functions ---
    def create_upload():
        return s3.create_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name)

    def upload_part(upload_id, part_number, chunk):
        return s3.upload_part(
            Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id,
            PartNumber=part_number, Body=chunk
        )

    def complete_upload(upload_id, parts):
        return s3.complete_multipart_upload(
            Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    
    def abort_upload(upload_id):
        return s3.abort_multipart_upload(
            Bucket=WASABI_BUCKET, Key=file_name, UploadId=upload_id
        )

    try:
        # 1. Initiate Multipart Upload in a non-blocking thread
        multi_part_upload = await asyncio.to_thread(create_upload)
        upload_id = multi_part_upload['UploadId']
        
        parts = []
        part_number = 1
        seen_so_far = 0
        
        # S3 parts must be at least 5MB, except for the last one.
        MIN_PART_SIZE = 5 * 1024 * 1024 
        buffer = bytearray()

        # 2. Stream from Telegram and upload parts to Wasabi
        async for chunk in app.stream_media(message):
            buffer.extend(chunk)
            seen_so_far += len(chunk)
            
            # If buffer is large enough, upload a part
            while len(buffer) >= MIN_PART_SIZE:
                part_chunk = buffer[:MIN_PART_SIZE]
                buffer = buffer[MIN_PART_SIZE:]
                
                part_response = await asyncio.to_thread(upload_part, upload_id, part_number, part_chunk)
                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                part_number += 1

            # Update progress
            await progress_callback(seen_so_far, file_size, status_message, start_time, "⚡ Streaming to Cloud", last_update_time)

        # Upload the final remaining part in the buffer
        if buffer:
            part_response = await asyncio.to_thread(upload_part, upload_id, part_number, bytes(buffer))
            parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})

        # 3. Complete the multipart upload
        await asyncio.to_thread(complete_upload, upload_id, parts)

    except Exception as e:
        # If something went wrong, abort the multipart upload
        if 'upload_id' in locals():
            await asyncio.to_thread(abort_upload, upload_id)
        await status_message.edit_text(f"❌ **Stream Upload Failed:** {e}")
        return

    # --- Generate Presigned URL ---
    try:
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=604800  # Link valid for 7 days
        )
        final_message = (
            f"✅ **Upload Successful!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"🔗 **Shareable Link:**\n`{presigned_url}`\n\n"
            f"This link will expire in 7 days."
        )
        await status_message.edit_text(final_message)
    except Exception as e:
        await status_message.edit_text(f"❌ **Could not generate link:** {e}")

# --- Run the Bot and Web Server ---
async def main():
    """Starts the bot and the web server concurrently."""
    # Set up web server
    webapp = web.Application()
    webapp.router.add_get("/", health_check)
    runner = web.AppRunner(webapp)
    await runner.setup()
    
    # Get port from environment variable, default to 5000 for local testing
    port = int(os.environ.get("PORT", 5000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    # Start bot and web server
    await app.start()
    await site.start()
    print(f"🚀 Bot and web server started on port {port}...")
    
    # Keep the application running until interrupted
    await idle()
    
    # Cleanup on shutdown
    await app.stop()
    await runner.cleanup()
    print("👋 Bot and web server have stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Interrupted by user. Shutting down...")
