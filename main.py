import os
import time
import asyncio
import logging
import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from aiohttp import web

from config import config

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Initialize Pyrogram Client ---
try:
    app = Client(
        "telegram_file_bot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN
    )
except Exception as e:
    logger.error(f"Failed to initialize Pyrogram client: {e}")
    exit(1)


# --- Initialize Boto3 S3 Client for Wasabi ---
try:
    s3 = boto3.client(
        's3',
        endpoint_url=config.WASABI_ENDPOINT_URL,
        aws_access_key_id=config.WASABI_ACCESS_KEY,
        aws_secret_access_key=config.WASABI_SECRET_KEY,
        region_name=config.WASABI_REGION,
        config=BotoConfig(signature_version='s3v4')
    )
    logger.info("Boto3 S3 client initialized successfully for Wasabi.")
except (NoCredentialsError, PartialCredentialsError) as e:
    logger.error(f"Credentials not available for Boto3 client: {e}")
    exit(1)
except Exception as e:
    logger.error(f"Failed to initialize Boto3 S3 client: {e}")
    exit(1)

# --- Web Server for Health Checks ---
routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    """Handles the root endpoint for health checks. Now checks bot connection status."""
    if app.is_connected:
        return web.json_response({
            "status": "ok",
            "message": "Web server is running and the bot is connected to Telegram."
        })
    else:
        return web.json_response({
            "status": "error",
            "message": "Web server is running, but the bot has disconnected from Telegram."
        }, status=503) # 503 Service Unavailable
        
# --- Helper Functions ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: "B", 1: "KB", 2: "MB", 3: "GB", 4: "TB"}
    while size > power:
        size /= power
        t_n += 1
    return "{:.2f} {}".format(size, power_dict[t_n])

# --- Progress Callback Functions ---
async def upload_progress_callback(current, total, message: Message, start_time):
    """Monitors and displays upload progress."""
    elapsed_time = time.time() - start_time
    if elapsed_time == 0:
        return
    speed = current / elapsed_time
    percentage = current * 100 / total
    progress_str = "[{:<20}] {:.2f}%".format(
        '=' * int(percentage / 5),
        percentage
    )
    eta = (total - current) / speed
    
    try:
        await message.edit_text(
            f"**Uploading to Telegram...**\n"
            f"{progress_str}\n"
            f"`{humanbytes(current)}` of `{humanbytes(total)}`\n"
            f"**Speed:** `{humanbytes(speed)}/s`\n"
            f"**ETA:** `{time.strftime('%H:%M:%S', time.gmtime(eta))}`"
        )
    except FloodWait as e:
        await asyncio.sleep(e.x)
    except Exception:
        pass

async def download_progress_callback(current, total, message: Message, start_time):
    """Monitors and displays download progress."""
    elapsed_time = time.time() - start_time
    if elapsed_time == 0:
        return
    speed = current / elapsed_time
    percentage = current * 100 / total
    progress_str = "[{:<20}] {:.2f}%".format(
        '=' * int(percentage / 5),
        percentage
    )
    
    try:
        await message.edit_text(
            f"**Downloading from Telegram...**\n"
            f"{progress_str}\n"
            f"`{humanbytes(current)}` of `{humanbytes(total)}`\n"
            f"**Speed:** `{humanbytes(speed)}/s`"
        )
    except FloodWait as e:
        await asyncio.sleep(e.x)
    except Exception:
        pass


# --- Bot Command Handlers ---

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handler for the /start command."""
    await message.reply_text(
        "**Welcome to the File Storage Bot!**\n\n"
        "I can upload files up to 4GB to Wasabi Cloud Storage.\n\n"
        "**How to use:**\n"
        "- Just send me any file or reply to a file with `/upload`.\n"
        "- Use `/help` to see all available commands."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    """Handler for the /help command."""
    await message.reply_text(
        "**Available Commands:**\n\n"
        "`/start` - Show welcome message.\n"
        "`/upload` - Upload a file (or just send it).\n"
        "`/download <file_id>` - Download a file by its ID.\n"
        "`/list` - List all your stored files.\n"
        "`/stream <file_id>` - Get a streaming link for a media file.\n"
        "`/test` - Check the connection to Wasabi storage.\n"
        "`/help` - Display this help message."
    )

@app.on_message(filters.command("test"))
async def test_wasabi_command(client, message: Message):
    """Handler for the /test command to check Wasabi connection."""
    status_msg = await message.reply_text("Testing Wasabi connection...")
    try:
        s3.list_buckets()
        await status_msg.edit_text("✅ **Wasabi connection successful!**")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        await status_msg.edit_text(f"❌ **Wasabi connection failed!**\n\n`Error: {error_code}`\n\nPlease check your Wasabi credentials and bucket configuration.")
    except Exception as e:
        await status_msg.edit_text(f"❌ **An unexpected error occurred.**\n\n`{str(e)}`")


@app.on_message(filters.document | filters.video | filters.audio | filters.photo | filters.command("upload"))
async def upload_file_handler(client, message: Message):
    """Handles file uploads."""
    if message.reply_to_message:
        file_message = message.reply_to_message
    else:
        file_message = message

    media = (
        file_message.document or 
        file_message.video or 
        file_message.audio or 
        file_message.photo
    )

    if not media:
        await message.reply_text("Please send a file or reply to a file with `/upload`.")
        return
        
    if media.file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("Sorry, I can only handle files up to 4GB.")
        return

    file_name = getattr(media, 'file_name', 'upload.dat')
    file_id = media.file_unique_id
    file_path = f"downloads/{file_id}_{file_name}"
    
    if file_id in config.FILE_DATABASE:
        file_info = config.FILE_DATABASE[file_id]
        await message.reply_text(
            f"This file is already in storage.\n\n**File:** `{file_info['file_name']}`\n"
            f"**File ID:** `{file_id}`",
            reply_markup=get_file_buttons(file_id, file_info['wasabi_url'])
        )
        return

    download_status_msg = await message.reply_text("Starting download from Telegram...")
    start_time = time.time()
    try:
        await client.download_media(
            message=file_message,
            file_name=file_path,
            progress=download_progress_callback,
            progress_args=(download_status_msg, start_time)
        )
    except Exception as e:
        await download_status_msg.edit_text(f"Failed to download file: {e}")
        logger.error(f"Download failed for {file_name}: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
        return
        
    await download_status_msg.edit_text("Download complete. Now uploading to Wasabi...")

    try:
        s3.upload_file(file_path, config.WASABI_BUCKET, file_name)
        wasabi_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': config.WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=3600*24*7 # Link valid for 7 days
        )
    except Exception as e:
        await download_status_msg.edit_text(f"Failed to upload to Wasabi: {e}")
        logger.error(f"Wasabi upload failed for {file_name}: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
        return

    channel_message_id = None
    if config.STORAGE_CHANNEL_ID:
        try:
            upload_status_msg = await message.reply_text("Backing up file to storage channel...")
            start_time_ch = time.time()
            backup_message = await client.send_document(
                chat_id=config.STORAGE_CHANNEL_ID,
                document=file_path,
                caption=f"**File:** `{file_name}`\n**File ID:** `{file_id}`",
                progress=upload_progress_callback,
                progress_args=(upload_status_msg, start_time_ch)
            )
            channel_message_id = backup_message.id
            await upload_status_msg.delete()
        except Exception as e:
            await message.reply_text(f"⚠️ **Warning:** Could not back up file to channel.\n`{e}`")
            logger.warning(f"Failed to backup {file_name} to channel: {e}")
            
    config.FILE_DATABASE[file_id] = {
        'file_name': file_name,
        'wasabi_url': wasabi_url,
        'channel_message_id': channel_message_id
    }

    await download_status_msg.edit_text(
        f"✅ **Upload Successful!**\n\n"
        f"**File:** `{file_name}`\n"
        f"**Size:** `{humanbytes(media.file_size)}`\n"
        f"**File ID:** `{file_id}` (use this to download/stream)",
        reply_markup=get_file_buttons(file_id, wasabi_url)
    )

    if os.path.exists(file_path):
        os.remove(file_path)

# --- Utility Functions for Buttons ---
def get_file_buttons(file_id, wasabi_url):
    """Generates inline buttons for a file."""
    buttons = [
        [InlineKeyboardButton("🔗 Get Download Link", callback_data=f"getlink_{file_id}")],
    ]
    if wasabi_url:
        mx_player_link = f"intent:{wasabi_url}#Intent;package=com.mxtech.videoplayer.ad;end"
        vlc_link = f"vlc://{wasabi_url}"
        buttons.append([
            InlineKeyboardButton("▶️ Stream in MX Player", url=mx_player_link),
            InlineKeyboardButton("🟠 Stream in VLC", url=vlc_link),
        ])
    return InlineKeyboardMarkup(buttons)
    
@app.on_callback_query(filters.regex(r"^getlink_"))
async def get_link_callback(client, callback_query: CallbackQuery):
    """Callback for 'Get Download Link' button."""
    file_id = callback_query.data.split("_")[1]
    if file_id in config.FILE_DATABASE:
        file_info = config.FILE_DATABASE[file_id]
        new_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': config.WASABI_BUCKET, 'Key': file_info['file_name']},
            ExpiresIn=3600 # Valid for 1 hour
        )
        await callback_query.message.reply_text(
            f"**Direct Download Link for `{file_info['file_name']}`:**\n\n"
            f"`{new_url}`\n\n"
            f"This link is valid for 1 hour."
        )
        await callback_query.answer("Link generated!", show_alert=False)
    else:
        await callback_query.answer("Sorry, file not found in database.", show_alert=True)
        
@app.on_message(filters.command("list"))
async def list_files_command(client, message: Message):
    if not config.FILE_DATABASE:
        await message.reply_text("No files have been uploaded yet.")
        return
        
    response_text = "**Stored Files:**\n\n"
    for file_id, data in config.FILE_DATABASE.items():
        response_text += f"- **{data['file_name']}** (ID: `{file_id}`)\n"
        
    await message.reply_text(response_text)
    
# --- Main Execution ---
async def main():
    """Initializes and runs the bot and web server, with graceful shutdown."""
    if not os.path.exists('downloads'):
        os.makedirs('downloads')
        
    runner = None
    try:
        web_app = web.Application(client_max_size=30000000)
        web_app.add_routes(routes)
        runner = web.AppRunner(web_app)

        # Step 1: Start and AUTHENTICATE the Pyrogram client FIRST
        await app.start()
        logger.info("Bot client connected to Telegram.")
        
        # NEW: Verify authentication by fetching bot info. This will crash if keys are bad.
        bot_info = await app.get_me()
        logger.info(f"✅ BOT AUTHENTICATED: Logged in as {bot_info.first_name} (@{bot_info.username}).")

        # Step 2: Start the web server ONLY after the bot is confirmed running
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", config.PORT)
        await site.start()
        logger.info(f"✅ WEB SERVER RUNNING: Health check server started on port {config.PORT}.")

        logger.info("All services are running. Idling to listen for updates...")
        await idle()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"A critical error occurred during startup: {e}", exc_info=True)
    finally:
        logger.info("Initiating graceful shutdown...")
        if app.is_initialized:
            await app.stop()
            logger.info("Pyrogram client stopped.")
        
        if runner:
            await runner.cleanup()
            logger.info("Web server cleaned up.")
        
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application has been shut down.")
