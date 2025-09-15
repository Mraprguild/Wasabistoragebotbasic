import os
import time
import math
import boto3
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from botocore.exceptions import NoCredentialsError, ClientError
from pyrogram.errors import FloodWait
from boto3.s3.transfer import TransferConfig

# --- Load environment variables ---
load_dotenv()

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
PORT = int(os.environ.get("PORT")

# --- Constants ---
FILES_PER_PAGE = 10
DOWNLOAD_DIR = "./downloads/"

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    logger.critical("Missing one or more required environment variables. Please check your configuration.")
    exit()

# --- Initialize Pyrogram Client ---
app = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=20,
    in_memory=True
)

# --- Boto3 Transfer Configuration for EXTREME SPEED ---
transfer_config = TransferConfig(
    multipart_threshold=25 * 1024 * 1024,
    max_concurrency=40,
    multipart_chunksize=16 * 1024 * 1024,
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

# --- Helper Functions & Classes ---
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

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Asynchronously reports progress of a background task."""
    while status.get('running', False):
        percentage = (status['seen'] / total_size) * 100 if total_size > 0 else 0
        percentage = min(percentage, 100)
        elapsed_time = time.time() - start_time
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        eta_seconds = (total_size - status['seen']) / speed if speed > 0 else 0
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds))

        progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), ' ' * (10 - int(percentage / 10)))

        text = (
            f"**{task}...**\n\n"
            f"{progress_bar} {percentage:.2f}%\n\n"
            f"**Done:** {humanbytes(status['seen'])} of {humanbytes(total_size)}\n"
            f"**Speed:** {humanbytes(speed)}/s\n"
            f"**ETA:** {eta}"
        )
        try:
            await message.edit_text(text)
        except FloodWait as e:
            await asyncio.sleep(e.x)
        except Exception:
            pass  # Ignore other edit errors
        await asyncio.sleep(3)

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations."""
    try:
        now = time.time()
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or now - pyrogram_progress_callback.last_edit_time > 3:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            text = f"**{task}...** {percentage:.2f}%"
            # Use asyncio.create_task to avoid blocking the callback
            asyncio.create_task(message.edit_text(text))
            pyrogram_progress_callback.last_edit_time = now
    except Exception as e:
        logger.warning(f"Error in pyrogram_progress_callback: {e}")

async def send_file_list_page(message_or_query, files: list, page: int = 0, query_str: str = ""):
    """Creates and sends a paginated list of files with interactive buttons."""
    start_offset = page * FILES_PER_PAGE
    end_offset = start_offset + FILES_PER_PAGE
    paginated_files = files[start_offset:end_offset]

    if not paginated_files and page == 0:
        text = "✅ Your Wasabi bucket is empty." if not query_str else f"❌ No files found matching `{query_str}`."
        # Determine how to edit the message based on the input type
        editor = message_or_query.edit_text if isinstance(message_or_query, Message) else message_or_query.message.edit_text
        await editor(text)
        return

    buttons = []
    for file in paginated_files:
        file_name = file['Key']
        file_size = humanbytes(file['Size'])
        # Simplified callback for direct download
        buttons.append([
            InlineKeyboardButton(f"📄 {file_name} ({file_size})", callback_data=f"download:{file_name}")
        ])

    total_pages = math.ceil(len(files) / FILES_PER_PAGE)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page:{page - 1}:{query_str}"))
    if total_pages > 1:
        nav_buttons.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
    if end_offset < len(files):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page:{page + 1}:{query_str}"))

    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"page:{page}:{query_str}")])

    keyboard = InlineKeyboardMarkup(buttons)
    text = "**Files in your Wasabi Bucket:**" if not query_str else f"**Search results for `{query_str}`:**"
    
    editor = message_or_query.edit_text if isinstance(message_or_query, Message) else message_or_query.message.edit_text
    await editor(text, reply_markup=keyboard)


# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(_, message: Message):
    """Handles the /start command with a welcome image."""
    # A cool, tech-themed image URL for the welcome message
    welcome_image_url = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/welcome.jpg" 
    
    welcome_caption = (
        "Hello! I am an **Extreme-Speed** Wasabi storage bot.\n\n"
        "I use aggressive parallel processing to make transfers incredibly fast.\n\n"
        "➡️ **To upload:** Just send me any file.\n"
        "⬅️ **To download:** Use the buttons in the file list.\n"
        "📂 **To list files:** Use `/list`.\n"
        "🔎 **To search files:** Use `/search <query>`.\n\n"
        "Generated links are direct streamable links valid for 24 hours."
    )
    
    try:
        await message.reply_photo(
            photo=welcome_image_url,
            caption=welcome_caption
        )
    except Exception as e:
        logger.error(f"Failed to send welcome photo, falling back to text: {e}")
        await message.reply_text(welcome_caption)


@app.on_message(filters.command(["list", "search"]))
async def list_or_search_files_handler(_, message: Message):
    """Handles /list and /search commands."""
    status_message = await message.reply_text("🔎 Fetching file list from Wasabi...", quote=True)
    query_str = " ".join(message.command[1:]) if message.command[0] == "search" else ""
    
    try:
        response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET)
        
        if 'Contents' in response:
            all_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            
            filtered_files = [f for f in all_files if query_str.lower() in f['Key'].lower()] if query_str else all_files
            
            await send_file_list_page(status_message, filtered_files, page=0, query_str=query_str)
        else:
            await status_message.edit_text("✅ Your Wasabi bucket is empty.")

    except ClientError as e:
        await status_message.edit_text(f"❌ **S3 Client Error:** Could not list files. Check credentials. Details: {e}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")


@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(_, message: Message):
    """Handles file uploads to Wasabi."""
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    status_message = await message.reply_text("Processing your request...", quote=True)

    try:
        file_name = media.file_name or "file_from_telegram.unknown"
        
        # Check if file exists
        try:
            await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
            # Store message details for the callback to retrieve
            # This is a simplified way; a more robust solution would use a dictionary or cache
            app.overwrite_map = {status_message.id: message}
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Overwrite", callback_data=f"overwrite:{status_message.id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")]
            ])
            await status_message.edit_text(f"⚠️ File `{file_name}` already exists. Do you want to overwrite it?", reply_markup=keyboard)
            return
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise # Re-raise unexpected errors

        # Proceed with upload if file does not exist
        await perform_upload(status_message, message, file_name)

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred during upload: {str(e)}")
        logger.error(f"Upload error: {e}", exc_info=True)


async def perform_upload(status_message: Message, original_message: Message, file_name: str):
    """Core logic to download from Telegram and upload to Wasabi."""
    file_path = None
    media = original_message.document or original_message.video or original_message.audio or original_message.photo
    
    try:
        await status_message.edit_text("Downloading from Telegram...")
        # Ensure download path is unique to avoid conflicts
        download_path = os.path.join(DOWNLOAD_DIR, f"{time.time()}_{file_name}")
        file_path = await original_message.download(file_name=download_path)
        
        status = {'running': True, 'seen': 0}
        
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, media.file_size, f"Uploading `{file_name}` (Extreme Speed)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_callback,
            Config=transfer_config
        )
        
        status['running'] = False
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400)
        
        await status_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Streamable Link (24h expiry):**\n`{presigned_url}`"
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {str(e)}")
        logger.error(f"perform_upload error: {e}", exc_info=True)
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


async def perform_download(status_message: Message, file_name: str, chat_id: int):
    """Core logic to download from Wasabi and upload to Telegram."""
    local_file_path = os.path.join(DOWNLOAD_DIR, file_name)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=file_name)
        total_size = int(meta.get('ContentLength', 0))

        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount
            
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading `{file_name}` (Extreme Speed)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            file_name,
            local_file_path,
            Callback=boto_callback,
            Config=transfer_config
        )
        
        status['running'] = False
        reporter_task.cancel()
        await asyncio.sleep(1)
        
        await status_message.edit_text("Uploading to Telegram...")
        await app.send_document(
            chat_id=chat_id,
            document=local_file_path,
            progress=pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "Uploading")
        )
        
        await status_message.delete()

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            await status_message.edit_text(f"❌ **Error:** File not found in Wasabi: `{file_name}`")
        else:
            await status_message.edit_text(f"❌ **S3 Client Error:** {e}")
    except Exception as e:
        await status_message.edit_text(f"❌ **An unexpected error occurred:** {str(e)}")
        logger.error(f"Download error: {e}", exc_info=True)
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

@app.on_callback_query()
async def callback_query_handler(_, query: CallbackQuery):
    """Handles all button presses from inline keyboards."""
    try:
        await query.answer() # Acknowledge the button press immediately
    except Exception:
        pass # Ignore if message is too old
        
    data = query.data
    parts = data.split(":", 2)
    action = parts[0]
    
    if action == "noop":
        return

    if action == "page":
        page = int(parts[1])
        query_str = parts[2] if len(parts) > 2 else ""
        
        try:
            response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET)
            all_files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
            filtered_files = [f for f in all_files if query_str.lower() in f['Key'].lower()] if query_str else all_files
            await send_file_list_page(query, filtered_files, page, query_str)
        except Exception as e:
            await query.message.edit_text(f"❌ Error refreshing list: {e}")

    elif action == "download":
        file_name = parts[1]
        status_message = await query.message.edit_text(f"Preparing to download `{file_name}`...")
        await perform_download(status_message, file_name, query.message.chat.id)
    
    elif action == "overwrite":
        original_message_id = int(parts[1])
        # Retrieve the original message using our map
        original_message = getattr(app, 'overwrite_map', {}).get(original_message_id)
        
        if not original_message:
            await query.message.edit_text("❌ Error: Could not find the original file message. Please try sending the file again.")
            return

        file_name = (original_message.document or original_message.video or original_message.audio or original_message.photo).file_name
        await query.message.edit_text(f"Acknowledged. Overwriting `{file_name}`...")
        await perform_upload(query.message, original_message, file_name)
        # Clean up the map
        del app.overwrite_map[original_message_id]

    elif action == "cancel_upload":
        await query.message.edit_text("✅ Upload cancelled.")

# --- Health Check Endpoint for Render ---
from aiohttp import web

async def health_check(_):
    return web.Response(text="OK")

# --- Main Execution with Render Support ---
if __name__ == "__main__":
    logger.info("Bot is starting with EXTREME-SPEED settings...")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    async def start_web_server():
        app_web = web.Application()
        app_web.router.add_get('/health', health_check)
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"Health check server started on port {PORT}")
    
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(start_web_server())
        app.run()
    except Exception as e:
        logger.critical(f"Bot failed to start: {e}", exc_info=True)
    finally:
        logger.info("Bot has stopped.")
