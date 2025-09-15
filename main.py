import os
import time
import math
import boto3
import asyncio
import re
import signal
import atexit
import threading
import socket
import json
import html
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from botocore.exceptions import NoCredentialsError, ClientError
from pyrogram.errors import FloodWait
from boto3.s3.transfer import TransferConfig

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")
AUTHORIZED_USERS = [int(user_id) for user_id in os.getenv("AUTHORIZED_USERS", "").split(",") if user_id]

# Welcome image URL (you can replace this with your own image)
WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/IMG-20250915-WA0013.jpg"

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- Initialize Pyrogram Client ---
# Increased workers for better performance with multiple concurrent tasks.
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=20)

# --- Boto3 Transfer Configuration for TURBO SPEED ---
# This enables multipart transfers and uses multiple threads for significant speed boosts.
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,   # Start multipart for files > 8MB
    max_concurrency=16,                   # 🚀 Increase parallel threads
    multipart_chunksize=32 * 1024 * 1024, # Larger chunks for fewer requests
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

# --- Rate limiting ---
user_limits = {}
MAX_REQUESTS_PER_MINUTE = 15
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB

# --- Authorization Check ---
async def is_authorized(user_id):
    return not AUTHORIZED_USERS or user_id in AUTHORIZED_USERS

# --- Simple HTTP Server for Health Checks ---
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "timestamp": time.time(),
                "bucket": WASABI_BUCKET
            }
            self.wfile.write(json.dumps(response).encode())
        elif self.path == '/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            current_time = time.time()
            active_users = len([k for k, v in user_limits.items() if any(current_time - t < 300 for t in v)])
            
            response = {
                "user_limits_count": len(user_limits),
                "active_users": active_users
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Wasabi Storage Bot is running!")

    def log_message(self, format, *args):
        # Disable logging to prevent conflicts with Pyrogram
        return

def run_http_server():
    # Use the PORT environment variable if available (common in cloud platforms)
    port = int(os.environ.get('PORT', 8080))
    
    # Create a simple HTTP server without signal handling
    with HTTPServer(('0.0.0.0', port), HealthHandler) as httpd:
        print(f"HTTP server running on port {port}")
        # Set timeout to prevent blocking
        httpd.timeout = 1
        while True:
            try:
                httpd.handle_request()
            except Exception as e:
                print(f"HTTP server error: {e}")
            time.sleep(5)  # Check for requests every 5 seconds

# --- Helper Functions & Classes ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power and t_n < len(power_dict) -1:
        size /= power
        t_n += 1
    return "{:.2f} {}".format(size, power_dict[t_n])

def sanitize_filename(filename):
    """Remove potentially dangerous characters from filenames"""
    # Keep only alphanumeric, spaces, dots, hyphens, and underscores
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
    # Limit length to avoid issues
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    return filename

def escape_html(text):
    """Escape HTML special characters"""
    if not text:
        return ""
    return html.escape(str(text))

def cleanup():
    """Clean up temporary files on exit"""
    for folder in ['.', './downloads']:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                if file.endswith('.tmp') or file.startswith('pyrogram'):
                    try:
                        os.remove(os.path.join(folder, file))
                    except:
                        pass

atexit.register(cleanup)

async def check_rate_limit(user_id):
    """Check if user has exceeded rate limits"""
    current_time = time.time()
    
    if user_id not in user_limits:
        user_limits[user_id] = []
    
    # Remove requests older than 1 minute
    user_limits[user_id] = [t for t in user_limits[user_id] if current_time - t < 60]
    
    if len(user_limits[user_id]) >= MAX_REQUESTS_PER_MINUTE:
        return False
    
    user_limits[user_id].append(current_time)
    return True

def get_user_folder(user_id):
    """Get user-specific folder path"""
    return f"user_{user_id}"

def create_modern_progress_bar(percentage, length=10):
    """Create a modern visual progress bar"""
    filled_length = int(length * percentage / 100)
    
    # Create a gradient effect based on progress
    if percentage < 30:
        filled_char = "▰"
        empty_char = "▱"
    elif percentage < 70:
        filled_char = "🟦"
        empty_char = "⬜"
    else:
        filled_char = "🟩"
        empty_char = "⬜"
    
    bar = filled_char * filled_length + empty_char * (length - filled_length)
    
    # Add percentage indicator at the end
    return f"{bar}"

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Modern progress reporter with sleek visual design"""
    last_update = 0
    
    while status['running']:
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        # Calculate progress
        if total_size > 0:
            percentage = min((status['seen'] / total_size) * 100, 100)
        else:
            percentage = 0
        
        # Calculate speed and ETA
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        remaining = total_size - status['seen']
        eta_seconds = remaining / speed if speed > 0 else 0
        
        # Format ETA
        if eta_seconds > 3600:
            eta = f"{int(eta_seconds/3600)}h {int((eta_seconds%3600)/60)}m"
        elif eta_seconds > 60:
            eta = f"{int(eta_seconds/60)}m {int(eta_seconds%60)}s"
        else:
            eta = f"{int(eta_seconds)}s" if eta_seconds > 0 else "Calculating..."
        
        # Create the progress bar with a modern design
        progress_bar = create_modern_progress_bar(percentage)
        
        # Only update if significant change or every 2 seconds
        if current_time - last_update > 2 or abs(percentage - status.get('last_percentage', 0)) > 3:
            status['last_percentage'] = percentage
            
            # Use HTML formatting
            escaped_task = escape_html(task)
            
            # File name with ellipsis if too long
            display_task = escaped_task
            if len(display_task) > 35:
                display_task = display_task[:32] + "..."
            
            text = (
                f"<b>🚀 TURBO UPLOAD</b>\n\n"
                f"<b>📁 {display_task}</b>\n\n"
                f"{progress_bar}\n"
                f"<b>{percentage:.1f}%</b> • {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
                f"<b>⚡ Speed:</b> {humanbytes(speed)}/s\n"
                f"<b>⏱️ ETA:</b> {eta}\n"
                f"<b>🕒 Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
            )
            
            try:
                await message.edit_text(text, parse_mode=ParseMode.HTML)
                last_update = current_time
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                # If HTML fails, try without formatting
                try:
                    plain_text = (
                        f"TURBO UPLOAD\n\n"
                        f"{display_task}\n\n"
                        f"{progress_bar}\n"
                        f"{percentage:.1f}% • {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
                        f"Speed: {humanbytes(speed)}/s\n"
                        f"ETA: {eta}\n"
                        f"Elapsed: {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
                    )
                    await message.edit_text(plain_text)
                    last_update = current_time
                except:
                    pass  # Ignore other edit errors
        
        await asyncio.sleep(1)  # Update every second

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations with new design."""
    try:
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or time.time() - pyrogram_progress_callback.last_edit_time > 2:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            
            # Create a modern progress bar
            bar_length = 8
            filled = int(bar_length * percentage / 100)
            bar = "▰" * filled + "▱" * (bar_length - filled)
            
            # Use HTML formatting
            escaped_task = escape_html(task)
            
            # Truncate long file names
            display_task = escaped_task
            if len(display_task) > 30:
                display_task = display_task[:27] + "..."
            
            elapsed_time = time.time() - start_time
            
            text = (
                f"<b>⬇️ DOWNLOADING</b>\n"
                f"<b>📁 {display_task}</b>\n"
                f"{bar} <b>{percentage:.1f}%</b>\n"
                f"<b>⏱️ Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
            )
            
            try:
                message.edit_text(text, parse_mode=ParseMode.HTML)
            except:
                # If HTML fails, try without formatting
                message.edit_text(
                    f"DOWNLOADING\n"
                    f"{display_task}\n"
                    f"{bar} {percentage:.1f}%\n"
                    f"Elapsed: {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
                )
            pyrogram_progress_callback.last_edit_time = time.time()
    except Exception:
        pass

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
        
    # Send the welcome image with caption
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption="Hello! I am a <b>Turbo-Speed</b> Cloud storage bot.\n\n"
                "I use parallel processing to make transfers incredibly fast.\n\n"
                "➡️ <b>To upload:</b> Just send me any file.\n"
                "⬅️ <b>To download:</b> Use <code>/download &lt;file_name&gt;</code>\n"
                "📋 <b>To list files:</b> Use <code>/list</code>\n\n"
                "<b>Performance Features:</b>\n"
        "Multi-threaded parallel processing\n"
        "4GB file size support\n"
        "Adaptive retry system\n"
        "Real-time speed monitoring\n"
        "Connection pooling\n"
        "Memory optimization\n\n"
        "<b>Owner:</b> Mraprguild\n"
        "<b>Email:</b> mraprguild@gmail.com"
        "<b>Telegram:</b> @Sathishkumar33",
        
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    # Check file size limit
    if hasattr(media, 'file_size') and media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
        return

    file_path = None
    status_message = await message.reply_text("🔄 Processing your request...", quote=True)

    try:
        await status_message.edit_text("⬇️ Downloading from Telegram...")
        file_path = await message.download(progress=pyrogram_progress_callback, progress_args=(status_message, time.time(), "Downloading"))
        
        file_name = f"{get_user_folder(message.from_user.id)}/{sanitize_filename(os.path.basename(file_path))}"
        status = {'running': True, 'seen': 0}
        
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, media.file_size, f"Uploading {os.path.basename(file_path)} (Turbo)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=boto_callback,
            Config=transfer_config  # <-- TURBO SPEED ENABLED
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400) # 24 hours
        
        # Use HTML formatting instead of markdown
        safe_file_name = escape_html(os.path.basename(file_path))
        safe_url = escape_html(presigned_url)
        
        await status_message.edit_text(
            f"✅ <b>Upload Successful!</b>\n\n"
            f"<b>📁 File:</b> <code>{safe_file_name}</code>\n"
            f"<b>📦 Size:</b> {humanbytes(media.file_size)}\n"
            f"<b>🔗 Streamable Link (24h expiry):</b>\n<code>{safe_url}</code>",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {escape_html(str(e))}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using multipart transfers."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage: <code>/download &lt;file_name_in_wasabi&gt;</code>", parse_mode=ParseMode.HTML)
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    safe_file_name = escape_html(file_name)
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True)
    
    status_message = await message.reply_text(f"🔍 Searching for <code>{safe_file_name}</code>...", quote=True, parse_mode=ParseMode.HTML)

    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=user_file_name)
        total_size = int(meta.get('ContentLength', 0))

        # Check file size limit
        if total_size > MAX_FILE_SIZE:
            await status_message.edit_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
            return

        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount
            
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"Downloading {safe_file_name} (Turbo)", time.time())
        )
        
        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            user_file_name,
            local_file_path,
            Callback=boto_callback,
            Config=transfer_config  # <-- TURBO SPEED ENABLED
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()
        
        await status_message.edit_text("ðŸ“¤ Uploading to Telegram...")
        await message.reply_document(
            document=local_file_path,
            caption=f"âœ… <b>Download Complete:</b> <code>{safe_file_name}</code>\n"
                    f"ðŸ“¦ <b>Size:</b> {humanbytes(total_size)}",
            parse_mode=ParseMode.HTML,
            progress=pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "Uploading to Telegram")
        )
        
        await status_message.delete()

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            await status_message.edit_text(f"âŒ <b>Error:</b> File not found in Wasabi: <code>{safe_file_name}</code>", parse_mode=ParseMode.HTML)
        elif error_code == '403':
            await status_message.edit_text("âŒ <b>Error:</b> Access denied. Check your Wasabi credentials.", parse_mode=ParseMode.HTML)
        elif error_code == 'NoSuchBucket':
            await status_message.edit_text("âŒ <b>Error:</b> Bucket does not exist.", parse_mode=ParseMode.HTML)
        else:
            error_msg = escape_html(str(e))
            await status_message.edit_text(f"âŒ <b>S3 Error:</b> {error_code} - {error_msg}", parse_mode=ParseMode.HTML)
    except Exception as e:
        error_msg = escape_html(str(e))
        await status_message.edit_text(f"âŒ <b>An unexpected error occurred:</b> {error_msg}", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in the Wasabi bucket"""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("âŒ Rate limit exceeded. Please try again in a minute.")
        return
        
    try:
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET, Prefix=user_prefix)
        
        if 'Contents' not in response:
            await message.reply_text("ðŸ“‚ No files found in your storage.")
            return
        
        # Remove the user prefix from displayed filenames
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        safe_files = [escape_html(file) for file in files[:20]]  # Show first 20 files
        files_list = "\n".join([f"â€¢ <code>{file}</code>" for file in safe_files])
        
        if len(files) > 20:
            files_list += f"\n\n...and {len(files) - 20} more files"
        
        await message.reply_text(f"ðŸ“ <b>Your files:</b>\n\n{files_list}", parse_mode=ParseMode.HTML)
    
    except Exception as e:
        error_msg = escape_html(str(e))
        await message.reply_text(f"âŒ Error listing files: {error_msg}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with TURBO-SPEED settings...")
    
    # Start HTTP server in a separate thread for health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Start the Pyrogram bot with FloodWait handling
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print("Starting bot...")
            app.run()
            break
        except FloodWait as e:
            retry_count += 1
            wait_time = e.value + 5
            print(f"Telegram flood wait error: Need to wait {e.value} seconds")
            print(f"Waiting {wait_time} seconds before retry {retry_count}/{max_retries}...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    print("Bot has stopped.")
