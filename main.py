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
from botocore.config import Config as BotoConfig

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
# Extreme workers for maximum performance
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

# --- Extreme Boto3 Configuration for ULTRA TURBO SPEED ---
# Optimized for maximum parallel processing
boto_config = BotoConfig(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    max_pool_connections=100,  # Extreme connection pooling
    connect_timeout=30,
    read_timeout=60,
    tcp_keepalive=True
)

transfer_config = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,   # Start multipart for files > 5MB
    max_concurrency=50,                    # Extreme parallel threads
    multipart_chunksize=50 * 1024 * 1024,  # Larger chunks for fewer requests
    num_download_attempts=10,              # More retries for stability
    use_threads=True
)

# --- Initialize Boto3 Client for Wasabi with Extreme Settings ---
wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=boto_config  # Apply extreme config
)

# --- Rate limiting ---
user_limits = {}
MAX_REQUESTS_PER_MINUTE = 30  # Increased limit for power users
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB Ultra size limit

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
                "bucket": WASABI_BUCKET,
                "performance_mode": "ULTRA_TURBO"
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        elif self.path == '/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            current_time = time.time()
            active_users = len([k for k, v in user_limits.items() if any(current_time - t < 300 for t in v)])
            
            response = {
                "user_limits_count": len(user_limits),
                "active_users": active_users,
                "performance_mode": "ULTRA_TURBO"
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("Ultra Turbo Wasabi Storage Bot is running!".encode('utf-8'))

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

def create_ultra_progress_bar(percentage, length=12):
    """Create an ultra modern visual progress bar"""
    filled_length = int(length * percentage / 100)
    
    # Create a gradient effect based on progress
    if percentage < 25:
        filled_char = "âš¡"
        empty_char = "âš¡"
    elif percentage < 50:
        filled_char = "ðŸ”¥"
        empty_char = "âš¡"
    elif percentage < 75:
        filled_char = "ðŸš€"
        empty_char = "ðŸ”¥"
    else:
        filled_char = "ðŸ’¯"
        empty_char = "ðŸš€"
    
    bar = filled_char * filled_length + empty_char * (length - filled_length)
    return f"{bar}"

async def ultra_progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Ultra turbo progress reporter with extreme performance metrics"""
    last_update = 0
    speed_samples = []
    
    while status['running']:
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        # Calculate progress
        if total_size > 0:
            percentage = min((status['seen'] / total_size) * 100, 100)
        else:
            percentage = 0
        
        # Calculate speed with smoothing
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        speed_samples.append(speed)
        if len(speed_samples) > 5:
            speed_samples.pop(0)
        avg_speed = sum(speed_samples) / len(speed_samples) if speed_samples else 0
        
        # Calculate ETA
        remaining = total_size - status['seen']
        eta_seconds = remaining / avg_speed if avg_speed > 0 else 0
        
        # Format ETA
        if eta_seconds > 3600:
            eta = f"{int(eta_seconds/3600)}h {int((eta_seconds%3600)/60)}m"
        elif eta_seconds > 60:
            eta = f"{int(eta_seconds/60)}m {int(eta_seconds%60)}s"
        else:
            eta = f"{int(eta_seconds)}s" if eta_seconds > 0 else "Calculating..."
        
        # Create the progress bar with ultra design
        progress_bar = create_ultra_progress_bar(percentage)
        
        # Only update if significant change or every 1.5 seconds
        if current_time - last_update > 1.5 or abs(percentage - status.get('last_percentage', 0)) > 2:
            status['last_percentage'] = percentage
            
            # Use HTML formatting
            escaped_task = escape_html(task)
            
            # File name with ellipsis if too long
            display_task = escaped_task
            if len(display_task) > 35:
                display_task = display_task[:32] + "..."
            
            text = (
                f"<b>âš¡ ULTRA TURBO MODE</b>\n\n"
                f"<b>ðŸ“ {display_task}</b>\n\n"
                f"{progress_bar}\n"
                f"<b>{percentage:.1f}%</b> â€¢ {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
                f"<b>ðŸš€ Speed:</b> {humanbytes(avg_speed)}/s\n"
                f"<b>â±ï¸ ETA:</b> {eta}\n"
                f"<b>ðŸ•’ Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}\n"
                f"<b>ðŸ”§ Threads:</b> {transfer_config.max_concurrency}"
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
                        f"ULTRA TURBO MODE\n\n"
                        f"{display_task}\n\n"
                        f"{progress_bar}\n"
                        f"{percentage:.1f}% â€¢ {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
                        f"Speed: {humanbytes(avg_speed)}/s\n"
                        f"ETA: {eta}\n"
                        f"Elapsed: {time.strftime('%M:%S', time.gmtime(elapsed_time))}\n"
                        f"Threads: {transfer_config.max_concurrency}"
                    )
                    await message.edit_text(plain_text)
                    last_update = current_time
                except:
                    pass  # Ignore other edit errors
        
        await asyncio.sleep(0.8)  # Update faster for ultra mode

def ultra_pyrogram_progress_callback(current, total, message, start_time, task):
    """Ultra progress callback for Pyrogram's synchronous operations."""
    try:
        if not hasattr(ultra_pyrogram_progress_callback, 'last_edit_time') or time.time() - ultra_pyrogram_progress_callback.last_edit_time > 1.5:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            
            # Create an ultra progress bar
            bar_length = 10
            filled = int(bar_length * percentage / 100)
            bar = "ðŸš€" * filled + "âš¡" * (bar_length - filled)
            
            # Use HTML formatting
            escaped_task = escape_html(task)
            
            # Truncate long file names
            display_task = escaped_task
            if len(display_task) > 30:
                display_task = display_task[:27] + "..."
            
            elapsed_time = time.time() - start_time
            
            text = (
                f"<b>â¬‡ï¸ ULTRA DOWNLOAD</b>\n"
                f"<b>ðŸ“ {display_task}</b>\n"
                f"{bar} <b>{percentage:.1f}%</b>\n"
                f"<b>â±ï¸ Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
            )
            
            try:
                message.edit_text(text, parse_mode=ParseMode.HTML)
            except:
                # If HTML fails, try without formatting
                message.edit_text(
                    f"ULTRA DOWNLOAD\n"
                    f"{display_task}\n"
                    f"{bar} {percentage:.1f}%\n"
                    f"Elapsed: {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
                )
            ultra_pyrogram_progress_callback.last_edit_time = time.time()
    except Exception:
        pass

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
        
    # Send the welcome image with caption
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption="ðŸš€ <b>ULTRA TURBO CLOUD STORAGE BOT</b>\n\n"
                "Experience extreme speed with our optimized parallel processing technology!\n\n"
                "âž¡ï¸ <b>To upload:</b> Just send me any file (up to 10GB!)\n"
                "â¬…ï¸ <b>To download:</b> Use <code>/download &lt;file_name&gt;</code>\n"
                "ðŸ“‹ <b>To list files:</b> Use <code>/list</code>\n\n"
                "<b>âš¡ Extreme Performance Features:</b>\n"
                "â€¢ 50x Multi-threaded parallel processing\n"
                "â€¢ 10GB file size support\n"
                "â€¢ Adaptive retry system with 10 attempts\n"
                "â€¢ Real-time speed monitoring with smoothing\n"
                "â€¢ 100 connection pooling for maximum throughput\n"
                "â€¢ Memory optimization for large files\n"
                "â€¢ TCP Keepalive for stable connections\n\n"
                "<b>ðŸ’Ž Owner:</b> Mraprguild\n"
                "<b>ðŸ“§ Email:</b> mraprguild@gmail.com\n"
                "<b>ðŸ“± Telegram:</b> @Sathishkumar33",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("turbo"))
async def turbo_mode_command(client, message: Message):
    """Shows turbo mode status"""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
        
    await message.reply_text(
        f"âš¡ <b>ULTRA TURBO MODE ACTIVE</b>\n\n"
        f"<b>Max Concurrency:</b> {transfer_config.max_concurrency} threads\n"
        f"<b>Chunk Size:</b> {humanbytes(transfer_config.multipart_chunksize)}\n"
        f"<b>Multipart Threshold:</b> {humanbytes(transfer_config.multipart_threshold)}\n"
        f"<b>Max File Size:</b> {humanbytes(MAX_FILE_SIZE)}\n"
        f"<b>Connection Pool:</b> {boto_config.max_pool_connections} connections",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using extreme multipart transfers."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("âŒ Rate limit exceeded. Please try again in a minute.")
        return

    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    # Check file size limit
    if hasattr(media, 'file_size') and media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"âŒ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
        return

    file_path = None
    status_message = await message.reply_text("âš¡ Initializing ULTRA TURBO mode...", quote=True)

    try:
        await status_message.edit_text("â¬‡ï¸ Downloading from Telegram (Turbo Mode)...")
        file_path = await message.download(progress=ultra_pyrogram_progress_callback, progress_args=(status_message, time.time(), "Downloading"))
        
        file_name = f"{get_user_folder(message.from_user.id)}/{sanitize_filename(os.path.basename(file_path))}"
        status = {'running': True, 'seen': 0}
        
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount

        reporter_task = asyncio.create_task(
            ultra_progress_reporter(status_message, status, media.file_size, f"Uploading {os.path.basename(file_path)} (ULTRA TURBO)", time.time())
        )
        
        # Use thread pool for maximum parallelism
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: s3_client.upload_file(
                file_path,
                WASABI_BUCKET,
                file_name,
                Callback=boto_callback,
                Config=transfer_config  # <-- ULTRA TURBO SPEED
            )
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': file_name}, ExpiresIn=86400) # 24 hours
        
        # Use HTML formatting instead of markdown
        safe_file_name = escape_html(os.path.basename(file_path))
        safe_url = escape_html(presigned_url)
        
        await status_message.edit_text(
            f"âœ… <b>ULTRA TURBO UPLOAD COMPLETE!</b>\n\n"
            f"<b>ðŸ“ File:</b> <code>{safe_file_name}</code>\n"
            f"<b>ðŸ“¦ Size:</b> {humanbytes(media.file_size)}\n"
            f"<b>ðŸ”— Streamable Link (24h expiry):</b>\n<code>{safe_url}</code>\n\n"
            f"<b>âš¡ Performance:</b> Ultra Turbo Mode",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        await status_message.edit_text(f"âŒ An error occurred: {escape_html(str(e))}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using extreme multipart transfers."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
    
    {humanbytes(media.file_size)}\n"
            f"<b>ðŸ”— Streamable Link (24h expiry):</b>\n<code>{safe_url}</code>\n\n"
            f"<b>âš¡ Performance:</b> Ultra Turbo Mode",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        await status_message.edit_text(f"âŒ An error occurred: {escape_html(str(e))}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using extreme multipart transfers."""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("âŒ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("âŒ Rate limit exceeded. Please try again in a minute.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage: <code>/download &lt;file_name_in_wasabi&gt;</code>", parse_mode=ParseMode.HTML)
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    safe_file_name = escape_html(file_name)
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True)
    
    status_message = await message.reply_text(f"ðŸ” Searching for <code>{safe_file_name}</code>...", quote=True, parse_mode=ParseMode.HTML)

    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=user_file_name)
        total_size = int(meta.get('ContentLength', 0))

        # Check file size limit
        if total_size > MAX_FILE_SIZE:
            await status_message.edit_text(f"âŒ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
            return

        status = {'running': True, 'seen': 0}
        def boto_callback(bytes_amount):
            status['seen'] += bytes_amount
            
        reporter_task = asyncio.create_task(
            ultra_progress_reporter(status_message, status, total_size, f"Downloading {safe_file_name} (ULTRA TURBO)", time.time())
        )
        
        # Use thread pool for maximum parallelism
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: s3_client.download_file(
                WASABI_BUCKET,
                user_file_name,
                local_file_path,
                Callback=boto_callback,
                Config=transfer_config  # <-- ULTRA TURBO SPEED
            )
        )
        
        status['running'] = False
        await asyncio.sleep(0.1)  # Give the reporter task a moment to finish
        reporter_task.cancel()
        
        await status_message.edit_text("ðŸ“¤ Uploading to Telegram (Turbo Mode)...")
        await message.reply_document(
            document=local_file_path,
            caption=f"âœ… <b>ULTRA TURBO DOWNLOAD COMPLETE!</b>\n"
                    f"<b>File:</b> <code>{safe_file_name}</code>\n"
                    f"<b>Size:</b> {humanbytes(total_size)}\n"
                    f"<b>Mode:</b> âš¡ Ultra Turbo",
            parse_mode=ParseMode.HTML,
            progress=ultra_pyrogram_progress_callback,
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
    print("Starting ULTRA TURBO Wasabi Storage Bot with extreme performance settings...")
    
    # Start HTTP server in a separate thread for health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Start the Pyrogram bot with FloodWait handling
    max_retries = 5  # Increased retries for reliability
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print("Starting bot in ULTRA TURBO mode...")
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
