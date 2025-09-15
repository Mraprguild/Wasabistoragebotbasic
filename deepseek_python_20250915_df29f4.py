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
from pyrogram.errors import FloodWait, RPCError
from boto3.s3.transfer import TransferConfig
import aiohttp
import aiofiles
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing

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

# Welcome image URL
WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/IMG-20250915-WA0013.jpg"

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- MAXIMUM PERFORMANCE SETTINGS ---
# Get number of CPU cores for optimal parallel processing
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = min(100, CPU_CORES * 10)  # Maximum workers based on CPU cores

# --- Initialize Pyrogram Client with MAXIMUM performance settings ---
app = Client(
    "wasabi_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN, 
    workers=MAX_WORKERS,  # Maximum workers for parallel processing
    max_concurrent_transmissions=15,  # Increased concurrent transmissions
    sleep_threshold=120,  # Higher sleep threshold for better performance
    no_updates=True,  # Disable update handling for better performance
    in_memory=True  # Store sessions in memory for faster access
)

# --- Boto3 Transfer Configuration for MAXIMUM SPEED ---
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,  # Lower threshold for multipart (8MB)
    max_concurrency=MAX_WORKERS,  # Maximum concurrent threads
    multipart_chunksize=8 * 1024 * 1024,  # Optimized chunk size (8MB)
    use_threads=True,
    num_download_attempts=15,  # More retry attempts
    max_io_queue=2000  # Larger IO queue
)

# --- Initialize Boto3 Client for Wasabi with performance settings ---
wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=boto3.session.Config(
        max_pool_connections=MAX_WORKERS * 2,  # Maximum connection pool size
        retries={'max_attempts': 15, 'mode': 'adaptive'},  # More retry attempts with adaptive mode
        connect_timeout=15,  # Connection timeout
        read_timeout=30,  # Read timeout
        signature_version='s3v4',  # Latest signature version
        s3={'addressing_style': 'virtual'}  # Virtual addressing style for better performance
    )
)

# Create thread pools for parallel operations
io_thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
cpu_thread_pool = ThreadPoolExecutor(max_workers=CPU_CORES)

# --- Rate limiting ---
user_limits = {}
MAX_REQUESTS_PER_MINUTE = 15  # Increased limit for premium users
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB increased limit

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
                "performance": {
                    "max_workers": MAX_WORKERS,
                    "cpu_cores": CPU_CORES
                }
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
                "active_users": active_users,
                "thread_pool_stats": {
                    "io_pool_active": io_thread_pool._work_queue.qsize(),
                    "cpu_pool_active": cpu_thread_pool._work_queue.qsize()
                }
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Ultra-Speed Wasabi Storage Bot is running!")

    def log_message(self, format, *args):
        return

def run_http_server():
    port = int(os.environ.get('PORT', 8080))
    
    with HTTPServer(('0.0.0.0', port), HealthHandler) as httpd:
        print(f"HTTP server running on port {port}")
        httpd.timeout = 1
        while True:
            try:
                httpd.handle_request()
            except Exception as e:
                print(f"HTTP server error: {e}")
            time.sleep(5)

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
    return "{:.2f}".format(size) + power_dict[t_n]

def sanitize_filename(filename):
    """Remove potentially dangerous characters from filenames"""
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
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
    
    user_limits[user_id] = [t for t in user_limits[user_id] if current_time - t < 60]
    
    if len(user_limits[user_id]) >= MAX_REQUESTS_PER_MINUTE:
        return False
    
    user_limits[user_id].append(current_time)
    return True

def get_user_folder(user_id):
    """Get user-specific folder path"""
    return f"user_{user_id}"

async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    """Asynchronously reports progress of a background task."""
    last_update = 0
    last_seen = 0
    speed_samples = []
    
    while status['running']:
        current_time = time.time()
        if current_time - last_update < 0.5:  # Update more frequently
            await asyncio.sleep(0.05)
            continue
            
        percentage = (status['seen'] / total_size) * 100 if total_size > 0 else 0
        percentage = min(percentage, 100)

        elapsed_time = current_time - start_time
        instant_speed = (status['seen'] - last_seen) / (current_time - last_update) if last_update > 0 else 0
        
        # Calculate average speed from samples
        speed_samples.append(instant_speed)
        if len(speed_samples) > 10:
            speed_samples.pop(0)
        avg_speed = sum(speed_samples) / len(speed_samples) if speed_samples else 0
        
        eta_seconds = (total_size - status['seen']) / avg_speed if avg_speed > 0 else 0
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds)) if avg_speed > 0 else "Calculating..."
        
        progress_bar = "[{0}{1}]".format('█' * int(percentage / 5), ' ' * (20 - int(percentage / 5)))
        
        text = (
            f"<b>⚡ {escape_html(task)}</b>\n"
            f"{progress_bar} {percentage:.2f}%\n"
            f"<b>Transferred:</b> {humanbytes(status['seen'])} / {humanbytes(total_size)}\n"
            f"<b>Speed:</b> {humanbytes(avg_speed)}/s (Peak: {humanbytes(max(speed_samples)) if speed_samples else 0}/s)\n"
            f"<b>ETA:</b> {eta}"
        )
        try:
            await message.edit_text(text, parse_mode=ParseMode.HTML)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            try:
                plain_text = (
                    f"⚡ {task}\n"
                    f"{progress_bar} {percentage:.2f}%\n"
                    f"Transferred: {humanbytes(status['seen'])} / {humanbytes(total_size)}\n"
                    f"Speed: {humanbytes(avg_speed)}/s\n"
                    f"ETA: {eta}"
                )
                await message.edit_text(plain_text)
            except:
                pass
                
        last_seen = status['seen']
        last_update = current_time
        await asyncio.sleep(0.3)  # Reduced sleep time for more frequent updates

def pyrogram_progress_callback(current, total, message, start_time, task):
    """Progress callback for Pyrogram's synchronous operations."""
    try:
        current_time = time.time()
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or current_time - pyrogram_progress_callback.last_edit_time > 1:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            
            elapsed_time = current_time - start_time
            speed = current / elapsed_time if elapsed_time > 0 else 0
            
            text = f"<b>⚡ {escape_html(task)}</b> {percentage:.2f}% | {humanbytes(speed)}/s"
            try:
                message.edit_text(text, parse_mode=ParseMode.HTML)
            except:
                message.edit_text(f"⚡ {task} {percentage:.2f}% | {humanbytes(speed)}/s")
            pyrogram_progress_callback.last_edit_time = current_time
    except Exception:
        pass

async def download_file_with_retry(message, file_id, file_name, retries=5):
    """Download file with retry mechanism for better reliability"""
    for attempt in range(retries):
        try:
            file_path = await message.download(
                progress=pyrogram_progress_callback, 
                progress_args=(message, time.time(), f"Downloading (Attempt {attempt+1}/{retries})"),
                file_name=file_name
            )
            return file_path
        except (RPCError, FloodWait) as e:
            if attempt == retries - 1:
                raise e
            wait_time = e.value if hasattr(e, 'value') else 2
            await message.edit_text(f"⚠️ Download failed, retrying in {wait_time}s... (Attempt {attempt+1}/{retries})")
            await asyncio.sleep(wait_time)
    return None

async def upload_to_wasabi_with_retry(file_path, bucket, key, retries=5):
    """Upload file to Wasabi with retry mechanism"""
    for attempt in range(retries):
        try:
            status = {'running': True, 'seen': 0}
            
            def boto_callback(bytes_amount):
                status['seen'] += bytes_amount

            await asyncio.get_event_loop().run_in_executor(
                io_thread_pool,
                lambda: s3_client.upload_file(
                    file_path,
                    bucket,
                    key,
                    Callback=boto_callback,
                    Config=transfer_config
                )
            )
            status['running'] = False
            return True
        except Exception as e:
            if attempt == retries - 1:
                raise e
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return False

async def download_from_wasabi_with_retry(bucket, key, file_path, retries=5):
    """Download file from Wasabi with retry mechanism"""
    for attempt in range(retries):
        try:
            status = {'running': True, 'seen': 0}
            
            def boto_callback(bytes_amount):
                status['seen'] += bytes_amount

            await asyncio.get_event_loop().run_in_executor(
                io_thread_pool,
                lambda: s3_client.download_file(
                    bucket,
                    key,
                    file_path,
                    Callback=boto_callback,
                    Config=transfer_config
                )
            )
            status['running'] = False
            return True
        except Exception as e:
            if attempt == retries - 1:
                raise e
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return False

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handles the /start command."""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
        
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption="🚀 <b>ULTRA-SPEED Cloud Storage Bot</b>\n\n"
                "Maximum performance optimized for lightning-fast transfers!\n\n"
                "➡️ <b>To upload:</b> Just send me any file\n"
                "⬅️ <b>To download:</b> Use <code>/download &lt;file_name&gt;</code>\n"
                "📋 <b>To list files:</b> Use <code>/list</code>\n\n"
                "⚡ <b>Performance Features:</b>\n"
                "• Multi-threaded parallel processing\n"
                "• 10GB file size support\n"
                "• Adaptive retry system\n"
                "• Real-time speed monitoring\n"
                "• Connection pooling\n"
                "• Memory optimization\n\n"
                "<b>Owner:</b> Mraprguild\n"
                "<b>Telegram:</b> @Sathishkumar33",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using multipart transfers."""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    if hasattr(media, 'file_size') and media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
        return

    file_path = None
    status_message = await message.reply_text("🚀 Starting ultra-speed upload...", quote=True)

    try:
        file_name = sanitize_filename(
            media.file_name if hasattr(media, 'file_name') and media.file_name 
            else f"file_{int(time.time())}"
        )
        
        await status_message.edit_text("📥 Downloading from Telegram...")
        file_path = await download_file_with_retry(
            message, 
            media.file_id, 
            file_name,
            retries=5
        )
        
        if not file_path:
            await status_message.edit_text("❌ Failed to download file after multiple attempts.")
            return
            
        wasabi_file_name = f"{get_user_folder(message.from_user.id)}/{os.path.basename(file_path)}"
        file_size = os.path.getsize(file_path)
        
        status = {'running': True, 'seen': 0}
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, file_size, f"🚀 Uploading to Wasabi", time.time())
        )
        
        await upload_to_wasabi_with_retry(file_path, WASABI_BUCKET, wasabi_file_name, retries=5)
        
        status['running'] = False
        await asyncio.sleep(0.1)
        reporter_task.cancel()

        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_file_name}, 
            ExpiresIn=604800
        )
        
        safe_file_name = escape_html(os.path.basename(file_path))
        await status_message.edit_text(
            f"✅ <b>Upload Successful!</b>\n\n"
            f"<b>File:</b> <code>{safe_file_name}</code>\n"
            f"<b>Size:</b> {humanbytes(file_size)}\n"
            f"<b>Direct Link (7 days):</b>\n<code>{presigned_url}</code>\n\n"
            f"⚡ <i>Use /download {safe_file_name} to retrieve later</i>",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        error_msg = escape_html(str(e))
        await status_message.edit_text(f"❌ Upload failed: {error_msg}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using multipart transfers."""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage: <code>/download &lt;file_name&gt;</code>", parse_mode=ParseMode.HTML)
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    safe_file_name = escape_html(file_name)
    local_file_path = f"./downloads/{file_name}"
    os.makedirs("./downloads", exist_ok=True)
    
    status_message = await message.reply_text(f"🔍 Searching for <code>{safe_file_name}</code>...", quote=True, parse_mode=ParseMode.HTML)

    try:
        meta = await asyncio.get_event_loop().run_in_executor(
            io_thread_pool,
            lambda: s3_client.head_object(Bucket=WASABI_BUCKET, Key=user_file_name)
        )
        total_size = int(meta.get('ContentLength', 0))

        if total_size > MAX_FILE_SIZE:
            await status_message.edit_text(f"❌ File too large. Maximum size is {humanbytes(MAX_FILE_SIZE)}")
            return

        status = {'running': True, 'seen': 0}
        reporter_task = asyncio.create_task(
            progress_reporter(status_message, status, total_size, f"📥 Downloading from Wasabi", time.time())
        )
        
        await download_from_wasabi_with_retry(WASABI_BUCKET, user_file_name, local_file_path, retries=5)
        
        status['running'] = False
        await asyncio.sleep(0.1)
        reporter_task.cancel()
        
        await status_message.edit_text("📤 Uploading to Telegram...")
        await message.reply_document(
            document=local_file_path,
            caption=f"✅ <b>Download Complete:</b> <code>{safe_file_name}</code>\n"
                    f"<b>Size:</b> {humanbytes(total_size)}",
            parse_mode=ParseMode.HTML,
            progress=pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "🚀 Uploading to Telegram")
        )
        
        await status_message.delete()

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            await status_message.edit_text(f"❌ File not found: <code>{safe_file_name}</code>", parse_mode=ParseMode.HTML)
        elif error_code == '403':
            await status_message.edit_text("❌ Access denied. Check your Wasabi credentials.", parse_mode=ParseMode.HTML)
        else:
            error_msg = escape_html(str(e))
            await status_message.edit_text(f"❌ S3 Error: {error_code} - {error_msg}", parse_mode=ParseMode.HTML)
    except Exception as e:
        error_msg = escape_html(str(e))
        await status_message.edit_text(f"❌ Download failed: {error_msg}", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in the Wasabi bucket"""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return
        
    try:
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = await asyncio.get_event_loop().run_in_executor(
            io_thread_pool,
            lambda: s3_client.list_objects_v2(Bucket=WASABI_BUCKET, Prefix=user_prefix)
        )
        
        if 'Contents' not in response:
            await message.reply_text("No files found in your storage.")
            return
        
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        file_sizes = [humanbytes(obj['Size']) for obj in response['Contents']]
        
        files_list = ""
        for i, (file, size) in enumerate(zip(files[:15], file_sizes[:15])):
            safe_file = escape_html(file)
            files_list += f"• <code>{safe_file}</code> ({size})\n"
        
        if len(files) > 15:
            files_list += f"\n...and {len(files) - 15} more files"
        
        total_size = sum(obj['Size'] for obj in response['Contents'])
        await message.reply_text(
            f"<b>Your files ({len(files)}):</b>\n"
            f"<b>Total storage used:</b> {humanbytes(total_size)}\n\n"
            f"{files_list}",
            parse_mode=ParseMode.HTML
        )
    
    except Exception as e:
        error_msg = escape_html(str(e))
        await message.reply_text(f"❌ Error listing files: {error_msg}")

@app.on_message(filters.command("speedtest"))
async def speed_test(client, message: Message):
    """Perform a speed test"""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    test_file_size = 10 * 1024 * 1024  # 10MB test file
    status_message = await message.reply_text("🚀 Starting speed test...")
    
    try:
        # Create test file
        test_file = f"./speedtest_{int(time.time())}.bin"
        with open(test_file, 'wb') as f:
            f.write(os.urandom(test_file_size))
        
        # Upload test
        start_time = time.time()
        await upload_to_wasabi_with_retry(test_file, WASABI_BUCKET, f"speedtest/{os.path.basename(test_file)}", retries=2)
        upload_time = time.time() - start_time
        upload_speed = test_file_size / upload_time
        
        # Download test
        start_time = time.time()
        await download_from_wasabi_with_retry(WASABI_BUCKET, f"speedtest/{os.path.basename(test_file)}", test_file + ".download", retries=2)
        download_time = time.time() - start_time
        download_speed = test_file_size / download_time
        
        # Cleanup
        os.remove(test_file)
        if os.path.exists(test_file + ".download"):
            os.remove(test_file + ".download")
        
        await status_message.edit_text(
            f"📊 <b>Speed Test Results:</b>\n\n"
            f"<b>Upload Speed:</b> {humanbytes(upload_speed)}/s\n"
            f"<b>Download Speed:</b> {humanbytes(download_speed)}/s\n"
            f"<b>Test File Size:</b> {humanbytes(test_file_size)}\n\n"
            f"<i>Performance optimized for maximum throughput</i>",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        error_msg = escape_html(str(e))
        await status_message.edit_text(f"❌ Speed test failed: {error_msg}")

# --- Main Execution ---
if __name__ == "__main__":
    print(f"🚀 Starting ULTRA-SPEED Bot with {MAX_WORKERS} workers and {CPU_CORES} CPU cores...")
    
    # Start HTTP server in a separate thread for health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Start the Pyrogram bot with FloodWait handling
    max_retries = 5
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
    
    # Cleanup thread pools
    io_thread_pool.shutdown()
    cpu_thread_pool.shutdown()
    print("Bot has stopped.")