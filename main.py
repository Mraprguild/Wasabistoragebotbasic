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
from flask import Flask, render_template_string, jsonify, request
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
    multipart_threshold=32 * 1024 * 1024,   # Start multipart for files > 5MB
    max_concurrency=50,                    # Extreme parallel threads
    multipart_chunksize=32 * 1024 * 1024,  # Larger chunks for fewer requests
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

# --- Flask App for Health Checks and Dashboard ---
flask_app = Flask(__name__)

# HTML Templates
MAIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ULTRA TURBO Wasabi Storage Bot</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .header p {
            margin: 10px 0 0;
            font-size: 1.2em;
            opacity: 0.9;
        }
        .content {
            padding: 30px;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border-left: 4px solid #667eea;
        }
        .stat-card h3 {
            margin: 0 0 10px;
            color: #495057;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #343a40;
            margin: 0;
        }
        .performance-badge {
            display: inline-block;
            background: linear-gradient(135deg, #ffd93d 0%, #ff6b6b 100%);
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            margin: 10px 0;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.2);
        }
        .features {
            background: #e9ecef;
            padding: 25px;
            border-radius: 10px;
            margin: 20px 0;
        }
        .features h2 {
            color: #495057;
            margin-top: 0;
        }
        .feature-list {
            columns: 2;
            gap: 20px;
        }
        .feature-list li {
            margin-bottom: 10px;
            break-inside: avoid;
        }
        .footer {
            text-align: center;
            padding: 20px;
            background: #343a40;
            color: white;
            margin-top: 30px;
        }
        @media (max-width: 768px) {
            .feature-list {
                columns: 1;
            }
            .header h1 {
                font-size: 2em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚡ ULTRA TURBO CLOUD STORAGE</h1>
            <p>Extreme Performance Wasabi Storage Bot</p>
            <div class="performance-badge">ULTRA TURBO MODE ACTIVE</div>
        </div>
        
        <div class="content">
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>Active Users</h3>
                    <p class="stat-value">{{ stats.active_users }}</p>
                </div>
                <div class="stat-card">
                    <h3>Total Users</h3>
                    <p class="stat-value">{{ stats.user_limits_count }}</p>
                </div>
                <div class="stat-card">
                    <h3>Bucket</h3>
                    <p class="stat-value" style="font-size: 1.2em;">{{ stats.bucket }}</p>
                </div>
                <div class="stat-card">
                    <h3>Status</h3>
                    <p class="stat-value" style="color: #28a745;">{{ stats.status }}</p>
                </div>
            </div>
            
            <div class="features">
                <h2>🚀 Extreme Performance Features</h2>
                <ul class="feature-list">
                    <li><strong>50x Multi-threaded</strong> parallel processing</li>
                    <li><strong>10GB file size</strong> support</li>
                    <li>Adaptive retry system with <strong>10 attempts</strong></li>
                    <li>Real-time speed monitoring with smoothing</li>
                    <li><strong>100 connection pooling</strong> for maximum throughput</li>
                    <li>Memory optimization for large files</li>
                    <li>TCP Keepalive for stable connections</li>
                    <li>Ultra modern progress tracking</li>
                </ul>
            </div>
            
            <div style="text-align: center; margin: 30px 0;">
                <h3>📊 System Information</h3>
                <p><strong>Performance Mode:</strong> {{ stats.performance_mode }}</p>
                <p><strong>Uptime:</strong> {{ stats.timestamp | datetime }}</p>
                <p><strong>Region:</strong> {{ stats.region }}</p>
            </div>
        </div>
        
        <div class="footer">
            <p>Powered by Pyrogram & Wasabi | Built with ⚡ Ultra Turbo Technology</p>
            <p>💎 Owner: Mraprguild | 📧 Email: mraprguild@gmail.com</p>
        </div>
    </div>
</body>
</html>
"""

HEALTH_TEMPLATE = """
{
    "status": "{{ status }}",
    "timestamp": {{ timestamp }},
    "bucket": "{{ bucket }}",
    "performance_mode": "{{ performance_mode }}",
    "region": "{{ region }}"
}
"""

STATS_TEMPLATE = """
{
    "user_limits_count": {{ user_limits_count }},
    "active_users": {{ active_users }},
    "performance_mode": "{{ performance_mode }}",
    "region": "{{ region }}"
}
"""

@flask_app.route('/')
def index():
    """Main dashboard page"""
    stats = {
        'active_users': len([k for k, v in user_limits.items() if any(time.time() - t < 300 for t in v)]),
        'user_limits_count': len(user_limits),
        'bucket': WASABI_BUCKET,
        'status': 'healthy',
        'performance_mode': 'ULTRA_TURBO',
        'region': WASABI_REGION,
        'timestamp': time.time()
    }
    return render_template_string(MAIN_TEMPLATE, stats=stats)

@flask_app.route('/health')
def health():
    """Health check endpoint"""
    health_data = {
        "status": "healthy",
        "timestamp": time.time(),
        "bucket": WASABI_BUCKET,
        "performance_mode": "ULTRA_TURBO",
        "region": WASABI_REGION
    }
    return jsonify(health_data)

@flask_app.route('/stats')
def stats():
    """Statistics endpoint"""
    current_time = time.time()
    active_users = len([k for k, v in user_limits.items() if any(current_time - t < 300 for t in v)])
    
    stats_data = {
        "user_limits_count": len(user_limits),
        "active_users": active_users,
        "performance_mode": "ULTRA_TURBO",
        "region": WASABI_REGION
    }
    return jsonify(stats_data)

@flask_app.route('/api/users')
def api_users():
    """API endpoint for user statistics"""
    current_time = time.time()
    active_users = {user_id: len([t for t in times if current_time - t < 300]) 
                   for user_id, times in user_limits.items()}
    
    return jsonify({
        "total_users": len(user_limits),
        "active_users_count": len([k for k, v in active_users.items() if v > 0]),
        "user_activity": active_users,
        "performance_mode": "ULTRA_TURBO"
    })

def run_flask_server():
    """Run Flask server in a separate thread"""
    port = int(os.environ.get('PORT', 8080))
    # Disable Flask's development server logging
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    print(f"Flask server running on port {port}")
    flask_app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

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
        filled_char = "⚡"
        empty_char = "⚡"
    elif percentage < 50:
        filled_char = "🔥"
        empty_char = "⚡"
    elif percentage < 75:
        filled_char = "🚀"
        empty_char = "🔥"
    else:
        filled_char = "💯"
        empty_char = "🚀"
    
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
                f"<b>⚡ ULTRA TURBO MODE</b>\n\n"
                f"<b>📁 {display_task}</b>\n\n"
                f"{progress_bar}\n"
                f"<b>{percentage:.1f}%</b> • {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
                f"<b>🚀 Speed:</b> {humanbytes(avg_speed)}/s\n"
                f"<b>⏱️ ETA:</b> {eta}\n"
                f"<b>🕒 Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}\n"
                f"<b>🔧 Threads:</b> {transfer_config.max_concurrency}"
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
                        f"{percentage:.1f}% • {humanbytes(status['seen'])} / {humanbytes(total_size)}\n\n"
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
            bar = "🚀" * filled + "⚡" * (bar_length - filled)
            
            # Use HTML formatting
            escaped_task = escape_html(task)
            
            # Truncate long file names
            display_task = escaped_task
            if len(display_task) > 30:
                display_task = display_task[:27] + "..."
            
            elapsed_time = time.time() - start_time
            
            text = (
                f"<b>⬇️ ULTRA DOWNLOAD</b>\n"
                f"<b>📁 {display_task}</b>\n"
                f"{bar} <b>{percentage:.1f}%</b>\n"
                f"<b>⏱️ Elapsed:</b> {time.strftime('%M:%S', time.gmtime(elapsed_time))}"
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
        await message.reply_text("❌ Unauthorized access.")
        return
        
    # Send the welcome image with caption
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption="🚀 <b>ULTRA TURBO CLOUD STORAGE BOT</b>\n\n"
                "Experience extreme speed with our optimized parallel processing technology!\n\n"
                "➡️ <b>To upload:</b> Just send me any file (up to 10GB!)\n"
                "⬅️ <b>To download:</b> Use <code>/download &lt;file_name&gt;</code>\n"
                "📋 <b>To list files:</b> Use <code>/list</code>\n\n"
                "<b>⚡ Extreme Performance Features:</b>\n"
                "• 50x Multi-threaded parallel processing\n"
                "• 10GB file size support\n"
                "• Adaptive retry system with 10 attempts\n"
                "• Real-time speed monitoring with smoothing\n"
                "• 100 connection pooling for maximum throughput\n"
                "• Memory optimization for large files\n"
                "• TCP Keepalive for stable connections\n\n"
                "<b>💎 Owner:</b> Mraprguild\n"
                "<b>📧 Email:</b> mraprguild@gmail.com\n"
                "<b>📱 Telegram:</b> @Sathishkumar33",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("turbo"))
async def turbo_mode_command(client, message: Message):
    """Shows turbo mode status"""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
        
    await message.reply_text(
        f"⚡ <b>ULTRA TURBO MODE ACTIVE</b>\n\n"
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
    status_message = await message.reply_text("⚡ Initializing ULTRA TURBO mode...", quote=True)

    try:
        await status_message.edit_text("⬇️ Downloading from Telegram (Turbo Mode)...")
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
            f"✅ <b>ULTRA TURBO UPLOAD COMPLETE!</b>\n\n"
            f"<b>📁 File:</b> <code>{safe_file_name}</code>\n"
            f"<b>📦 Size:</b> {humanbytes(media.file_size)}\n"
            f"<b>🔗 Streamable Link (24h expiry):</b>\n<code>{safe_url}</code>\n\n"
            f"<b>⚡ Performance:</b> Ultra Turbo Mode",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        await status_message.edit_text(f"❌ An error occurred: {escape_html(str(e))}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using extreme multipart transfers."""
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
        
        await status_message.edit_text("📤 Uploading to Telegram (Turbo Mode)...")
        await message.reply_document(
            document=local_file_path,
            caption=f"✅ <b>ULTRA TURBO DOWNLOAD COMPLETE!</b>\n"
                    f"<b>File:</b> <code>{safe_file_name}</code>\n"
                    f"<b>Size:</b> {humanbytes(total_size)}\n"
                    f"<b>Mode:</b> ⚡ Ultra Turbo",
            parse_mode=ParseMode.HTML,
            progress=ultra_pyrogram_progress_callback,
            progress_args=(status_message, time.time(), "Uploading to Telegram")
        )
        
        await status_message.delete()

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            await status_message.edit_text(f"❌ <b>Error:</b> File not found in Wasabi: <code>{safe_file_name}</code>", parse_mode=ParseMode.HTML)
        elif error_code == '403':
            await status_message.edit_text("❌ <b>Error:</b> Access denied. Check your Wasabi credentials.", parse_mode=ParseMode.HTML)
        elif error_code == 'NoSuchBucket':
            await status_message.edit_text("❌ <b>Error:</b> Bucket does not exist.", parse_mode=ParseMode.HTML)
        else:
            error_msg = escape_html(str(e))
            await status_message.edit_text(f"❌ <b>S3 Error:</b> {error_code} - {error_msg}", parse_mode=ParseMode.HTML)
    except Exception as e:
        error_msg = escape_html(str(e))
        await status_message.edit_text(f"❌ <b>An unexpected error occurred:</b> {error_msg}", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in the Wasabi bucket"""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    # Check rate limiting
    if not await check_rate_limit(message.from_user.id):
        await message.reply_text("❌ Rate limit exceeded. Please try again in a minute.")
        return
        
    try:
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=WASABI_BUCKET, Prefix=user_prefix)
        
        if 'Contents' not in response:
            await message.reply_text("📂 No files found in your storage.")
            return
        
        # Remove the user prefix from displayed filenames
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        safe_files = [escape_html(file) for file in files[:20]]  # Show first 20 files
        files_list = "\n".join([f"• <code>{file}</code>" for file in safe_files])
        
        if len(files) > 20:
            files_list += f"\n\n...and {len(files) - 20} more files"
        
        await message.reply_text(f"📁 <b>Your files:</b>\n\n{files_list}", parse_mode=ParseMode.HTML)
    
    except Exception as e:
        error_msg = escape_html(str(e))
        await message.reply_text(f"❌ Error listing files: {error_msg}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting ULTRA TURBO Wasabi Storage Bot with extreme performance settings...")
    
    # Start Flask server in a separate thread for health checks and dashboard
    flask_thread = threading.Thread(target=run_flask_server, daemon=True)
    flask_thread.start()
    
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
