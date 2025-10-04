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

# Status rotation configuration
STATUS_ROTATION = [
    "⚡ ULTRA TURBO MODE • 10GB Support",
    "🚀 Extreme Parallel Processing • 50 Threads",
    "💨 Turbo Uploads & Downloads Active",
    "🔧 100 Connection Pool • Maximum Throughput",
    "📊 Monitoring Performance • Ultra Mode",
    "🌟 Wasabi Storage • Extreme Speed",
    "🔥 50x Multi-threaded • Ultra Fast",
    "💎 Mraprguild • Professional Storage",
    "📁 10GB Files • Lightning Transfers",
    "⚙️ Adaptive Retry • Maximum Reliability"
]

STATUS_CHANGE_INTERVAL = 30  # seconds

# --- Global Bot Status Variables ---
BOT_START_TIME = time.time()
BOT_STATUS = "initializing"
CURRENT_STATUS_INDEX = 0
TOTAL_UPLOADS = 0
TOTAL_DOWNLOADS = 0
ACTIVE_TRANSFERS = 0
LAST_ERROR = None

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

# --- Status Rotation Task ---
async def rotate_status():
    """Rotate bot status periodically"""
    global CURRENT_STATUS_INDEX, BOT_STATUS
    
    index = 0
    while True:
        try:
            status = STATUS_ROTATION[index]
            await app.update_profile(bio=status)
            CURRENT_STATUS_INDEX = index
            BOT_STATUS = "running"
            print(f"🔄 Status updated: {status}")
            
            index = (index + 1) % len(STATUS_ROTATION)
            await asyncio.sleep(STATUS_CHANGE_INTERVAL)
        except Exception as e:
            print(f"❌ Status update error: {e}")
            BOT_STATUS = f"error: {str(e)}"
            await asyncio.sleep(STATUS_CHANGE_INTERVAL)

# --- Authorization Check ---
async def is_authorized(user_id):
    return not AUTHORIZED_USERS or user_id in AUTHORIZED_USERS

# --- Enhanced HTTP Server for Bot Status ---
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global BOT_STATUS, TOTAL_UPLOADS, TOTAL_DOWNLOADS, ACTIVE_TRANSFERS, LAST_ERROR
        
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            uptime = time.time() - BOT_START_TIME
            current_status = STATUS_ROTATION[CURRENT_STATUS_INDEX] if STATUS_ROTATION else "Unknown"
            
            response = {
                "status": "healthy",
                "bot_status": BOT_STATUS,
                "current_bio": current_status,
                "timestamp": time.time(),
                "uptime_seconds": uptime,
                "uptime_human": self.format_uptime(uptime),
                "bucket": WASABI_BUCKET,
                "performance_mode": "ULTRA_TURBO",
                "total_uploads": TOTAL_UPLOADS,
                "total_downloads": TOTAL_DOWNLOADS,
                "active_transfers": ACTIVE_TRANSFERS,
                "last_error": LAST_ERROR
            }
            self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))
            
        elif self.path == '/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            current_time = time.time()
            active_users = len([k for k, v in user_limits.items() if any(current_time - t < 300 for t in v)])
            uptime = time.time() - BOT_START_TIME
            
            response = {
                "user_limits_count": len(user_limits),
                "active_users": active_users,
                "performance_mode": "ULTRA_TURBO",
                "status_rotation_count": len(STATUS_ROTATION),
                "max_file_size": MAX_FILE_SIZE,
                "bot_uptime": self.format_uptime(uptime),
                "total_operations": TOTAL_UPLOADS + TOTAL_DOWNLOADS,
                "upload_download_ratio": f"{TOTAL_UPLOADS}:{TOTAL_DOWNLOADS}",
                "rate_limit": f"{MAX_REQUESTS_PER_MINUTE}/minute"
            }
            self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))
            
        elif self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            uptime = time.time() - BOT_START_TIME
            current_status = STATUS_ROTATION[CURRENT_STATUS_INDEX] if STATUS_ROTATION else "Unknown"
            
            html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Ultra Turbo Wasabi Bot Status</title>
                <style>
                    body {{
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        margin: 0;
                        padding: 20px;
                        min-height: 100vh;
                    }}
                    .container {{
                        max-width: 1200px;
                        margin: 0 auto;
                        background: white;
                        border-radius: 15px;
                        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                        overflow: hidden;
                    }}
                    .header {{
                        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
                        color: white;
                        padding: 30px;
                        text-align: center;
                    }}
                    .header h1 {{
                        margin: 0;
                        font-size: 2.5em;
                        font-weight: 300;
                    }}
                    .header .subtitle {{
                        font-size: 1.2em;
                        opacity: 0.9;
                        margin-top: 10px;
                    }}
                    .stats-grid {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                        gap: 20px;
                        padding: 30px;
                    }}
                    .stat-card {{
                        background: #f8f9fa;
                        border-radius: 10px;
                        padding: 20px;
                        text-align: center;
                        border-left: 4px solid #667eea;
                    }}
                    .stat-card.primary {{
                        border-left-color: #ff6b6b;
                    }}
                    .stat-card.success {{
                        border-left-color: #51cf66;
                    }}
                    .stat-card.warning {{
                        border-left-color: #ff922b;
                    }}
                    .stat-number {{
                        font-size: 2em;
                        font-weight: bold;
                        color: #495057;
                        margin: 10px 0;
                    }}
                    .stat-label {{
                        color: #6c757d;
                        font-size: 0.9em;
                        text-transform: uppercase;
                        letter-spacing: 1px;
                    }}
                    .status-section {{
                        padding: 30px;
                        background: #f1f3f5;
                        border-top: 1px solid #dee2e6;
                    }}
                    .status-item {{
                        display: flex;
                        justify-content: between;
                        align-items: center;
                        padding: 15px;
                        background: white;
                        margin: 10px 0;
                        border-radius: 8px;
                        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    }}
                    .status-label {{
                        font-weight: 600;
                        color: #495057;
                        min-width: 200px;
                    }}
                    .status-value {{
                        color: #6c757d;
                    }}
                    .status-badge {{
                        padding: 5px 15px;
                        border-radius: 20px;
                        font-size: 0.8em;
                        font-weight: bold;
                    }}
                    .status-running {{
                        background: #d3f9d8;
                        color: #2b8a3e;
                    }}
                    .status-error {{
                        background: #ffe3e3;
                        color: #c92a2a;
                    }}
                    .footer {{
                        text-align: center;
                        padding: 20px;
                        background: #495057;
                        color: white;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>⚡ ULTRA TURBO WASABI BOT</h1>
                        <div class="subtitle">Real-time Bot Status & Performance Metrics</div>
                    </div>
                    
                    <div class="stats-grid">
                        <div class="stat-card primary">
                            <div class="stat-label">Bot Status</div>
                            <div class="stat-number">{BOT_STATUS.upper()}</div>
                            <div class="status-badge status-running">ACTIVE</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Uptime</div>
                            <div class="stat-number">{self.format_uptime(uptime)}</div>
                            <div>Since {time.ctime(BOT_START_TIME)}</div>
                        </div>
                        <div class="stat-card success">
                            <div class="stat-label">Total Uploads</div>
                            <div class="stat-number">{TOTAL_UPLOADS}</div>
                            <div>Files to Wasabi</div>
                        </div>
                        <div class="stat-card warning">
                            <div class="stat-label">Total Downloads</div>
                            <div class="stat-number">{TOTAL_DOWNLOADS}</div>
                            <div>Files from Wasabi</div>
                        </div>
                    </div>
                    
                    <div class="status-section">
                        <h2>📊 Detailed Status Information</h2>
                        
                        <div class="status-item">
                            <span class="status-label">Current Bio Status</span>
                            <span class="status-value">{current_status}</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Performance Mode</span>
                            <span class="status-value">⚡ ULTRA TURBO</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Wasabi Bucket</span>
                            <span class="status-value">{WASABI_BUCKET}</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Active Transfers</span>
                            <span class="stat-number">{ACTIVE_TRANSFERS}</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Max File Size</span>
                            <span class="status-value">{self.humanbytes(MAX_FILE_SIZE)}</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Rate Limit</span>
                            <span class="status-value">{MAX_REQUESTS_PER_MINUTE} requests/minute</span>
                        </div>
                        
                        <div class="status-item">
                            <span class="status-label">Last Error</span>
                            <span class="status-value">{LAST_ERROR or 'No errors'}</span>
                        </div>
                    </div>
                    
                    <div class="footer">
                        <p>Ultra Turbo Wasabi Storage Bot • Powered by Pyrogram & Wasabi S3</p>
                        <p>Owner: Mraprguild • Email: mraprguild@gmail.com</p>
                    </div>
                </div>
                
                <script>
                    // Auto-refresh every 10 seconds
                    setTimeout(() => {{
                        location.reload();
                    }}, 10000);
                </script>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
            
        elif self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            html_content = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Ultra Turbo Wasabi Bot</title>
                <style>
                    body {
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        margin: 0;
                        padding: 0;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        min-height: 100vh;
                    }
                    .container {
                        text-align: center;
                        background: white;
                        padding: 50px;
                        border-radius: 20px;
                        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    }
                    h1 {
                        color: #333;
                        margin-bottom: 20px;
                    }
                    .links {
                        margin-top: 30px;
                    }
                    .link {
                        display: inline-block;
                        margin: 10px;
                        padding: 12px 24px;
                        background: #667eea;
                        color: white;
                        text-decoration: none;
                        border-radius: 25px;
                        transition: all 0.3s ease;
                    }
                    .link:hover {
                        background: #764ba2;
                        transform: translateY(-2px);
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>⚡ Ultra Turbo Wasabi Storage Bot</h1>
                    <p>Real-time monitoring and status dashboard</p>
                    <div class="links">
                        <a href="/status" class="link">📊 Live Status</a>
                        <a href="/health" class="link">❤️ Health Check</a>
                        <a href="/stats" class="link">📈 Statistics</a>
                    </div>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"error": "Endpoint not found", "available_endpoints": ["/", "/status", "/health", "/stats"]}
            self.wfile.write(json.dumps(response).encode('utf-8'))

    def format_uptime(self, seconds):
        """Format uptime in human readable format"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m {secs}s"
        elif hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    def humanbytes(self, size):
        """Converts bytes to human readable format"""
        if not size:
            return "0 B"
        power = 1024
        t_n = 0
        power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
        while size >= power and t_n < len(power_dict) -1:
            size /= power
            t_n += 1
        return "{:.2f} {}".format(size, power_dict[t_n])

    def log_message(self, format, *args):
        # Disable logging to prevent conflicts with Pyrogram
        return

def run_http_server():
    # Use the PORT environment variable if available (common in cloud platforms)
    port = int(os.environ.get('PORT', 8080))
    
    # Create a simple HTTP server without signal handling
    with HTTPServer(('0.0.0.0', port), HealthHandler) as httpd:
        print(f"🌐 HTTP server running on port {port}")
        print(f"📊 Status dashboard available at: http://localhost:{port}/status")
        print(f"❤️ Health check available at: http://localhost:{port}/health")
        print(f"📈 Statistics available at: http://localhost:{port}/stats")
        
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
    global ACTIVE_TRANSFERS
    
    last_update = 0
    speed_samples = []
    
    ACTIVE_TRANSFERS += 1
    
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
    
    ACTIVE_TRANSFERS -= 1

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
                "📋 <b>To list files:</b> Use <code>/list</code>\n"
                "📊 <b>Bot Status:</b> Visit web dashboard for real-time monitoring\n\n"
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
        f"<b>Connection Pool:</b> {boto_config.max_pool_connections} connections\n"
        f"<b>Status Rotation:</b> {len(STATUS_ROTATION)} messages every {STATUS_CHANGE_INTERVAL}s\n"
        f"<b>Total Operations:</b> {TOTAL_UPLOADS + TOTAL_DOWNLOADS}",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("status"))
async def status_command(client, message: Message):
    """Shows current bot status"""
    # Check authorization
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
        
    try:
        current_bio = await app.get_chat("me")
        bio_text = current_bio.bio if current_bio.bio else "No status set"
        uptime = time.time() - BOT_START_TIME
        
        # Format uptime
        days = int(uptime // 86400)
        hours = int((uptime % 86400) // 3600)
        minutes = int((uptime % 3600) // 60)
        
        uptime_str = f"{days}d {hours}h {minutes}m" if days > 0 else f"{hours}h {minutes}m"
        
        await message.reply_text(
            f"🤖 <b>Bot Status Information</b>\n\n"
            f"<b>Current Status:</b> {bio_text}\n"
            f"<b>Bot Uptime:</b> {uptime_str}\n"
            f"<b>Status Rotation:</b> {len(STATUS_ROTATION)} messages\n"
            f"<b>Change Interval:</b> Every {STATUS_CHANGE_INTERVAL} seconds\n"
            f"<b>Active Users:</b> {len(user_limits)}\n"
            f"<b>Total Uploads:</b> {TOTAL_UPLOADS}\n"
            f"<b>Total Downloads:</b> {TOTAL_DOWNLOADS}\n"
            f"<b>Active Transfers:</b> {ACTIVE_TRANSFERS}\n"
            f"<b>Performance Mode:</b> ⚡ ULTRA TURBO\n\n"
            f"<b>🌐 Web Dashboard:</b> Available at /status endpoint",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.reply_text(f"❌ Error getting status: {escape_html(str(e))}")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    """Handles file uploads to Wasabi using extreme multipart transfers."""
    global TOTAL_UPLOADS, LAST_ERROR
    
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
        
        TOTAL_UPLOADS += 1
        LAST_ERROR = None
        
        await status_message.edit_text(
            f"✅ <b>ULTRA TURBO UPLOAD COMPLETE!</b>\n\n"
            f"<b>📁 File:</b> <code>{safe_file_name}</code>\n"
            f"<b>📦 Size:</b> {humanbytes(media.file_size)}\n"
            f"<b>🔗 Streamable Link (24h expiry):</b>\n<code>{safe_url}</code>\n\n"
            f"<b>⚡ Performance:</b> Ultra Turbo Mode",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        error_msg = str(e)
        LAST_ERROR = error_msg
        await status_message.edit_text(f"❌ An error occurred: {escape_html(error_msg)}")

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    """Handles file downloads from Wasabi using extreme multipart transfers."""
    global TOTAL_DOWNLOADS, LAST_ERROR
    
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
        
        TOTAL_DOWNLOADS += 1
        LAST_ERROR = None
        await status_message.delete()

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = f"{error_code}: {str(e)}"
        LAST_ERROR = error_msg
        if error_code == '404':
            await status_message.edit_text(f"❌ <b>Error:</b> File not found in Wasabi: <code>{safe_file_name}</code>", parse_mode=ParseMode.HTML)
        elif error_code == '403':
            await status_message.edit_text("❌ <b>Error:</b> Access denied. Check your Wasabi credentials.", parse_mode=ParseMode.HTML)
        elif error_code == 'NoSuchBucket':
            await status_message.edit_text("❌ <b>Error:</b> Bucket does not exist.", parse_mode=ParseMode.HTML)
        else:
            await status_message.edit_text(f"❌ <b>S3 Error:</b> {error_code} - {escape_html(str(e))}", parse_mode=ParseMode.HTML)
    except Exception as e:
        error_msg = str(e)
        LAST_ERROR = error_msg
        await status_message.edit_text(f"❌ <b>An unexpected error occurred:</b> {escape_html(error_msg)}", parse_mode=ParseMode.HTML)
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
    print("🚀 Starting ULTRA TURBO Wasabi Storage Bot with extreme performance settings...")
    print(f"📊 Status rotation: {len(STATUS_ROTATION)} messages every {STATUS_CHANGE_INTERVAL} seconds")
    
    # Start HTTP server in a separate thread for health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Start the Pyrogram bot with FloodWait handling
    max_retries = 5  # Increased retries for reliability
    retry_count = 0
    
    async def main():
        # Start status rotation task
        status_task = asyncio.create_task(rotate_status())
        
        # Start the bot
        await app.start()
        BOT_STATUS = "running"
        print("✅ Bot started successfully!")
        print("🌐 Web dashboard is now available!")
        
        # Keep the bot running
        await asyncio.Event().wait()
    
    while retry_count < max_retries:
        try:
            print("⚡ Starting bot in ULTRA TURBO mode...")
            asyncio.run(main())
            break
        except FloodWait as e:
            retry_count += 1
            wait_time = e.value + 5
            print(f"⏳ Telegram flood wait error: Need to wait {e.value} seconds")
            print(f"🔄 Waiting {wait_time} seconds before retry {retry_count}/{max_retries}...")
            time.sleep(wait_time)
        except Exception as e:
            BOT_STATUS = f"error: {str(e)}"
            print(f"❌ Unexpected error: {e}")
            break
    
    BOT_STATUS = "stopped"
    print("🛑 Bot has stopped.")