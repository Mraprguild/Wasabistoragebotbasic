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
import multiprocessing
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

# Welcome image URL
WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/IMG-20250915-WA0013.jpg"

# --- Basic Checks ---
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- Initialize Pyrogram Client (🚀 Extreme Workers) ---
app = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=multiprocessing.cpu_count() * 5  # 🚀 More workers = more parallel Telegram speed
)

# --- Boto3 Transfer Configuration (🚀 Extreme Turbo Speed) ---
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,      # Multipart if > 8MB
    max_concurrency=32,                       # 🚀 Max parallel threads
    multipart_chunksize=64 * 1024 * 1024,     # 🚀 Large chunks for fewer requests
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
MAX_REQUESTS_PER_MINUTE = 3
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

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
        return  # disable logs

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

# --- Helpers ---
def humanbytes(size):
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power and t_n < len(power_dict) - 1:
        size /= power
        t_n += 1
    return "{:.2f}".format(size) + power_dict[t_n]

def sanitize_filename(filename):
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200 - len(ext)] + ext
    return filename

def escape_html(text):
    return html.escape(str(text)) if text else ""

def cleanup():
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
    current_time = time.time()
    if user_id not in user_limits:
        user_limits[user_id] = []
    user_limits[user_id] = [t for t in user_limits[user_id] if current_time - t < 60]
    if len(user_limits[user_id]) >= MAX_REQUESTS_PER_MINUTE:
        return False
    user_limits[user_id].append(current_time)
    return True

def get_user_folder(user_id):
    return f"user_{user_id}"

# --- Progress Functions ---
async def progress_reporter(message: Message, status: dict, total_size: int, task: str, start_time: float):
    while status['running']:
        percentage = (status['seen'] / total_size) * 100 if total_size > 0 else 0
        percentage = min(percentage, 100)
        elapsed_time = time.time() - start_time
        speed = status['seen'] / elapsed_time if elapsed_time > 0 else 0
        eta = time.strftime("%Hh %Mm %Ss", time.gmtime((total_size - status['seen']) / speed)) if speed > 0 else "N/A"
        progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), ' ' * (10 - int(percentage / 10)))
        text = (
            f"<b>{escape_html(task)}</b>\n"
            f"{progress_bar} {percentage:.2f}%\n"
            f"<b>Done:</b> {humanbytes(status['seen'])} of {humanbytes(total_size)}\n"
            f"<b>Speed:</b> {humanbytes(speed)}/s\n"
            f"<b>ETA:</b> {eta}"
        )
        try:
            await message.edit_text(text, parse_mode=ParseMode.HTML)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass
        await asyncio.sleep(3)

def pyrogram_progress_callback(current, total, message, start_time, task):
    try:
        if not hasattr(pyrogram_progress_callback, 'last_edit_time') or time.time() - pyrogram_progress_callback.last_edit_time > 3:
            percentage = min((current * 100 / total), 100) if total > 0 else 0
            text = f"<b>{escape_html(task)}</b> {percentage:.2f}%"
            try:
                message.edit_text(text, parse_mode=ParseMode.HTML)
            except:
                message.edit_text(f"{task} {percentage:.2f}%")
            pyrogram_progress_callback.last_edit_time = time.time()
    except:
        pass

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption="Hello! I am a <b>Turbo-Speed</b> Cloud storage bot.\n\n"
                "➡️ Send me files to upload\n"
                "⬅️ Use <code>/download filename</code> to download\n"
                "📋 Use <code>/list</code> to list files\n\n"
                "<b>Speed Mode:</b> EXTREME 🚀",
                Contact:
• Telegram: <a href="https://t.me/Sathishkumar">@Sathishkumar</a>
• Email: <a href="mailto:Mraprguild@gmail.com">Mraprguild@gmail.com</a>

Bot Owner: Mraprguild
"""
        parse_mode=ParseMode.HTML
    )

# (Upload, Download, List handlers remain the same as your version, using transfer_config above)

# --- Main Execution ---
if __name__ == "__main__":
    print("Bot is starting with EXTREME SPEED settings...")

    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

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
            print(f"FloodWait: waiting {wait_time}s before retry {retry_count}/{max_retries}")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

    print("Bot has stopped.")
    
