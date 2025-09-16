import os
import time
import math
import boto3
import asyncio
import re
import atexit
import threading
import json
import html
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from botocore.exceptions import ClientError
from pyrogram.errors import FloodWait
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from urllib.parse import quote

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

WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/Mraprguild8133/Telegramstorage-/refs/heads/main/IMG-20250915-WA0013.jpg"

if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    print("Missing one or more required environment variables. Please check your .env file.")
    exit()

# --- Initialize Pyrogram Client ---
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

# --- Extreme Boto3 Config ---
boto_config = BotoConfig(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    max_pool_connections=100,
    connect_timeout=30,
    read_timeout=60,
    tcp_keepalive=True
)

transfer_config = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,
    max_concurrency=50,
    multipart_chunksize=50 * 1024 * 1024,
    num_download_attempts=10,
    use_threads=True
)

wasabi_endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint_url,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=boto_config
)

user_limits = {}
MAX_REQUESTS_PER_MINUTE = 30
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024

user_file_cache = {}

# --- Helpers ---
def sanitize_filename(filename):
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    return filename

def escape_html(text):
    return html.escape(str(text)) if text else ""

def get_user_folder(user_id):
    return f"user_{user_id}"

def humanbytes(size):
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power and t_n < len(power_dict) - 1:
        size /= power
        t_n += 1
    return "{:.2f} {}".format(size, power_dict[t_n])

async def is_authorized(user_id):
    return not AUTHORIZED_USERS or user_id in AUTHORIZED_USERS

# --- Fix: Find closest matching file ---
def find_closest_filename(user_id, requested_name):
    prefix = get_user_folder(user_id) + "/"
    try:
        response = s3_client.list_objects_v2(Bucket=WASABI_BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            return None
        requested_sanitized = sanitize_filename(requested_name)
        for obj in response['Contents']:
            actual_name = obj['Key'].replace(prefix, "")
            if actual_name == requested_name or actual_name == requested_sanitized:
                return actual_name
        for obj in response['Contents']:
            actual_name = obj['Key'].replace(prefix, "")
            if actual_name.lower() == requested_name.lower():
                return actual_name
    except Exception:
        return None
    return None

# --- Core Bot Handlers (patched with filename correction) ---
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    await message.reply_photo(
        photo=WELCOME_IMAGE_URL,
        caption=("🚀 <b>ULTRA TURBO CLOUD STORAGE BOT</b>\n\n"
                 "➡️ Send me files (up to 10GB)\n"
                 "⬅️ Use /download <file> or /play <file>\n"
                 "📋 Use /list to see files"),
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    try:
        prefix = get_user_folder(message.from_user.id) + "/"
        response = s3_client.list_objects_v2(Bucket=WASABI_BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            await message.reply_text("📂 No files found in your storage.")
            return
        files = [obj['Key'].replace(prefix, "") for obj in response['Contents']]
        safe_files = [escape_html(file) for file in files[:20]]
        files_list = "\n".join([f"• <code>{file}</code>" for file in safe_files])
        if len(files) > 20:
            files_list += f"\n\n...and {len(files) - 20} more files"
        await message.reply_text(f"📁 <b>Your files:</b>\n\n{files_list}", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {escape_html(str(e))}")

@app.on_message(filters.command("play"))
async def play_handler(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    if len(message.command) < 2:
        await message.reply_text("Usage: <code>/play &lt;file_name&gt;</code>", parse_mode=ParseMode.HTML)
        return
    requested_name = " ".join(message.command[1:])
    actual_name = find_closest_filename(message.from_user.id, requested_name)
    if not actual_name:
        await message.reply_text(f"❌ File not found: <code>{escape_html(requested_name)}</code>", parse_mode=ParseMode.HTML)
        return
    key = f"{get_user_folder(message.from_user.id)}/{actual_name}"
    try:
        meta = await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=key)
        size = int(meta.get('ContentLength', 0))
        url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': key}, ExpiresIn=86400)
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬇️ Download", url=url)]])
        await message.reply_text(
            f"🎬 <b>ONLINE PLAYER READY</b>\n\n"
            f"<b>📁 File:</b> <code>{escape_html(actual_name)}</code>\n"
            f"<b>📦 Size:</b> {humanbytes(size)}\n"
            f"<b>⏰ Expires:</b> 24 hours",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    except Exception as e:
        await message.reply_text(f"❌ Error: {escape_html(str(e))}")

@app.on_message(filters.command("download"))
async def download_handler(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    if len(message.command) < 2:
        await message.reply_text("Usage: <code>/download &lt;file_name&gt;</code>", parse_mode=ParseMode.HTML)
        return
    requested_name = " ".join(message.command[1:])
    actual_name = find_closest_filename(message.from_user.id, requested_name)
    if not actual_name:
        await message.reply_text(f"❌ File not found: <code>{escape_html(requested_name)}</code>", parse_mode=ParseMode.HTML)
        return
    key = f"{get_user_folder(message.from_user.id)}/{actual_name}"
    local_path = f"./downloads/{actual_name}"
    os.makedirs("./downloads", exist_ok=True)
    try:
        await asyncio.to_thread(s3_client.download_file, WASABI_BUCKET, key, local_path)
        await message.reply_document(document=local_path, caption=f"✅ Downloaded <code>{escape_html(actual_name)}</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {escape_html(str(e))}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# --- Main ---
if __name__ == "__main__":
    print("Starting FULL ULTRA TURBO Wasabi Storage Bot with filename correction...")
    app.run()