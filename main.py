import os
import time
import math
import asyncio
import logging
import base64
import threading
from functools import wraps

import boto3
from botocore.exceptions import ClientError
from pyrogram import Client, filters
from pyrogram.types import Message
from flask import Flask, render_template, jsonify

# Import configuration
from config import config

# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use configuration from config module
API_ID = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = config.BOT_TOKEN
WASABI_ACCESS_KEY = config.WASABI_ACCESS_KEY
WASABI_SECRET_KEY = config.WASABI_SECRET_KEY
WASABI_BUCKET = config.WASABI_BUCKET
WASABI_REGION = config.WASABI_REGION
ADMIN_ID = config.ADMIN_ID
FLASK_HOST = getattr(config, 'FLASK_HOST', '0.0.0.0')
FLASK_PORT = getattr(config, 'FLASK_PORT', 8000)

# In-memory storage for authorized user IDs. Starts with the admin.
# For persistence, consider using a database or a file.
ALLOWED_USERS = {ADMIN_ID}

# --- Bot & Wasabi Client Initialization ---
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Boto3 S3 client for Wasabi
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=WASABI_REGION,
        config=boto3.session.Config(
            s3={'addressing_style': 'virtual'},
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
    )
    # Test connection
    s3_client.head_bucket(Bucket=WASABI_BUCKET)
    logger.info("Successfully connected to Wasabi.")
except Exception as e:
    logger.error(f"Failed to connect to Wasabi: {e}")
    s3_client = None

# --- Flask app for player.html ---
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Wasabi Bot Player</title>
        <style>
            body {
                margin: 0;
                padding: 40px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                font-family: Arial, sans-serif;
                text-align: center;
            }
            .container {
                max-width: 600px;
                margin: 0 auto;
                background: rgba(255,255,255,0.1);
                padding: 40px;
                border-radius: 15px;
                backdrop-filter: blur(10px);
            }
            h1 {
                margin-bottom: 20px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎮 Wasabi Bot Media Player</h1>
            <p>Use the Telegram bot to upload files and get player links.</p>
            <p>This server is running and ready to serve media content.</p>
        </div>
    </body>
    </html>
    """

@flask_app.route("/player/<media_type>/<encoded_url>")
def player(media_type, encoded_url):
    # Decode the URL
    try:
        # Add padding if needed
        padding = 4 - (len(encoded_url) % 4)
        if padding != 4:
            encoded_url += '=' * padding
        media_url = base64.urlsafe_b64decode(encoded_url).decode()
        
        # HTML template for media player
        player_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Media Player - {media_type}</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body {{
                    margin: 0;
                    padding: 20px;
                    background: #1a1a1a;
                    color: white;
                    font-family: Arial, sans-serif;
                    min-height: 100vh;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                }}
                .container {{
                    max-width: 900px;
                    width: 100%;
                    margin: 0 auto;
                    text-align: center;
                }}
                .header {{
                    margin-bottom: 30px;
                }}
                video {{
                    width: 100%;
                    max-width: 800px;
                    height: auto;
                    border-radius: 10px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.5);
                }}
                audio {{
                    width: 100%;
                    max-width: 600px;
                    margin: 20px 0;
                }}
                .download-btn {{
                    display: inline-block;
                    padding: 12px 30px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    text-decoration: none;
                    border-radius: 25px;
                    margin: 20px 0;
                    font-weight: bold;
                    transition: transform 0.3s ease;
                }}
                .download-btn:hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 5px 15px rgba(0,0,0,0.3);
                }}
                .info {{
                    background: rgba(255,255,255,0.1);
                    padding: 15px;
                    border-radius: 10px;
                    margin: 20px 0;
                    word-break: break-all;
                }}
                @media (max-width: 768px) {{
                    body {{
                        padding: 10px;
                    }}
                    .container {{
                        max-width: 100%;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎵 Media Player</h1>
                    <p>Streaming your {media_type} file</p>
                </div>
                
        """
        
        if media_type == 'video':
            player_html += f"""
                <video controls controlsList="nodownload">
                    <source src="{media_url}" type="video/mp4">
                    Your browser does not support the video tag.
                </video>
            """
        elif media_type == 'audio':
            player_html += f"""
                <audio controls controlsList="nodownload">
                    <source src="{media_url}" type="audio/mpeg">
                    Your browser does not support the audio tag.
                </audio>
            """
        else:
            player_html += f"""
                <div style="padding: 40px; background: rgba(255,0,0,0.1); border-radius: 10px;">
                    <h3>⚠️ File Type Not Supported for Playback</h3>
                    <p>This file type cannot be played in the browser. Please download it instead.</p>
                </div>
            """
        
        player_html += f"""
                <div class="info">
                    <strong>File URL:</strong><br>
                    <small>{media_url}</small>
                </div>
                
                <a href="{media_url}" class="download-btn" download target="_blank">
                    📥 Download File
                </a>
                
                <div style="margin-top: 30px; padding: 15px; background: rgba(255,255,255,0.05); border-radius: 10px;">
                    <p><small>Link expires in 7 days • Powered by Wasabi Bot</small></p>
                </div>
            </div>
            
            <script>
                // Auto-focus the media element for better UX
                document.addEventListener('DOMContentLoaded', function() {{
                    const media = document.querySelector('video, audio');
                    if (media) {{
                        media.focus();
                    }}
                }});
            </script>
        </body>
        </html>
        """
        
        return player_html
    except Exception as e:
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>Error</title></head>
        <body style="background: #1a1a1a; color: white; text-align: center; padding: 50px;">
            <h1>❌ Error</h1>
            <p>Failed to decode media URL: {str(e)}</p>
            <p>Please check if the link is correct and try again.</p>
        </body>
        </html>
        """, 400

@flask_app.route("/health")
def health():
    return jsonify({
        "status": "ok", 
        "service": "wasabi-bot-player",
        "timestamp": time.time()
    })

@flask_app.route("/api/files")
def list_files():
    """API endpoint to list files in bucket (admin only)"""
    try:
        response = s3_client.list_objects_v2(Bucket=WASABI_BUCKET, MaxKeys=10)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        return jsonify({"files": files, "count": len(files)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_flask():
    """Run Flask app in a separate thread"""
    logger.info(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}")
    flask_app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False, use_reloader=False)

# --- Helpers & Decorators ---
def is_admin(func):
    """Decorator to check if the user is the admin."""
    @wraps(func)
    async def wrapper(client, message):
        if message.from_user.id == ADMIN_ID:
            await func(client, message)
        else:
            await message.reply_text("⛔️ Access denied. This command is for the admin only.")
    return wrapper

def is_authorized(func):
    """Decorator to check if the user is authorized."""
    @wraps(func)
    async def wrapper(client, message):
        if message.from_user.id in ALLOWED_USERS:
            await func(client, message)
        else:
            await message.reply_text("⛔️ You are not authorized to use this bot. Contact the admin.")
    return wrapper

def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size:
        return "0B"
    size = int(size)
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels) -1 :
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

def encode_url(url):
    """Encode URL for safe passing in Flask routes"""
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')

# --- Progress Callback Management ---
last_update_time = {}

async def progress_callback(current, total, message, status):
    """Updates the progress message in Telegram."""
    chat_id = message.chat.id
    message_id = message.id
    
    # Throttle updates to avoid hitting Telegram API limits
    now = time.time()
    if (now - last_update_time.get(message_id, 0)) < 2 and current != total:
        return
    last_update_time[message_id] = now

    percentage = current * 100 / total
    progress_bar = "[{0}{1}]".format(
        '█' * int(percentage / 5),
        ' ' * (20 - int(percentage / 5))
    )
    
    details = (
        f"**{status}**\n"
        f"`{progress_bar}`\n"
        f"**Progress:** {percentage:.2f}%\n"
        f"**Done:** {humanbytes(current)}\n"
        f"**Total:** {humanbytes(total)}"
    )
    
    try:
        await app.edit_message_text(chat_id, message_id, text=details)
    except Exception as e:
        logger.warning(f"Failed to edit message: {e}")

# --- Enhanced S3 Operations ---
async def upload_to_wasabi(file_path, file_name, status_message):
    """Upload file to Wasabi with retry logic and progress tracking."""
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            loop = asyncio.get_event_loop()
            
            class ProgressTracker:
                def __init__(self):
                    self.uploaded = 0
                    self.file_size = os.path.getsize(file_path)
                
                def __call__(self, bytes_amount):
                    self.uploaded += bytes_amount
                    # Schedule progress update in the main thread
                    asyncio.run_coroutine_threadsafe(
                        progress_callback(
                            self.uploaded, 
                            self.file_size, 
                            status_message, 
                            f"Uploading... (Attempt {attempt + 1}/{max_retries})"
                        ),
                        loop
                    )
            
            progress_tracker = ProgressTracker()
            
            await loop.run_in_executor(
                None,
                lambda: s3_client.upload_file(
                    file_path,
                    WASABI_BUCKET,
                    file_name,
                    Callback=progress_tracker
                )
            )
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.warning(f"Upload attempt {attempt + 1} failed: {error_code}")
            
            if attempt == max_retries - 1:  # Last attempt
                raise e
                
            # Exponential backoff
            delay = base_delay * (2 ** attempt)
            await status_message.edit_text(
                f"⚠️ Upload failed (attempt {attempt + 1}/{max_retries}). "
                f"Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)
    
    return False

async def generate_presigned_url(file_name):
    """Generate presigned URL with error handling."""
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=604800  # 7 days
        )
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        return None

def get_media_type(filename):
    """Determine media type based on file extension"""
    if not filename:
        return 'file'
        
    ext = filename.lower().split('.')[-1]
    video_extensions = ['mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v', '3gp']
    audio_extensions = ['mp3', 'wav', 'ogg', 'flac', 'aac', 'm4a', 'wma', 'opus']
    
    if ext in video_extensions:
        return 'video'
    elif ext in audio_extensions:
        return 'audio'
    else:
        return 'file'

# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        f"👋 Welcome!\n\nThis bot can upload files to Wasabi storage.\n"
        f"Your User ID is: `{message.from_user.id}`\n\n"
        "Send me any file if you are an authorized user."
    )

@app.on_message(filters.command("adduser"))
@is_admin
async def add_user_handler(client: Client, message: Message):
    try:
        user_id_to_add = int(message.text.split(" ", 1)[1])
        ALLOWED_USERS.add(user_id_to_add)
        await message.reply_text(f"✅ User `{user_id_to_add}` has been added successfully.")
    except (IndexError, ValueError):
        await message.reply_text("⚠️ **Usage:** /adduser `<user_id>`")

@app.on_message(filters.command("removeuser"))
@is_admin
async def remove_user_handler(client: Client, message: Message):
    try:
        user_id_to_remove = int(message.text.split(" ", 1)[1])
        if user_id_to_remove == ADMIN_ID:
            await message.reply_text("🚫 You cannot remove the admin.")
            return
        if user_id_to_remove in ALLOWED_USERS:
            ALLOWED_USERS.remove(user_id_to_remove)
            await message.reply_text(f"🗑 User `{user_id_to_remove}` has been removed.")
        else:
            await message.reply_text("🤷 User not found in the authorized list.")
    except (IndexError, ValueError):
        await message.reply_text("⚠️ **Usage:** /removeuser `<user_id>`")
        
@app.on_message(filters.command("listusers"))
@is_admin
async def list_users_handler(client: Client, message: Message):
    user_list = "\n".join([f"- `{user_id}`" for user_id in ALLOWED_USERS])
    await message.reply_text(f"👥 **Authorized Users:**\n{user_list}")

@app.on_message(filters.command("stats"))
@is_admin
async def stats_handler(client: Client, message: Message):
    """Show bot statistics"""
    stats_text = (
        f"🤖 **Bot Statistics**\n"
        f"• Authorized users: {len(ALLOWED_USERS)}\n"
        f"• Wasabi connected: {'✅' if s3_client else '❌'}\n"
        f"• Bucket: {WASABI_BUCKET}\n"
        f"• Region: {WASABI_REGION}\n"
        f"• Flask server: {FLASK_HOST}:{FLASK_PORT}"
    )
    await message.reply_text(stats_text)

@app.on_message(filters.command("player"))
async def player_info_handler(client: Client, message: Message):
    """Show information about the media player"""
    player_url = f"http://{FLASK_HOST}:{FLASK_PORT}"
    await message.reply_text(
        f"🎮 **Media Player Information**\n\n"
        f"• **Player URL:** {player_url}\n"
        f"• **Status:** {'🟢 Online' if FLASK_PORT else '🔴 Offline'}\n"
        f"• **Supported formats:** MP4, MP3, WAV, AVI, MKV, etc.\n\n"
        f"Upload any media file to get a player link automatically!"
    )

# --- File Handling Logic ---
@app.on_message(filters.document | filters.video | filters.audio)
@is_authorized
async def file_handler(client: Client, message: Message):
    if not s3_client:
        await message.reply_text("❌ **Error:** Wasabi client is not initialized. Check server logs.")
        return

    media = message.document or message.video or message.audio
    file_name = media.file_name
    file_size = media.file_size
    
    # Telegram's limit for bots is 2GB for download, 4GB for upload with MTProto API
    if file_size > 4 * 1024 * 1024 * 1024:
        await message.reply_text("❌ **Error:** File is larger than 4GB, which is not supported.")
        return

    status_message = await message.reply_text("🚀 Preparing to process your file...")
    
    # Create unique file path to avoid conflicts
    timestamp = int(time.time())
    safe_filename = f"{timestamp}_{file_name}"
    file_path = f"./downloads/{safe_filename}"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        # 1. Download from Telegram
        await client.download_media(
            message=message,
            file_name=file_path,
            progress=progress_callback,
            progress_args=(status_message, "Downloading...")
        )
        await status_message.edit_text("✅ Download complete. Starting upload to Wasabi...")

        # 2. Upload to Wasabi
        await upload_to_wasabi(file_path, safe_filename, status_message)
        await status_message.edit_text("✅ Upload complete. Generating shareable link...")
        
        # 3. Generate a pre-signed URL (valid for 7 days)
        presigned_url = await generate_presigned_url(safe_filename)
        
        if presigned_url:
            # Determine media type for player
            media_type = get_media_type(file_name)
            
            # Generate Flask player URL
            encoded_url = encode_url(presigned_url)
            player_url = f"http://{FLASK_HOST}:{FLASK_PORT}/player/{media_type}/{encoded_url}"
            
            final_message = (
                f"✅ **File Uploaded Successfully!**\n\n"
                f"**File:** `{file_name}`\n"
                f"**Size:** {humanbytes(file_size)}\n"
                f"**Stored as:** `{safe_filename}`\n\n"
                f"**🎮 Media Player:**\n{player_url}\n\n"
                f"**📎 Direct Link:**\n`{presigned_url}`"
            )
            await status_message.edit_text(final_message, disable_web_page_preview=False)
        else:
            await status_message.edit_text(
                f"✅ **File Uploaded Successfully!**\n\n"
                f"**File:** `{file_name}`\n"
                f"**Size:** {humanbytes(file_size)}\n"
                f"**Stored as:** `{safe_filename}`\n"
                f"⚠️ *Could not generate shareable link*"
            )

    except Exception as e:
        logger.error(f"An error occurred during file processing: {e}", exc_info=True)
        await status_message.edit_text(f"❌ **Upload failed:**\n`{str(e)}`")
    finally:
        # 4. Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up local file: {file_path}")
        if status_message.id in last_update_time:
             del last_update_time[status_message.id]

# --- Main Execution ---
if __name__ == "__main__":
    # Create downloads directory if it doesn't exist
    os.makedirs("./downloads", exist_ok=True)
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    logger.info("Bot is starting...")
    logger.info(f"Flask server running on {FLASK_HOST}:{FLASK_PORT}")
    
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        logger.info("Bot has stopped.")
