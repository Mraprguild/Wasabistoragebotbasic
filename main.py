import os
import asyncio
import boto3
from botocore.exceptions import NoCredentialsError
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait

# --- Configuration ---
from dotenv import load_dotenv
load_dotenv()

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
WASABI_REGION = os.environ.get("WASABI_REGION")
STORAGE_CHANNEL_ID = os.environ.get("STORAGE_CHANNEL_ID") # Optional

# --- Initialize Telegram Bot ---
app = Client("file_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Initialize Wasabi S3 Client ---
s3 = boto3.client(
    's3',
    endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY
)

# In-memory dictionary to store file info (for simplicity)
# In a real-world scenario, you would use a database.
file_storage = {}
file_counter = 1

# --- Helper Functions ---
async def progress(current, total, message: Message, text: str):
    """Updates the message with upload/download progress."""
    try:
        await message.edit_text(f"{text}: {current * 100 / total:.1f}%")
    except FloodWait as e:
        await asyncio.sleep(e.x)
    except Exception:
        # Handle cases where the message might not be editable
        pass

# --- Bot Commands ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "Welcome to the File Storage Bot!\n\n"
        "I can help you upload, download, and stream files up to 4GB using Wasabi Cloud Storage.\n\n"
        "**Features:**\n"
        "- 4GB File Support\n"
        "- Wasabi Cloud Integration\n"
        "- MX Player & VLC Integration\n"
        "- Real-time progress updates\n\n"
        "Use /help to see all available commands."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    help_text = """
**Available Commands:**

/start - Show welcome message and help
/upload - Reply to a file to upload it (or just send any file)
/download <file_id> - Download a file by its ID
/list - List all stored files
/stream <file_id> - Get a streaming link for a video/audio file
/setchannel <channel_id> - Set a storage channel for backups (admin only)
/test - Test the connection to Wasabi
/web - Get the web player interface link (placeholder)
/help - Show this help information
    """
    await message.reply_text(help_text)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message):
    """Handles file uploads sent directly to the bot."""
    global file_counter
    file = message.document or message.video or message.audio or message.photo
    if not file:
        await message.reply_text("Please send a file.")
        return

    file_name = getattr(file, 'file_name', f"upload_{file.file_unique_id}.{file.mime_type.split('/')[1]}")
    file_id = str(file_counter)
    
    progress_message = await message.reply_text("Starting download from Telegram servers...")

    try:
        # Download from Telegram
        file_path = await message.download(
            progress=progress,
            progress_args=(progress_message, "Downloading from Telegram")
        )
        await progress_message.edit_text("Download complete. Now uploading to Wasabi...")

        # Upload to Wasabi
        s3.upload_file(
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=lambda bytes_transferred: asyncio.run(
                progress(bytes_transferred, file.file_size, progress_message, "Uploading to Wasabi")
            )
        )
        
        # Store file info
        file_storage[file_id] = {'name': file_name, 'size': file.file_size}
        file_counter += 1

        await progress_message.edit_text(
            f"✅ **Upload Successful!**\n\n"
            f"**File Name:** `{file_name}`\n"
            f"**File ID:** `{file_id}`\n\n"
            f"Use `/download {file_id}` to get the file or `/stream {file_id}` to get a streaming link."
        )

        # Optional: Forward to backup channel
        if STORAGE_CHANNEL_ID:
            try:
                await client.send_document(int(STORAGE_CHANNEL_ID), file_path, caption=f"Backup: {file_name}\nFile ID: {file_id}")
            except Exception as e:
                await message.reply_text(f"⚠️ Could not forward to backup channel. Error: {e}")

    except NoCredentialsError:
        await progress_message.edit_text("❌ **Error:** Wasabi credentials not found. Please check your environment variables.")
    except Exception as e:
        await progress_message.edit_text(f"❌ **An error occurred:** {e}")
    finally:
        # Clean up the downloaded file
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)


@app.on_message(filters.command("download"))
async def download_command(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <file_id>`")
        return
        
    file_id = message.command[1]
    if file_id not in file_storage:
        await message.reply_text("❌ File ID not found.")
        return

    file_info = file_storage[file_id]
    file_name = file_info['name']
    
    progress_message = await message.reply_text("Generating download link...")

    try:
        # Generate a presigned URL for download
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=3600  # Link expires in 1 hour
        )
        
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Download Now", url=url)]])
        await progress_message.edit_text(
            f"Click the button below to download:\n\n**File:** `{file_name}`",
            reply_markup=keyboard
        )

    except Exception as e:
        await progress_message.edit_text(f"❌ **Could not generate download link:** {e}")


@app.on_message(filters.command("list"))
async def list_files_command(client, message):
    if not file_storage:
        await message.reply_text("No files have been uploaded yet.")
        return

    file_list = "**Stored Files:**\n\n"
    for file_id, info in file_storage.items():
        size_mb = info['size'] / (1024 * 1024)
        file_list += f"**ID:** `{file_id}` - **Name:** `{info['name']}` ({size_mb:.2f} MB)\n"

    await message.reply_text(file_list)


@app.on_message(filters.command("stream"))
async def stream_command(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/stream <file_id>`")
        return

    file_id = message.command[1]
    if file_id not in file_storage:
        await message.reply_text("❌ File ID not found.")
        return
        
    file_info = file_storage[file_id]
    file_name = file_info['name']

    progress_message = await message.reply_text("Generating streaming link...")

    try:
        # Generate a presigned URL for streaming
        stream_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=86400  # Link valid for 24 hours
        )

        # Create player links
        mx_player_link = f"intent:{stream_url}#Intent;package=com.mxtech.videoplayer.ad;end"
        vlc_link = f"vlc://{stream_url}"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Open in MX Player", url=mx_player_link)],
            [InlineKeyboardButton("🟠 Open in VLC", url=vlc_link)],
            [InlineKeyboardButton("🌐 Web Player", url=f"https://example.com/webplayer?url={stream_url}")], # Replace with your actual web player
            [InlineKeyboardButton("🔗 Get Direct Link", callback_data=f"getlink_{stream_url}")]
        ])
        
        await progress_message.edit_text(
            f"**Streaming Links for:** `{file_name}`\n\n"
            "Choose a player to start streaming.",
            reply_markup=keyboard
        )

    except Exception as e:
        await progress_message.edit_text(f"❌ **Could not generate streaming link:** {e}")

# Callback query handler for the "Get Direct Link" button
@app.on_callback_query(filters.regex("^getlink_"))
async def get_direct_link(client, callback_query):
    stream_url = callback_query.data.split("_")[1]
    await callback_query.answer(f"Direct Link:\n{stream_url}", show_alert=True)


@app.on_message(filters.command("test"))
async def test_wasabi_connection(client, message):
    try:
        s3.list_buckets()
        await message.reply_text("✅ Wasabi connection successful!")
    except NoCredentialsError:
        await message.reply_text("❌ **Error:** Wasabi credentials not found. Please check your `.env` file.")
    except Exception as e:
        await message.reply_text(f"❌ **Wasabi connection failed:**\n`{e}`")


if __name__ == "__main__":
    print("Bot is starting...")
    app.run()
    print("Bot stopped.")
