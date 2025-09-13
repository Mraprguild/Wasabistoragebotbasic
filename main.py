#!/usr/bin/env python3
"""
High-Performance Telegram Bot with Wasabi Storage Integration
Supports files up to 5GB with streaming capabilities
"""

import os
import asyncio
import time
import mimetypes
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging
from urllib.parse import quote

from pyrogram import Client, filters, types
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import humanize

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WasabiStorage:
    """Wasabi S3-compatible storage handler"""
    
    def __init__(self):
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f'https://s3.{os.getenv("WASABI_REGION")}.wasabisys.com',
                aws_access_key_id=os.getenv('WASABI_ACCESS_KEY'),
                aws_secret_access_key=os.getenv('WASABI_SECRET_KEY'),
                region_name=os.getenv('WASABI_REGION')
            )
            self.bucket = os.getenv('WASABI_BUCKET')
            logger.info("Wasabi storage initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Wasabi storage: {e}")
            raise

    async def upload_file(self, file_path: str, key: str, progress_callback=None) -> bool:
        """Upload file to Wasabi with progress tracking"""
        try:
            file_size = os.path.getsize(file_path)
            
            # Use multipart upload for files larger than 100MB
            if file_size > 100 * 1024 * 1024:
                return await self._multipart_upload(file_path, key, progress_callback)
            else:
                return await self._simple_upload(file_path, key, progress_callback)
                
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False

    async def _simple_upload(self, file_path: str, key: str, progress_callback=None) -> bool:
        """Simple upload for smaller files"""
        try:
            with open(file_path, 'rb') as file:
                self.s3_client.upload_fileobj(file, self.bucket, key)
            
            if progress_callback:
                await progress_callback(100, 100)
            
            return True
        except Exception as e:
            logger.error(f"Simple upload failed: {e}")
            return False

    async def _multipart_upload(self, file_path: str, key: str, progress_callback=None) -> bool:
        """Multipart upload for large files"""
        try:
            # Create multipart upload
            response = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=key)
            upload_id = response['UploadId']
            
            parts = []
            part_size = 100 * 1024 * 1024  # 100MB parts
            file_size = os.path.getsize(file_path)
            uploaded = 0
            
            with open(file_path, 'rb') as file:
                part_number = 1
                while True:
                    data = file.read(part_size)
                    if not data:
                        break
                    
                    # Upload part
                    response = self.s3_client.upload_part(
                        Bucket=self.bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data
                    )
                    
                    parts.append({
                        'ETag': response['ETag'],
                        'PartNumber': part_number
                    })
                    
                    uploaded += len(data)
                    if progress_callback:
                        progress = (uploaded / file_size) * 100
                        await progress_callback(uploaded, file_size)
                    
                    part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Multipart upload failed: {e}")
            # Abort multipart upload on failure
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id
                )
            except:
                pass
            return False

    def generate_streaming_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate presigned URL for streaming"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate streaming URL: {e}")
            return None

    def delete_file(self, key: str) -> bool:
        """Delete file from Wasabi"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            return False

class TelegramBot:
    """Main Telegram bot class"""
    
    def __init__(self):
        # Initialize Pyrogram client
        self.app = Client(
            "wasabi_bot",
            api_id=int(os.getenv('API_ID')),
            api_hash=os.getenv('API_HASH'),
            bot_token=os.getenv('BOT_TOKEN')
        )
        
        # Initialize storage
        self.storage = WasabiStorage()
        
        # Track active uploads/downloads
        self.active_operations: Dict[int, Dict[str, Any]] = {}
        
        self.setup_handlers()

    def setup_handlers(self):
        """Setup message handlers"""
        
        @self.app.on_message(filters.command("start"))
        async def start_handler(client, message: Message):
            welcome_text = """
🚀 **High-Speed File Bot** 

I can help you upload files to cloud storage and generate streaming links!

**Features:**
• Upload files up to 5GB
• High-speed transfers with progress tracking
• Generate streaming links for direct playback
• Support for MX Player, VLC, and other players

**Commands:**
/upload - Upload a file to cloud storage
/help - Show this help message
/stats - Show your usage statistics

Just send me any file to get started!
            """
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📤 Upload File", callback_data="upload")],
                [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
                [InlineKeyboardButton("❓ Help", callback_data="help")]
            ])
            
            await message.reply_text(welcome_text, reply_markup=keyboard)

        @self.app.on_message(filters.command("help"))
        async def help_handler(client, message: Message):
            help_text = """
🔧 **How to use this bot:**

1. **Upload Files**: Send any file (up to 5GB)
2. **Get Streaming Links**: Receive direct download/streaming URLs
3. **Play in External Players**: Links work with MX Player, VLC, etc.

**Supported File Types:**
• Videos (MP4, MKV, AVI, etc.)
• Audio (MP3, FLAC, WAV, etc.)
• Documents (PDF, DOC, ZIP, etc.)
• Images (JPG, PNG, GIF, etc.)

**Player Support:**
• MX Player: Tap "Open with MX Player"
• VLC: Copy link and open in VLC
• Browser: Direct streaming in browser

**Tips:**
• Large files are processed faster with multipart uploads
• Links expire after 1 hour for security
• Progress is shown in real-time during uploads
            """
            await message.reply_text(help_text)

        @self.app.on_message(filters.document | filters.video | filters.audio | filters.photo)
        async def file_handler(client, message: Message):
            await self.handle_file_upload(message)

    async def handle_file_upload(self, message: Message):
        """Handle file upload with progress tracking"""
        user_id = message.from_user.id
        
        # Get file info
        file_obj = None
        if message.document:
            file_obj = message.document
        elif message.video:
            file_obj = message.video
        elif message.audio:
            file_obj = message.audio
        elif message.photo:
            file_obj = message.photo[-1]  # Get largest photo
        
        if not file_obj:
            await message.reply_text("❌ Unsupported file type!")
            return
        
        file_name = getattr(file_obj, 'file_name', f"file_{file_obj.file_id}")
        file_size = getattr(file_obj, 'file_size', 0)
        
        # Check file size limit (5GB)
        if file_size > 5 * 1024 * 1024 * 1024:
            await message.reply_text("❌ File too large! Maximum size is 5GB.")
            return
        
        # Create progress message
        progress_msg = await message.reply_text(
            f"📤 **Uploading:** `{file_name}`\n"
            f"📦 **Size:** {humanize.naturalsize(file_size)}\n"
            f"⏳ **Progress:** 0%\n"
            f"🚀 **Speed:** Calculating..."
        )
        
        start_time = time.time()
        last_update = start_time
        
        async def progress_callback(current, total):
            nonlocal last_update
            now = time.time()
            
            # Update every 2 seconds to avoid rate limits
            if now - last_update < 2:
                return
            
            last_update = now
            elapsed = now - start_time
            progress = (current / total) * 100
            speed = current / elapsed if elapsed > 0 else 0
            
            # Calculate ETA
            if speed > 0:
                remaining = (total - current) / speed
                eta = str(timedelta(seconds=int(remaining)))
            else:
                eta = "Calculating..."
            
            progress_bar = self.create_progress_bar(progress)
            
            try:
                await progress_msg.edit_text(
                    f"📤 **Uploading:** `{file_name}`\n"
                    f"📦 **Size:** {humanize.naturalsize(total)}\n"
                    f"⏳ **Progress:** {progress:.1f}%\n"
                    f"{progress_bar}\n"
                    f"🚀 **Speed:** {humanize.naturalsize(speed)}/s\n"
                    f"⏱️ **ETA:** {eta}"
                )
            except Exception:
                pass  # Ignore rate limit errors
        
        try:
            # Download file from Telegram
            temp_file = f"temp_{user_id}_{file_obj.file_id}"
            await message.download(temp_file, progress=progress_callback)
            
            # Generate unique key for storage
            timestamp = int(time.time())
            file_extension = os.path.splitext(file_name)[1]
            storage_key = f"{user_id}/{timestamp}_{file_obj.file_id}{file_extension}"
            
            # Upload to Wasabi
            await progress_msg.edit_text("☁️ **Uploading to cloud storage...**")
            
            upload_success = await self.storage.upload_file(
                temp_file, 
                storage_key, 
                progress_callback
            )
            
            # Clean up temp file
            try:
                os.remove(temp_file)
            except:
                pass
            
            if upload_success:
                # Generate streaming URL
                streaming_url = self.storage.generate_streaming_url(storage_key, 3600)
                
                if streaming_url:
                    # Detect file type for appropriate player suggestions
                    mime_type = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
                    
                    success_text = f"""
✅ **Upload Successful!**

📁 **File:** `{file_name}`
📦 **Size:** {humanize.naturalsize(file_size)}
⏱️ **Upload Time:** {time.time() - start_time:.1f}s
🔗 **Streaming URL:** [Click Here]({streaming_url})

**Player Instructions:**
"""
                    
                    # Add player-specific instructions
                    if mime_type.startswith('video/'):
                        success_text += """
🎬 **For MX Player:** Copy link and open in MX Player
📺 **For VLC:** Open VLC → Network Stream → Paste URL
🌐 **For Browser:** Click link for direct streaming
"""
                    elif mime_type.startswith('audio/'):
                        success_text += """
🎵 **For Audio Players:** Copy link and open in your preferred player
🌐 **For Browser:** Click link for direct playback
"""
                    else:
                        success_text += """
📄 **For Documents:** Click link for direct download
🌐 **For Browser:** Click link to view/download
"""
                    
                    success_text += f"\n⚠️ **Note:** Link expires in 1 hour"
                    
                    # Create action buttons
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("📎 Copy Link", url=streaming_url)],
                        [InlineKeyboardButton("🗑️ Delete File", callback_data=f"delete_{storage_key}")],
                        [InlineKeyboardButton("🔄 Generate New Link", callback_data=f"relink_{storage_key}")]
                    ])
                    
                    await progress_msg.edit_text(success_text, reply_markup=keyboard)
                else:
                    await progress_msg.edit_text("❌ Failed to generate streaming URL!")
            else:
                await progress_msg.edit_text("❌ Upload failed! Please try again.")
                
        except Exception as e:
            logger.error(f"File upload error: {e}")
            await progress_msg.edit_text("❌ Upload failed due to an error!")
            
            # Clean up temp file
            try:
                os.remove(temp_file)
            except:
                pass

    def create_progress_bar(self, progress: float, length: int = 20) -> str:
        """Create a visual progress bar"""
        filled = int(length * progress / 100)
        bar = "█" * filled + "░" * (length - filled)
        return f"[{bar}] {progress:.1f}%"

    async def start(self):
        """Start the bot"""
        logger.info("Starting Telegram bot...")
        await self.app.start()
        logger.info("Bot started successfully!")

    async def stop(self):
        """Stop the bot"""
        logger.info("Stopping Telegram bot...")
        await self.app.stop()

async def main():
    """Main function"""
    # Check required environment variables
    required_vars = [
        'API_ID', 'API_HASH', 'BOT_TOKEN',
        'WASABI_ACCESS_KEY', 'WASABI_SECRET_KEY', 
        'WASABI_BUCKET', 'WASABI_REGION'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return
    
    # Create and start bot
    bot = TelegramBot()
    
    try:
        await bot.start()
        logger.info("Bot is running... Press Ctrl+C to stop")
        await asyncio.sleep(float('inf'))
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
