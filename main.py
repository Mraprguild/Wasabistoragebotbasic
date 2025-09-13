import asyncio
import os
import tempfile
from typing import Optional
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
import aiofiles

from config import Config
from wasabi_storage import WasabiStorage
from telegram_storage import TelegramStorage
from web_interface import WebInterface

class TelegramFileBot:
    def __init__(self):
        # Validate configuration
        Config.validate()
        
        # Initialize Pyrogram client with MAXIMUM speed optimizations
        self.app = Client(
            "file_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            max_concurrent_transmissions=Config.MAX_CONCURRENT_TRANSMISSIONS,
            workers=Config.UPLOAD_WORKERS,
            workdir="./sessions",  # Organized session storage
            sleep_threshold=180  # Reduce API delays
        )
        
        # Initialize storage systems
        self.wasabi_storage = WasabiStorage()
        self.telegram_storage = TelegramStorage(self.app)
        
        # Initialize web interface
        self.web_interface = WebInterface(self.wasabi_storage, self.telegram_storage)
        
        # Register handlers
        self.register_handlers()
        
        # Storage for ongoing operations
        self.upload_progress = {}
        self.download_progress = {}
    
    def register_handlers(self):
        """Register bot command handlers"""
        
        @self.app.on_message(filters.command("start"))
        async def start_command(client, message: Message):
            await self.handle_start(message)
        
        @self.app.on_message(filters.command("help"))
        async def help_command(client, message: Message):
            await self.handle_help(message)
        
        @self.app.on_message(filters.command("test"))
        async def test_command(client, message: Message):
            await self.handle_test(message)
        
        @self.app.on_message(filters.command("upload"))
        async def upload_command(client, message: Message):
            await self.handle_upload_command(message)
        
        @self.app.on_message(filters.command("download"))
        async def download_command(client, message: Message):
            await self.handle_download_command(message)
        
        @self.app.on_message(filters.command("list"))
        async def list_command(client, message: Message):
            await self.handle_list(message)
        
        @self.app.on_message(filters.command("stream"))
        async def stream_command(client, message: Message):
            await self.handle_stream(message)
        
        @self.app.on_message(filters.command("web"))
        async def web_command(client, message: Message):
            await self.handle_web(message)
        
        @self.app.on_message(filters.command("setchannel"))
        async def setchannel_command(client, message: Message):
            await self.handle_set_channel(message)
        
        @self.app.on_message(filters.document | filters.video | filters.audio | filters.photo)
        async def handle_media(client, message: Message):
            await self.handle_file_upload(message)
        
        @self.app.on_callback_query()
        async def handle_callback(client, callback_query: CallbackQuery):
            await self.handle_callback_query(callback_query)
    
    async def handle_start(self, message: Message):
        """Handle /start command"""
        welcome_text = """
🤖 **Welcome to Telegram File Bot!**

🌟 **Features:**
• 📁 Upload files up to 4GB
• ☁️ Wasabi cloud storage integration
• 🎬 Direct streaming capabilities
• 📱 MX Player & VLC support
• 💾 Telegram channel backup storage
• 🌐 Web interface for file management

📋 **Commands:**
• Send any file to upload it
• `/upload` - Upload a file
• `/download <file_id>` - Download by ID
• `/list` - View all your files
• `/stream <file_id>` - Get streaming link
• `/web <file_id>` - Web player interface
• `/setchannel <id>` - Set backup channel
• `/test` - Test Wasabi connection
• `/help` - Show this help

🚀 **Ready to store your files securely!**
        """
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📁 Upload File", callback_data="upload_info"),
                InlineKeyboardButton("📋 List Files", callback_data="list_files")
            ],
            [
                InlineKeyboardButton("🌐 Web Interface", url=f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}")
            ],
            [
                InlineKeyboardButton("❓ Help", callback_data="help"),
                InlineKeyboardButton("🔧 Test Connection", callback_data="test_connection")
            ]
        ])
        
        await message.reply_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    async def handle_help(self, message: Message):
        """Handle /help command"""
        help_text = """
📚 **Telegram File Bot Help**

**🔥 Main Features:**
• **4GB File Support** - Handle large files with ease
• **Cloud Storage** - Reliable Wasabi cloud integration
• **Streaming** - Direct video/audio streaming
• **Mobile Support** - MX Player & VLC integration
• **Backup Storage** - Optional Telegram channel storage

**📱 How to Use:**

1️⃣ **Upload Files:**
   • Just send any file to the bot
   • Or use `/upload` command
   • Support for all file types

2️⃣ **Download Files:**
   • Use `/download <file_id>` 
   • Or click download from file list

3️⃣ **Stream Media:**
   • Use `/stream <file_id>` for direct links
   • Use `/web <file_id>` for web player
   • One-click MX Player integration

4️⃣ **Manage Files:**
   • `/list` - See all your files
   • Web interface for full management

5️⃣ **Backup Storage:**
   • `/setchannel <channel_id>` - Set backup channel
   • Files stored in both Wasabi and Telegram

**💡 Tips:**
• Files are stored securely in Wasabi cloud
• Streaming works on all devices
• Mobile users get automatic player detection
• Progress tracking for large files

Need more help? Contact support!
        """
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Back to Start", callback_data="start")],
            [InlineKeyboardButton("🌐 Web Interface", url=f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}")]
        ])
        
        await message.reply_text(help_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    async def handle_test(self, message: Message):
        """Handle /test command"""
        status_msg = await message.reply_text("🔧 Testing Wasabi connection...")
        
        try:
            is_connected = await self.wasabi_storage.test_connection()
            
            if is_connected:
                await status_msg.edit_text(
                    "✅ **Connection Test Successful!**\n\n"
                    f"🌐 **Endpoint:** {Config.WASABI_ENDPOINT}\n"
                    f"🪣 **Bucket:** {Config.WASABI_BUCKET}\n"
                    f"📍 **Region:** {Config.WASABI_REGION}\n\n"
                    "🚀 Bot is ready to handle files!",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await status_msg.edit_text(
                    "❌ **Connection Test Failed!**\n\n"
                    "Please check your Wasabi credentials:\n"
                    "• Access Key\n"
                    "• Secret Key\n"
                    "• Bucket Name\n"
                    "• Region\n\n"
                    "Contact admin for support.",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        except Exception as e:
            await status_msg.edit_text(
                f"❌ **Test Error:**\n`{str(e)}`\n\n"
                "Please check your configuration.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_upload_command(self, message: Message):
        """Handle /upload command"""
        await message.reply_text(
            "📁 **Ready to Upload!**\n\n"
            "Simply send me any file and I'll upload it to the cloud:\n"
            "• 📄 Documents\n"
            "• 🎬 Videos\n"
            "• 🎵 Audio\n"
            "• 🖼️ Photos\n"
            "• 📦 Archives\n\n"
            "**Maximum file size:** 4GB\n"
            "**Supported storage:** Wasabi Cloud + Telegram Backup",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_file_upload(self, message: Message):
        """Handle file upload"""
        if not (message.document or message.video or message.audio or message.photo):
            return
        
        # Get file info
        if message.document:
            file_obj = message.document
            file_name = file_obj.file_name or f"document_{file_obj.file_id}.bin"
        elif message.video:
            file_obj = message.video
            file_name = file_obj.file_name or f"video_{file_obj.file_id}.mp4"
        elif message.audio:
            file_obj = message.audio
            file_name = file_obj.file_name or f"audio_{file_obj.file_id}.mp3"
        elif message.photo:
            file_obj = message.photo
            file_name = f"photo_{file_obj.file_id}.jpg"
        else:
            await message.reply_text("❌ Unsupported file type!")
            return
        
        file_size = getattr(file_obj, 'file_size', 0)
        
        # Check file size
        if file_size > Config.MAX_FILE_SIZE:
            await message.reply_text(
                f"❌ **File too large!**\n\n"
                f"**File size:** {self._format_size(file_size)}\n"
                f"**Maximum:** {self._format_size(Config.MAX_FILE_SIZE)}\n\n"
                "Please upload a smaller file.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Start upload process
        progress_msg = await message.reply_text(
            f"⬆️ **Uploading:** `{file_name}`\n"
            f"📊 **Size:** {self._format_size(file_size)}\n"
            f"📈 **Progress:** 0%",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Download file from Telegram
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
            
            start_time = asyncio.get_event_loop().time()
            
            async def download_progress(current, total):
                progress = (current / total) * 50  # First 50% for download
                # Ultra-fast: Update every 16MB for maximum performance
                if current % Config.PROGRESS_UPDATE_INTERVAL == 0 or current == total:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    speed_mbps = (current / (1024 * 1024)) / max(0.1, elapsed)
                    eta_seconds = ((total - current) / max(1, current)) * elapsed if current > 0 else 0
                    
                    await progress_msg.edit_text(
                        f"🚀 **TURBO DOWNLOAD:** `{file_name}`\n"
                        f"📊 **Size:** {self._format_size(file_size)} | ⚡ **Speed:** {speed_mbps:.1f} MB/s\n"
                        f"📈 **Progress:** {progress:.1f}% | ⏱️ **ETA:** {eta_seconds:.0f}s",
                        parse_mode=ParseMode.MARKDOWN
                    )
            
            await self.app.download_media(message, file_name=temp_path, progress=download_progress)
            
            # Upload to Wasabi with turbo speed
            upload_start = asyncio.get_event_loop().time()
            
            async def upload_progress(progress):
                total_progress = 50 + (progress / 2)  # 50-100% for upload
                upload_elapsed = asyncio.get_event_loop().time() - upload_start
                upload_speed = (file_size * (progress / 100) / (1024 * 1024)) / max(0.1, upload_elapsed)
                
                await progress_msg.edit_text(
                    f"🚀 **TURBO CLOUD UPLOAD:** `{file_name}`\n"
                    f"📊 **Size:** {self._format_size(file_size)} | ⚡ **Speed:** {upload_speed:.1f} MB/s\n"
                    f"📈 **Total Progress:** {total_progress:.1f}%",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            file_id = await self.wasabi_storage.upload_file(temp_path, file_name, upload_progress)
            
            if file_id:
                # Upload to Telegram channel if configured and valid
                if Config.STORAGE_CHANNEL_ID and Config.STORAGE_CHANNEL_ID.strip():
                    try:
                        await self.telegram_storage.upload_file_to_channel(
                            temp_path, file_name, file_id
                        )
                    except Exception as e:
                        print(f"Telegram backup failed: {e}")
                        # Continue without backup storage
                
                # Create success message with actions
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📥 Download", callback_data=f"download_{file_id}"),
                        InlineKeyboardButton("🎬 Stream", callback_data=f"stream_{file_id}")
                    ],
                    [
                        InlineKeyboardButton("🌐 Web Player", callback_data=f"web_{file_id}"),
                        InlineKeyboardButton("📋 File Info", callback_data=f"info_{file_id}")
                    ]
                ])
                
                await progress_msg.edit_text(
                    f"✅ **Upload Successful!**\n\n"
                    f"📁 **File:** `{file_name}`\n"
                    f"📊 **Size:** {self._format_size(file_size)}\n"
                    f"🆔 **File ID:** `{file_id}`\n"
                    f"☁️ **Storage:** Wasabi Cloud\n"
                    f"💾 **Backup:** {'✅ Telegram Channel' if Config.STORAGE_CHANNEL_ID else '❌ Not configured'}\n\n"
                    "**Quick Actions:**",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await progress_msg.edit_text(
                    f"❌ **Upload Failed!**\n\n"
                    f"Failed to upload `{file_name}` to cloud storage.\n"
                    "Please try again or contact support.",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        except Exception as e:
            await progress_msg.edit_text(
                f"❌ **Upload Error!**\n\n"
                f"Error: `{str(e)}`\n"
                "Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        finally:
            # Clean up temporary file
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
    
    async def handle_download_command(self, message: Message):
        """Handle /download command"""
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply_text(
                "❌ **Usage:** `/download <file_id>`\n\n"
                "Get the file ID from `/list` command.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        file_id = command_parts[1]
        await self.download_file(message, file_id)
    
    async def download_file(self, message: Message, file_id: str):
        """Download file by ID"""
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            await message.reply_text(
                f"❌ **File not found!**\n\n"
                f"File ID: `{file_id}`\n"
                "Check the file ID and try again.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Generate download URL
        download_url = await self.wasabi_storage.generate_download_url(file_id, expires_in=3600)
        if not download_url:
            await message.reply_text(
                "❌ **Download Error!**\n\n"
                "Failed to generate download link.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Create download message
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download Now", url=download_url)],
            [
                InlineKeyboardButton("🎬 Stream", callback_data=f"stream_{file_id}"),
                InlineKeyboardButton("🌐 Web Player", callback_data=f"web_{file_id}")
            ]
        ])
        
        await message.reply_text(
            f"📥 **Download Ready!**\n\n"
            f"📁 **File:** `{file_info['file_name']}`\n"
            f"📊 **Size:** {self._format_size(file_info['file_size'])}\n"
            f"🆔 **File ID:** `{file_id}`\n"
            f"⏰ **Link expires in:** 1 hour\n\n"
            "Click the button below to download:",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_list(self, message: Message):
        """Handle /list command"""
        status_msg = await message.reply_text("📋 Loading your files...")
        
        try:
            files = await self.wasabi_storage.list_files()
            
            if not files:
                await status_msg.edit_text(
                    "📁 **No files found!**\n\n"
                    "Upload some files to get started.\n"
                    "Send any file to the bot or use `/upload`.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Sort files by modification date (newest first)
            files.sort(key=lambda x: x.get('last_modified', ''), reverse=True)
            
            # Show first 10 files with pagination
            page_size = 5
            total_files = len(files)
            page_files = files[:page_size]
            
            file_list = []
            for i, file_info in enumerate(page_files, 1):
                file_list.append(
                    f"{i}. **{file_info['file_name']}**\n"
                    f"   📊 {self._format_size(file_info['file_size'])} • "
                    f"🆔 `{file_info['file_id']}`"
                )
            
            # Create inline keyboard for file actions
            keyboard = []
            for file_info in page_files:
                keyboard.append([
                    InlineKeyboardButton(
                        f"📁 {file_info['file_name'][:20]}...",
                        callback_data=f"file_menu_{file_info['file_id']}"
                    )
                ])
            
            if total_files > page_size:
                keyboard.append([
                    InlineKeyboardButton("📄 View All Files", callback_data="view_all_files"),
                    InlineKeyboardButton("🌐 Web Interface", url=f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}")
                ])
            else:
                keyboard.append([
                    InlineKeyboardButton("🌐 Web Interface", url=f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}")
                ])
            
            keyboard_markup = InlineKeyboardMarkup(keyboard)
            
            await status_msg.edit_text(
                f"📋 **Your Files** ({total_files} total)\n\n" +
                "\n\n".join(file_list) +
                f"\n\n{'📄 Showing first 5 files' if total_files > page_size else '✅ All files shown'}",
                reply_markup=keyboard_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        
        except Exception as e:
            await status_msg.edit_text(
                f"❌ **Error loading files:**\n`{str(e)}`",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_stream(self, message: Message):
        """Handle /stream command"""
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply_text(
                "❌ **Usage:** `/stream <file_id>`\n\n"
                "Get the file ID from `/list` command.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        file_id = command_parts[1]
        await self.stream_file(message, file_id)
    
    async def stream_file(self, message: Message, file_id: str):
        """Generate streaming link for file"""
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            await message.reply_text(
                f"❌ **File not found!**\n\n"
                f"File ID: `{file_id}`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Generate streaming URL
        stream_url = await self.wasabi_storage.generate_download_url(file_id, expires_in=3600)
        if not stream_url:
            await message.reply_text(
                "❌ **Streaming Error!**\n\n"
                "Failed to generate streaming link.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Mobile player URLs - fix URL validation
        mx_player_url = stream_url  # Use direct URL for compatibility
        vlc_url = stream_url  # Use direct URL for compatibility
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Direct Stream", url=stream_url)],
            [InlineKeyboardButton("📱 Open in Player", url=stream_url)],
            [InlineKeyboardButton("🌐 Web Player", callback_data=f"web_{file_id}")]
        ])
        
        await message.reply_text(
            f"🎬 **Streaming Ready!**\n\n"
            f"📁 **File:** `{file_info['file_name']}`\n"
            f"📊 **Size:** {self._format_size(file_info['file_size'])}\n"
            f"🎯 **Type:** {file_info.get('content_type', 'Unknown')}\n"
            f"⏰ **Link expires in:** 1 hour\n\n"
            "Choose your preferred player:",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_web(self, message: Message):
        """Handle /web command"""
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply_text(
                "❌ **Usage:** `/web <file_id>`\n\n"
                "Get the file ID from `/list` command.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        file_id = command_parts[1]
        
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            await message.reply_text(
                f"❌ **File not found!**\n\n"
                f"File ID: `{file_id}`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        web_url = f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}/player/{file_id}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Open Web Player", url=web_url)],
            [InlineKeyboardButton("🎬 Direct Stream", callback_data=f"stream_{file_id}")]
        ])
        
        await message.reply_text(
            f"🌐 **Web Player Ready!**\n\n"
            f"📁 **File:** `{file_info['file_name']}`\n"
            f"📊 **Size:** {self._format_size(file_info['file_size'])}\n\n"
            "Click below to open in web player:",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_set_channel(self, message: Message):
        """Handle /setchannel command"""
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply_text(
                "❌ **Usage:** `/setchannel <channel_id>`\n\n"
                "**Examples:**\n"
                "• `/setchannel @your_channel`\n"
                "• `/setchannel -1001234567890`\n"
                "• `/setchannel -100123456789`\n\n"
                "**Steps:**\n"
                "1. Add bot as admin to your channel\n"
                "2. Give it 'Post Messages' permission\n"
                "3. Use this command with your channel ID",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        channel_id = command_parts[1]
        
        status_msg = await message.reply_text("🔧 Testing channel access and permissions...")
        
        # First set the channel in telegram storage
        success = await self.telegram_storage.set_storage_channel(channel_id)
        
        if success:
            # Test by sending a test message
            try:
                # Use the actual channel ID stored in telegram_storage
                actual_channel_id = self.telegram_storage.channel_id
                
                test_msg = await self.app.send_message(
                    chat_id=actual_channel_id,
                    text="✅ **Backup Storage Activated**\n\nThis channel is now configured for file backup storage."
                )
                
                # Delete the test message to keep channel clean
                try:
                    await asyncio.sleep(2)
                    await self.app.delete_messages(actual_channel_id, test_msg.id)
                except:
                    pass
                
                await status_msg.edit_text(
                    f"✅ **Channel Set Successfully!**\n\n"
                    f"📺 **Channel:** `{channel_id}`\n"
                    f"💾 **Backup Storage:** ✅ Active\n"
                    f"🧪 **Test Message:** ✅ Sent & cleaned up\n"
                    f"🔑 **Permissions:** ✅ Verified\n\n"
                    "🚀 **Ready!** New uploads will be backed up to this channel!",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                await status_msg.edit_text(
                    f"⚠️ **Channel Set But Upload Test Failed!**\n\n"
                    f"📺 **Channel:** `{channel_id}`\n"
                    f"❌ **Error:** {str(e)}\n\n"
                    "**Required Actions:**\n"
                    "1️⃣ Add bot as **Admin** to channel\n"
                    "2️⃣ Enable **'Post Messages'** permission\n"
                    "3️⃣ Enable **'Delete Messages'** permission\n"
                    "4️⃣ Try `/setchannel` command again\n\n"
                    "💡 **Tip:** Use `/setchannel @channel_username` format",
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await status_msg.edit_text(
                f"❌ **Failed to set channel!**\n\n"
                f"📺 **Channel:** `{channel_id}`\n\n"
                "**Checklist:**\n"
                "✅ Add bot as admin to channel\n"
                "✅ Grant 'Post Messages' permission\n"
                "✅ Use correct channel ID format\n"
                "✅ Channel must be accessible\n\n"
                "Try again after completing these steps.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_callback_query(self, callback_query: CallbackQuery):
        """Handle inline keyboard callbacks"""
        data = callback_query.data
        
        if data == "start":
            await self.handle_start(callback_query.message)
        elif data == "help":
            await self.handle_help(callback_query.message)
        elif data == "test_connection":
            await self.handle_test(callback_query.message)
        elif data == "upload_info":
            await self.handle_upload_command(callback_query.message)
        elif data == "list_files":
            await self.handle_list(callback_query.message)
        elif data.startswith("download_"):
            file_id = data.replace("download_", "")
            await self.download_file(callback_query.message, file_id)
        elif data.startswith("stream_"):
            file_id = data.replace("stream_", "")
            await self.stream_file(callback_query.message, file_id)
        elif data.startswith("web_"):
            file_id = data.replace("web_", "")
            web_url = f"http://{os.getenv('REPLIT_DEV_DOMAIN', 'localhost:5000')}/player/{file_id}"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Open Web Player", url=web_url)]
            ])
            await callback_query.message.reply_text(
                "🌐 **Web Player Link:**",
                reply_markup=keyboard
            )
        elif data.startswith("info_"):
            file_id = data.replace("info_", "")
            await self.show_file_info(callback_query.message, file_id)
        elif data.startswith("file_menu_"):
            file_id = data.replace("file_menu_", "")
            await self.show_file_menu(callback_query.message, file_id)
        
        await callback_query.answer()
    
    async def show_file_info(self, message: Message, file_id: str):
        """Show detailed file information"""
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            await message.reply_text("❌ File not found!")
            return
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📥 Download", callback_data=f"download_{file_id}"),
                InlineKeyboardButton("🎬 Stream", callback_data=f"stream_{file_id}")
            ],
            [InlineKeyboardButton("🌐 Web Player", callback_data=f"web_{file_id}")]
        ])
        
        await message.reply_text(
            f"📄 **File Information**\n\n"
            f"📁 **Name:** `{file_info['file_name']}`\n"
            f"🆔 **File ID:** `{file_id}`\n"
            f"📊 **Size:** {self._format_size(file_info['file_size'])}\n"
            f"🎯 **Type:** {file_info.get('content_type', 'Unknown')}\n"
            f"📅 **Uploaded:** {file_info.get('last_modified', 'Unknown')}\n"
            f"☁️ **Storage:** Wasabi Cloud\n\n"
            "**Available Actions:**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def show_file_menu(self, message: Message, file_id: str):
        """Show file menu with actions"""
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            await message.reply_text("❌ File not found!")
            return
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📥 Download", callback_data=f"download_{file_id}"),
                InlineKeyboardButton("🎬 Stream", callback_data=f"stream_{file_id}")
            ],
            [
                InlineKeyboardButton("🌐 Web Player", callback_data=f"web_{file_id}"),
                InlineKeyboardButton("📄 Info", callback_data=f"info_{file_id}")
            ],
            [InlineKeyboardButton("⬅️ Back to List", callback_data="list_files")]
        ])
        
        await message.reply_text(
            f"📁 **{file_info['file_name']}**\n\n"
            f"📊 **Size:** {self._format_size(file_info['file_size'])}\n"
            f"🆔 **ID:** `{file_id}`\n\n"
            "**Choose an action:**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_names[i]}"
    
    async def start_bot(self):
        """Start the bot and web interface"""
        web_runner = None
        try:
            print("🤖 Starting Telegram File Bot...")
            
            # Start web interface
            web_runner = await self.web_interface.start_server()
            print(f"🌐 Web interface running on port {Config.WEB_PORT}")
            
            # Start bot
            await self.app.start()
            print("✅ Bot started successfully!")
            print(f"📋 Bot username: @{(await self.app.get_me()).username}")
            
            # Auto-configure backup channel if provided
            if Config.STORAGE_CHANNEL_ID and Config.STORAGE_CHANNEL_ID.strip():
                await self._auto_setup_backup_channel()
            
            # Keep running
            await asyncio.Event().wait()
            
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
        except Exception as e:
            print(f"❌ Error starting bot: {e}")
        finally:
            try:
                if self.app.is_connected:
                    await self.app.stop()
            except:
                pass
            try:
                if web_runner:
                    await web_runner.cleanup()
            except:
                pass
    
    async def _auto_setup_backup_channel(self):
        """Automatically set up backup channel from environment"""
        try:
            print(f"💾 Configuring backup channel: {Config.STORAGE_CHANNEL_ID}")
            success = await self.telegram_storage.set_storage_channel(Config.STORAGE_CHANNEL_ID)
            
            if success:
                print("✅ Backup channel ready - files will be automatically backed up!")
            else:
                print(f"⚠️ Backup channel configuration failed")
        except Exception as e:
            print(f"Backup channel setup error: {e}")

async def main():
    bot = TelegramFileBot()
    await bot.start_bot()

if __name__ == "__main__":
    asyncio.run(main())