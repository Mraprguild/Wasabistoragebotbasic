import asyncio
import aiofiles
import os
from typing import Optional, Dict, List
from pyrogram import Client
from pyrogram.types import Message
from config import Config

class TelegramStorage:
    def __init__(self, app: Client):
        self.app = app
        self.channel_id = None
        self.file_metadata = {}  # In-memory storage for file metadata
        
        # Auto-configure channel if provided in environment
        if Config.STORAGE_CHANNEL_ID:
            asyncio.create_task(self._auto_configure_channel())
    
    async def set_storage_channel(self, channel_id: str) -> bool:
        """Set the storage channel ID with simplified validation for pre-configured channels"""
        try:
            # Handle different channel ID formats
            if channel_id.startswith('@'):
                chat_id = channel_id
            elif channel_id.startswith('-100'):
                chat_id = int(channel_id)
            elif channel_id.isdigit() or (channel_id.startswith('-') and channel_id[1:].isdigit()):
                chat_id = int(channel_id)
            else:
                chat_id = channel_id
            
            # For pre-configured channels, skip validation and set directly
            self.channel_id = chat_id
            print(f"✅ Storage channel set: {chat_id}")
            return True
            
        except Exception as e:
            print(f"Failed to set storage channel: {e}")
            return False
    
    async def _auto_configure_channel(self):
        """Auto-configure backup channel from environment variable"""
        try:
            await asyncio.sleep(2)  # Wait for app to be ready
            if Config.STORAGE_CHANNEL_ID:
                success = await self.set_storage_channel(Config.STORAGE_CHANNEL_ID)
                if success:
                    print(f"✅ Auto-configured backup channel: {Config.STORAGE_CHANNEL_ID}")
                else:
                    print(f"⚠️ Failed to auto-configure backup channel: {Config.STORAGE_CHANNEL_ID}")
        except Exception as e:
            print(f"Auto-configure channel error: {e}")
    
    async def upload_file_to_channel(self, file_path: str, file_name: str, file_id: str, 
                                   progress_callback: Optional[callable] = None) -> Optional[List[int]]:
        """Upload file to Telegram channel as backup storage"""
        if not self.channel_id:
            print("No storage channel configured")
            return None
        
        try:
            print(f"📤 Uploading {file_name} to backup channel...")
            file_size = os.path.getsize(file_path)
            
            # For files larger than 2GB, split into chunks
            max_chunk_size = 2 * 1024 * 1024 * 1024  # 2GB
            message_ids = []
            
            if file_size <= max_chunk_size:
                # Upload as single file
                async def progress_func(current, total):
                    if progress_callback:
                        progress = (current / total) * 100
                        await progress_callback(progress)
                
                message = await self.app.send_document(
                    chat_id=self.channel_id,
                    document=file_path,
                    caption=f"📁 **{file_name}**\n🆔 File ID: `{file_id}`\n📊 Size: {self._format_size(file_size)}",
                    progress=progress_func
                )
                message_ids.append(message.id)
            else:
                # Split into chunks for large files
                chunk_size = max_chunk_size
                chunks_total = (file_size + chunk_size - 1) // chunk_size
                
                async with aiofiles.open(file_path, 'rb') as f:
                    for chunk_num in range(chunks_total):
                        chunk_data = await f.read(chunk_size)
                        
                        # Create temporary chunk file
                        chunk_path = f"/tmp/{file_id}_chunk_{chunk_num}.tmp"
                        async with aiofiles.open(chunk_path, 'wb') as chunk_file:
                            await chunk_file.write(chunk_data)
                        
                        try:
                            async def chunk_progress(current, total):
                                if progress_callback:
                                    overall_progress = ((chunk_num * chunk_size + current) / file_size) * 100
                                    await progress_callback(overall_progress)
                            
                            message = await self.app.send_document(
                                chat_id=self.channel_id,
                                document=chunk_path,
                                caption=f"📁 **{file_name}** (Part {chunk_num + 1}/{chunks_total})\n🆔 File ID: `{file_id}`\n📊 Chunk Size: {self._format_size(len(chunk_data))}",
                                progress=chunk_progress
                            )
                            message_ids.append(message.id)
                        finally:
                            # Clean up temporary chunk file
                            if os.path.exists(chunk_path):
                                os.remove(chunk_path)
            
            # Store metadata
            self.file_metadata[file_id] = {
                'file_name': file_name,
                'file_size': file_size,
                'message_ids': message_ids,
                'channel_id': self.channel_id,
                'chunks': len(message_ids)
            }
            
            return message_ids
            
        except Exception as e:
            print(f"Failed to upload to Telegram channel: {e}")
            return None
    
    async def download_file_from_channel(self, file_id: str, download_path: str,
                                       progress_callback: Optional[callable] = None) -> bool:
        """Download file from Telegram channel"""
        if file_id not in self.file_metadata:
            return False
        
        try:
            metadata = self.file_metadata[file_id]
            message_ids = metadata['message_ids']
            
            if len(message_ids) == 1:
                # Single file download
                message = await self.app.get_messages(self.channel_id, message_ids[0])
                if message.document:
                    await self.app.download_media(
                        message,
                        file_name=download_path,
                        progress=progress_callback
                    )
                    return True
            else:
                # Multi-chunk download and reassembly
                async with aiofiles.open(download_path, 'wb') as output_file:
                    for i, message_id in enumerate(message_ids):
                        message = await self.app.get_messages(self.channel_id, message_id)
                        if message.document:
                            chunk_path = f"/tmp/{file_id}_download_chunk_{i}.tmp"
                            
                            try:
                                async def chunk_progress(current, total):
                                    if progress_callback:
                                        overall_progress = ((i * 100 + (current / total) * 100) / len(message_ids))
                                        await progress_callback(overall_progress)
                                
                                await self.app.download_media(
                                    message,
                                    file_name=chunk_path,
                                    progress=chunk_progress
                                )
                                
                                # Append chunk to output file
                                async with aiofiles.open(chunk_path, 'rb') as chunk_file:
                                    chunk_data = await chunk_file.read()
                                    await output_file.write(chunk_data)
                                
                            finally:
                                # Clean up chunk file
                                if os.path.exists(chunk_path):
                                    os.remove(chunk_path)
                
                return True
            
        except Exception as e:
            print(f"Failed to download from Telegram channel: {e}")
            return False
        
        return False
    
    async def get_channel_files(self) -> List[Dict]:
        """Get list of files stored in the channel"""
        if not self.channel_id:
            return []
        
        files = []
        for file_id, metadata in self.file_metadata.items():
            files.append({
                'file_id': file_id,
                'file_name': metadata['file_name'],
                'file_size': metadata['file_size'],
                'chunks': metadata['chunks'],
                'storage_type': 'telegram_channel'
            })
        
        return files
    
    async def delete_file_from_channel(self, file_id: str) -> bool:
        """Delete file from Telegram channel"""
        if file_id not in self.file_metadata:
            return False
        
        try:
            metadata = self.file_metadata[file_id]
            message_ids = metadata['message_ids']
            
            # Delete all messages
            await self.app.delete_messages(self.channel_id, message_ids)
            
            # Remove from metadata
            del self.file_metadata[file_id]
            
            return True
            
        except Exception as e:
            print(f"Failed to delete from Telegram channel: {e}")
            return False
    
    async def get_file_info(self, file_id: str) -> Optional[Dict]:
        """Get file information from channel storage"""
        if file_id not in self.file_metadata:
            return None
        
        metadata = self.file_metadata[file_id]
        return {
            'file_id': file_id,
            'file_name': metadata['file_name'],
            'file_size': metadata['file_size'],
            'chunks': metadata['chunks'],
            'storage_type': 'telegram_channel',
            'message_ids': metadata['message_ids']
        }
    
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