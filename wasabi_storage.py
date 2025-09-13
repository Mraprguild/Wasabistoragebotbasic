import boto3
import aiofiles
import asyncio
from typing import Optional, Callable
from botocore.exceptions import ClientError, NoCredentialsError
from config import Config
import os
import uuid

class WasabiStorage:
    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=Config.WASABI_ACCESS_KEY,
            aws_secret_access_key=Config.WASABI_SECRET_KEY
        )
        
        # Configure for maximum speed
        from botocore.config import Config as BotoConfig
        
        boto_config = BotoConfig(
            region_name=Config.WASABI_REGION,
            retries={'max_attempts': 10, 'mode': 'adaptive'},
            max_pool_connections=Config.CONNECTION_POOL_SIZE,
            # Enable multi-part upload for faster large file transfers
            s3={
                'max_concurrent_requests': Config.MAX_CONCURRENT_TRANSMISSIONS,
                'max_bandwidth': None,  # No bandwidth limit
                'use_accelerate_endpoint': False,
                'addressing_style': 'virtual'
            }
        )
        
        self.client = self.session.client(
            's3',
            endpoint_url=Config.WASABI_ENDPOINT,
            config=boto_config
        )
        
        self.bucket = Config.WASABI_BUCKET
    
    async def test_connection(self) -> bool:
        """Test connection to Wasabi"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self.client.head_bucket(Bucket=self.bucket))
            return True
        except Exception as e:
            print(f"Wasabi connection test failed: {e}")
            return False
    
    async def upload_file(self, file_path: str, file_name: str, progress_callback: Optional[Callable] = None) -> Optional[str]:
        """Upload file to Wasabi storage"""
        try:
            # Generate unique file ID
            file_id = str(uuid.uuid4())
            object_key = f"files/{file_id}/{file_name}"
            
            # Get file size for progress tracking
            file_size = os.path.getsize(file_path)
            uploaded = 0
            
            def upload_callback(bytes_amount):
                nonlocal uploaded
                uploaded += bytes_amount
                if progress_callback and uploaded % (8 * 1024 * 1024) == 0:  # Update every 8MB for maximum speed
                    progress = (uploaded / file_size) * 100
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(progress_callback(progress))
                    except RuntimeError:
                        pass  # Skip update if no event loop
            
            # Upload file with better error handling
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.client.upload_file(
                        file_path,
                        self.bucket,
                        object_key,
                        Callback=upload_callback
                    )
                )
            except Exception as upload_error:
                print(f"Wasabi upload error: {upload_error}")
                return None
            
            # Store metadata with safe tag values
            def sanitize_tag_value(value):
                """Sanitize tag values for S3 compatibility"""
                if not value:
                    return "unknown"
                # Remove invalid characters and limit length
                sanitized = ''.join(c for c in str(value) if c.isalnum() or c in '.-_')
                return sanitized[:256] if sanitized else "file"
            
            metadata = {
                'original_name': sanitize_tag_value(file_name),
                'file_size': str(file_size),
                'content_type': sanitize_tag_value(self._get_content_type(file_name))
            }
            
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.client.put_object_tagging(
                        Bucket=self.bucket,
                        Key=object_key,
                        Tagging={'TagSet': [{'Key': k, 'Value': v} for k, v in metadata.items()]}
                    )
                )
            except Exception as tag_error:
                print(f"Warning: Could not set tags: {tag_error}")
                # Continue without tags - file upload was successful
            
            return file_id
            
        except Exception as e:
            print(f"Upload failed: {e}")
            return None
    
    async def download_file(self, file_id: str, download_path: str, progress_callback: Optional[Callable] = None) -> bool:
        """Download file from Wasabi storage"""
        try:
            # Find file by ID
            object_key = await self._find_object_by_id(file_id)
            if not object_key:
                return False
            
            # Get file size for progress tracking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.head_object(Bucket=self.bucket, Key=object_key)
            )
            file_size = response['ContentLength']
            downloaded = 0
            
            def download_callback(bytes_amount):
                nonlocal downloaded
                downloaded += bytes_amount
                if progress_callback:
                    progress = (downloaded / file_size) * 100
                    asyncio.create_task(progress_callback(progress))
            
            # Download file
            await loop.run_in_executor(
                None,
                lambda: self.client.download_file(
                    self.bucket,
                    object_key,
                    download_path,
                    Callback=download_callback
                )
            )
            
            return True
            
        except Exception as e:
            print(f"Download failed: {e}")
            return False
    
    async def get_file_info(self, file_id: str) -> Optional[dict]:
        """Get file information"""
        try:
            object_key = await self._find_object_by_id(file_id)
            if not object_key:
                return None
            
            loop = asyncio.get_event_loop()
            
            # Get object metadata
            response = await loop.run_in_executor(
                None,
                lambda: self.client.head_object(Bucket=self.bucket, Key=object_key)
            )
            
            # Get tags with fallback
            try:
                tags_response = await loop.run_in_executor(
                    None,
                    lambda: self.client.get_object_tagging(Bucket=self.bucket, Key=object_key)
                )
                tags = {tag['Key']: tag['Value'] for tag in tags_response.get('TagSet', [])}
            except:
                tags = {}
            
            # Use filename from object key if no tags available
            original_name = tags.get('original_name', object_key.split('/')[-1])
            
            return {
                'file_id': file_id,
                'object_key': object_key,
                'file_name': original_name,
                'file_size': int(response['ContentLength']),
                'content_type': tags.get('content_type', response.get('ContentType', 'application/octet-stream')),
                'last_modified': response['LastModified'].isoformat() if response['LastModified'] else None
            }
            
        except Exception as e:
            print(f"Failed to get file info: {e}")
            return None
    
    async def generate_download_url(self, file_id: str, expires_in: int = 3600) -> Optional[str]:
        """Generate presigned download URL"""
        try:
            object_key = await self._find_object_by_id(file_id)
            if not object_key:
                return None
            
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                None,
                lambda: self.client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.bucket, 'Key': object_key},
                    ExpiresIn=expires_in
                )
            )
            
            return url
            
        except Exception as e:
            print(f"Failed to generate download URL: {e}")
            return None
    
    async def list_files(self, prefix: str = "files/") -> list:
        """List all files in storage"""
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            )
            
            files = []
            for obj in response.get('Contents', []):
                object_key = obj['Key']
                file_id = object_key.split('/')[1] if '/' in object_key else None
                
                if file_id:
                    file_info = await self.get_file_info(file_id)
                    if file_info:
                        files.append(file_info)
            
            return files
            
        except Exception as e:
            print(f"Failed to list files: {e}")
            return []
    
    async def delete_file(self, file_id: str) -> bool:
        """Delete file from storage"""
        try:
            object_key = await self._find_object_by_id(file_id)
            if not object_key:
                return False
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.delete_object(Bucket=self.bucket, Key=object_key)
            )
            
            return True
            
        except Exception as e:
            print(f"Failed to delete file: {e}")
            return False
    
    async def _find_object_by_id(self, file_id: str) -> Optional[str]:
        """Find object key by file ID"""
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.list_objects_v2(Bucket=self.bucket, Prefix=f"files/{file_id}/")
            )
            
            contents = response.get('Contents', [])
            if contents:
                return contents[0]['Key']
            
            return None
            
        except Exception as e:
            print(f"Failed to find object: {e}")
            return None
    
    def _get_content_type(self, filename: str) -> str:
        """Get content type based on file extension"""
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        
        content_types = {
            'mp4': 'video/mp4',
            'avi': 'video/x-msvideo',
            'mkv': 'video/x-matroska',
            'mov': 'video/quicktime',
            'wmv': 'video/x-ms-wmv',
            'flv': 'video/x-flv',
            'webm': 'video/webm',
            'mp3': 'audio/mpeg',
            'wav': 'audio/wav',
            'flac': 'audio/flac',
            'aac': 'audio/aac',
            'ogg': 'audio/ogg',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'webp': 'image/webp',
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'txt': 'text/plain',
            'zip': 'application/zip',
            'rar': 'application/x-rar-compressed',
            '7z': 'application/x-7z-compressed'
        }
        
        return content_types.get(ext, 'application/octet-stream')