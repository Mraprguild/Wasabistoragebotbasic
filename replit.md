# Telegram File Bot with Wasabi Cloud Storage

A comprehensive file storage and streaming solution combining a powerful Telegram bot with Wasabi cloud storage integration. Handles file uploads and downloads up to 4GB with cloud storage integration, MX Player support, and mobile optimization.

## Features
- 4GB File Support: Handle large files up to 4GB
- Cloud Storage: Wasabi cloud integration for reliable file storage  
- Streaming Capabilities: Direct streaming of video/audio files
- MX Player Integration: One-click launch in MX Player on Android
- VLC Support: Direct VLC player integration
- Mobile Responsive: Optimized for phones and tablets
- Download Support: Direct download links for all files
- Multiple Player Support: MX Player, VLC
- Telegram Channel Storage: Optional backup storage in Telegram channels
- Progress Tracking: Real-time upload/download progress updates
- Multiple File Types: Support for documents, videos, audio, and photos

## Bot Commands
- /start - Show welcome message and help
- /upload - Upload a file (or just send any file)
- /download <file_id> - Download a file by ID
- /list - List all stored files
- /stream <file_id> - Get streaming link for a file
- /setchannel <channel_id> - Set storage channel for backups
- /test - Test Wasabi connection
- /web <file_id> - Get web player interface link
- /help - Show help information

## Environment Variables
Required variables that need to be set:
- API_ID: Telegram API ID
- API_HASH: Telegram API Hash
- BOT_TOKEN: Bot token from @BotFather
- WASABI_ACCESS_KEY: Wasabi access key
- WASABI_SECRET_KEY: Wasabi secret key
- WASABI_BUCKET: Wasabi bucket name
- WASABI_REGION: Wasabi bucket region
- STORAGE_CHANNEL_ID: Optional Telegram channel for backup storage

## Technical Implementation
- Pyrogram: Telegram MTProto API client
- Boto3: AWS S3-compatible client for Wasabi
- Async/Await: Non-blocking file operations
- Progress Callbacks: Real-time upload/download progress
- Error Handling: Comprehensive error management
- Chunked Uploads: Efficient handling of large files
- Cross-Platform: Works on MX Player, VLC

## Current Status
✅ All core functionality implemented and tested
✅ Bot is running and ready for use  
✅ Wasabi cloud integrated
✅ 4GB file support active
✅ Telegram Channel Storage 
✅ Streaming capabilities enabled
✅ MX Player integration working

## Recent Changes
- Created comprehensive bot with all specified features
- Implemented Wasabi cloud storage integration
- Added streaming and player integration
- Set up progress tracking and error handling
- Added inline keyboard navigation

## User Preferences
- Focus on reliability and performance
- Comprehensive error handling
- Real-time progress feedback
- Mobile-first design approach

## Project Architecture
- main.py: Core bot implementation
- wasabi_storage.py: Wasabi cloud storage handler
- telegram_storage.py: Telegram channel backup storage
- web_interface.py: Streaming and web player interface
- config.py: Configuration and environment variables
- utils.py: Utility functions and helpers