# Telegram File Bot with Wasabi Cloud Storage

A powerful Telegram file management bot integrated with **Wasabi Cloud Storage**.  
Upload, stream, and share files up to **4GB**, with **MX Player** and **VLC** support — all optimized for mobile use.

---

## 🚀 Features
- **4GB File Support** – Handle large files easily  
- **Wasabi Cloud Integration** – Secure and fast cloud storage  
- **Streaming Support** – Direct streaming of video and audio  
- **MX Player / VLC Integration** – One-click playback  
- **Mobile Responsive** – Works smoothly on phones and tablets  
- **Direct Downloads** – Share instant download links  
- **Telegram Channel Storage** – Optional Telegram backup  
- **Progress Tracking** – Real-time upload/download progress  
- **Multi-Format Support** – Documents, videos, audio, and photos  

---

## 💬 Bot Commands

| Command | Description |
|----------|-------------|
| `/start` | Show welcome message and help |
| `/upload` | Upload a file (or just send a file) |
| `/download <file_id>` | Download file by ID |
| `/list` | List all stored files |
| `/stream <file_id>` | Get streaming link |
| `/web <file_id>` | Open web player interface |
| `/setchannel <channel_id>` | Set Telegram channel for backups |
| `/test` | Test Wasabi connection |
| `/help` | Show help information |

---

## ⚙️ Environment Variables

| Variable | Description |
|-----------|-------------|
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `BOT_TOKEN` | Bot token from @BotFather |
| `WASABI_ACCESS_KEY` | Wasabi access key |
| `WASABI_SECRET_KEY` | Wasabi secret key |
| `WASABI_BUCKET` | Wasabi bucket name |
| `WASABI_REGION` | Wasabi region (e.g., us-east-1) |
| `STORAGE_CHANNEL_ID` | Optional Telegram channel ID for backup |

---

## 🧩 Technical Implementation
- **Framework:** Pyrogram (Telegram MTProto API client)  
- **Cloud SDK:** Boto3 (AWS S3-compatible for Wasabi)  
- **Async Architecture:** Non-blocking I/O with `async/await`  
- **Progress Callbacks:** Real-time upload/download tracking  
- **Error Handling:** Robust retry and exception management  
- **Chunked Uploads:** Efficient large file transfers  
- **Cross-Platform:** MX Player & VLC support  

---

## 📁 Project Structure

