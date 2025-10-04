from aiohttp import web, ClientSession
import aiofiles
import os
import json
from typing import Optional, Dict
from config import Config

class WebInterface:
    def __init__(self, wasabi_storage, telegram_storage):
        self.wasabi_storage = wasabi_storage
        self.telegram_storage = telegram_storage
        self.app = web.Application()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup web routes"""
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/stream/{file_id}', self.stream_file)
        self.app.router.add_get('/download/{file_id}', self.download_file)
        self.app.router.add_get('/player/{file_id}', self.web_player)
        self.app.router.add_get('/api/files', self.list_files_api)
        self.app.router.add_get('/api/file/{file_id}', self.file_info_api)
        # Add static route only if directory exists
        if os.path.exists('static'):
            self.app.router.add_static('/static', 'static')
    
    async def index(self, request):
        """Main page"""
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Telegram File Bot - Cloud Storage</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                h1 { color: #333; text-align: center; margin-bottom: 30px; }
                .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
                .stat-card { background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }
                .stat-value { font-size: 2em; font-weight: bold; color: #007bff; }
                .stat-label { color: #666; margin-top: 5px; }
                .file-list { margin-top: 30px; }
                .file-item { background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; }
                .file-info { flex: 1; }
                .file-name { font-weight: bold; color: #333; }
                .file-size { color: #666; font-size: 0.9em; }
                .file-actions { display: flex; gap: 10px; }
                .btn { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
                .btn-primary { background: #007bff; color: white; }
                .btn-success { background: #28a745; color: white; }
                .btn-info { background: #17a2b8; color: white; }
                @media (max-width: 768px) {
                    .file-item { flex-direction: column; gap: 10px; }
                    .file-actions { justify-content: center; }
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🤖 Telegram File Bot</h1>
                <p style="text-align: center; color: #666; margin-bottom: 30px;">
                    Cloud Storage & Streaming Solution with Wasabi Integration
                </p>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value" id="total-files">-</div>
                        <div class="stat-label">Total Files</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value" id="total-size">-</div>
                        <div class="stat-label">Total Storage</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">4GB</div>
                        <div class="stat-label">Max File Size</div>
                    </div>
                </div>
                
                <div class="file-list">
                    <h3>Recent Files</h3>
                    <div id="files-container">
                        <p style="text-align: center; color: #666;">Loading files...</p>
                    </div>
                </div>
            </div>
            
            <script>
                async function loadFiles() {
                    try {
                        const response = await fetch('/api/files');
                        const files = await response.json();
                        
                        document.getElementById('total-files').textContent = files.length;
                        
                        const totalSize = files.reduce((sum, file) => sum + file.file_size, 0);
                        document.getElementById('total-size').textContent = formatSize(totalSize);
                        
                        const container = document.getElementById('files-container');
                        if (files.length === 0) {
                            container.innerHTML = '<p style="text-align: center; color: #666;">No files uploaded yet. Use the Telegram bot to upload files!</p>';
                            return;
                        }
                        
                        container.innerHTML = files.map(file => `
                            <div class="file-item">
                                <div class="file-info">
                                    <div class="file-name">${file.file_name}</div>
                                    <div class="file-size">${formatSize(file.file_size)} • ${new Date(file.last_modified).toLocaleDateString()}</div>
                                </div>
                                <div class="file-actions">
                                    <a href="/download/${file.file_id}" class="btn btn-primary">Download</a>
                                    ${isVideoOrAudio(file.content_type) ? `<a href="/player/${file.file_id}" class="btn btn-success">Play</a>` : ''}
                                    <a href="/stream/${file.file_id}" class="btn btn-info">Stream</a>
                                </div>
                            </div>
                        `).join('');
                        
                    } catch (error) {
                        console.error('Failed to load files:', error);
                        document.getElementById('files-container').innerHTML = '<p style="text-align: center; color: #dc3545;">Failed to load files</p>';
                    }
                }
                
                function formatSize(bytes) {
                    if (bytes === 0) return '0 B';
                    const k = 1024;
                    const sizes = ['B', 'KB', 'MB', 'GB'];
                    const i = Math.floor(Math.log(bytes) / Math.log(k));
                    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
                }
                
                function isVideoOrAudio(contentType) {
                    return contentType && (contentType.startsWith('video/') || contentType.startsWith('audio/'));
                }
                
                loadFiles();
                setInterval(loadFiles, 30000); // Refresh every 30 seconds
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def stream_file(self, request):
        """Stream file endpoint"""
        file_id = request.match_info['file_id']
        
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            return web.Response(text="File not found", status=404)
        
        # Generate streaming URL
        stream_url = await self.wasabi_storage.generate_download_url(file_id, expires_in=3600)
        if not stream_url:
            return web.Response(text="Failed to generate stream URL", status=500)
        
        # Redirect to Wasabi URL for direct streaming
        return web.Response(status=302, headers={'Location': stream_url})
    
    async def download_file(self, request):
        """Download file endpoint"""
        file_id = request.match_info['file_id']
        
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            return web.Response(text="File not found", status=404)
        
        # Generate download URL
        download_url = await self.wasabi_storage.generate_download_url(file_id, expires_in=3600)
        if not download_url:
            return web.Response(text="Failed to generate download URL", status=500)
        
        # Redirect to Wasabi URL for direct download
        return web.Response(status=302, headers={'Location': download_url})
    
    async def web_player(self, request):
        """Web player interface"""
        file_id = request.match_info['file_id']
        
        # Get file info
        file_info = await self.wasabi_storage.get_file_info(file_id)
        if not file_info:
            return web.Response(text="File not found", status=404)
        
        # Generate streaming URL
        stream_url = await self.wasabi_storage.generate_download_url(file_id, expires_in=3600)
        if not stream_url:
            return web.Response(text="Failed to generate stream URL", status=500)
        
        # Generate player HTML
        player_html = await self._generate_player_html(file_info, stream_url)
        return web.Response(text=player_html, content_type='text/html')
    
    async def list_files_api(self, request):
        """API endpoint to list files"""
        try:
            files = await self.wasabi_storage.list_files()
            # Add CORS headers for web interface
            response = web.json_response(files)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            return response
        except Exception as e:
            print(f"Error listing files: {e}")
            # Return empty list instead of error for better UX
            response = web.json_response([])
            response.headers['Access-Control-Allow-Origin'] = '*'
            return response
    
    async def file_info_api(self, request):
        """API endpoint to get file info"""
        file_id = request.match_info['file_id']
        
        try:
            file_info = await self.wasabi_storage.get_file_info(file_id)
            if not file_info:
                return web.json_response({'error': 'File not found'}, status=404)
            
            return web.json_response(file_info)
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    async def _generate_player_html(self, file_info: Dict, stream_url: str) -> str:
        """Generate HTML player interface"""
        content_type = file_info.get('content_type', '')
        file_name = file_info.get('file_name', 'Unknown')
        
        if content_type.startswith('video/'):
            player_element = f"""
            <video controls width="100%" style="max-height: 70vh;">
                <source src="{stream_url}" type="{content_type}">
                Your browser does not support the video tag.
            </video>
            """
        elif content_type.startswith('audio/'):
            player_element = f"""
            <audio controls style="width: 100%;">
                <source src="{stream_url}" type="{content_type}">
                Your browser does not support the audio tag.
            </audio>
            """
        else:
            player_element = f"""
            <div style="text-align: center; padding: 50px;">
                <p>Preview not available for this file type.</p>
                <a href="{stream_url}" class="btn btn-primary">Download File</a>
            </div>
            """
        
        # Mobile player detection and MX Player support
        mx_player_url = f"intent:{stream_url}#Intent;package=com.mxtech.videoplayer.ad;end"
        vlc_url = f"vlc://{stream_url}"
        
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Player - {file_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #000; color: white; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ text-align: center; margin-bottom: 20px; }}
                .player-container {{ background: #222; padding: 20px; border-radius: 10px; margin-bottom: 20px; }}
                .controls {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin-top: 20px; }}
                .btn {{ padding: 12px 24px; border: none; border-radius: 6px; cursor: pointer; text-decoration: none; display: inline-block; font-weight: bold; }}
                .btn-primary {{ background: #007bff; color: white; }}
                .btn-success {{ background: #28a745; color: white; }}
                .btn-warning {{ background: #ffc107; color: #212529; }}
                .btn-info {{ background: #17a2b8; color: white; }}
                @media (max-width: 768px) {{
                    .controls {{ flex-direction: column; align-items: center; }}
                    .btn {{ width: 200px; text-align: center; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎬 {file_name}</h1>
                    <p>File Size: {self._format_size(file_info.get('file_size', 0))}</p>
                </div>
                
                <div class="player-container">
                    {player_element}
                </div>
                
                <div class="controls">
                    <a href="{stream_url}" class="btn btn-primary">📥 Direct Download</a>
                    <a href="{mx_player_url}" class="btn btn-success">📱 Open in MX Player</a>
                    <a href="{vlc_url}" class="btn btn-warning">🎥 Open in VLC</a>
                    <a href="/" class="btn btn-info">🏠 Back to Home</a>
                </div>
                
                <script>
                    // Auto-detect mobile and suggest appropriate player
                    function detectMobile() {{
                        return window.innerWidth <= 768 || /Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
                    }}
                    
                    if (detectMobile()) {{
                        console.log('Mobile device detected');
                        // Could add mobile-specific functionality here
                    }}
                </script>
            </div>
        </body>
        </html>
        """
        
        return html
    
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
    
    async def start_server(self):
        """Start the web server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, Config.WEB_HOST, Config.WEB_PORT)
        await site.start()
        print(f"Web interface started on http://{Config.WEB_HOST}:{Config.WEB_PORT}")
        return runner
