# web_server.py
import os
import logging
from flask import Flask, render_template, jsonify

logger = logging.getLogger(__name__)

def create_flask_app():
    """Create and configure the Flask app"""
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
                    padding: 30px;
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
        import base64
        # Decode the URL
        try:
            # Add padding if needed
            padding = 4 - (len(encoded_url) % 4)
            if padding != 4:
                encoded_url += '=' * padding
            media_url = base64.urlsafe_b64decode(encoded_url).decode()
            
            # HTML template with media player
            if media_type == 'video':
                player_html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Video Player</title>
                    <style>
                        body {{
                            margin: 0;
                            padding: 20px;
                            background: #1a1a1a;
                            color: white;
                            font-family: Arial, sans-serif;
                            text-align: center;
                        }}
                        .container {{
                            max-width: 800px;
                            margin: 0 auto;
                        }}
                        video {{
                            width: 100%;
                            max-width: 800px;
                            margin: 20px 0;
                            border-radius: 10px;
                        }}
                        .download-btn {{
                            display: inline-block;
                            padding: 12px 24px;
                            background: #007bff;
                            color: white;
                            text-decoration: none;
                            border-radius: 5px;
                            margin: 10px;
                            font-weight: bold;
                        }}
                        .download-btn:hover {{
                            background: #0056b3;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>🎥 Video Player</h1>
                        <video controls controlsList="nodownload">
                            <source src="{media_url}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                        <br>
                        <a href="{media_url}" class="download-btn" download>📥 Download Video</a>
                    </div>
                </body>
                </html>
                """
            elif media_type == 'audio':
                player_html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Audio Player</title>
                    <style>
                        body {{
                            margin: 0;
                            padding: 40px;
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            font-family: Arial, sans-serif;
                            text-align: center;
                        }}
                        .container {{
                            max-width: 600px;
                            margin: 0 auto;
                            background: rgba(255,255,255,0.1);
                            padding: 40px;
                            border-radius: 15px;
                            backdrop-filter: blur(10px);
                        }}
                        audio {{
                            width: 100%;
                            margin: 30px 0;
                        }}
                        .download-btn {{
                            display: inline-block;
                            padding: 12px 24px;
                            background: #28a745;
                            color: white;
                            text-decoration: none;
                            border-radius: 5px;
                            margin: 10px;
                            font-weight: bold;
                        }}
                        .download-btn:hover {{
                            background: #1e7e34;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>🎵 Audio Player</h1>
                        <audio controls controlsList="nodownload">
                            <source src="{media_url}" type="audio/mpeg">
                            Your browser does not support the audio tag.
                        </audio>
                        <br>
                        <a href="{media_url}" class="download-btn" download>📥 Download Audio</a>
                    </div>
                </body>
                </html>
                """
            else:
                player_html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>File Download</title>
                    <style>
                        body {{
                            margin: 0;
                            padding: 40px;
                            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                            color: white;
                            font-family: Arial, sans-serif;
                            text-align: center;
                        }}
                        .container {{
                            max-width: 600px;
                            margin: 0 auto;
                            background: rgba(255,255,255,0.1);
                            padding: 40px;
                            border-radius: 15px;
                            backdrop-filter: blur(10px);
                        }}
                        .download-btn {{
                            display: inline-block;
                            padding: 15px 30px;
                            background: #ff6b6b;
                            color: white;
                            text-decoration: none;
                            border-radius: 8px;
                            margin: 20px;
                            font-size: 18px;
                            font-weight: bold;
                        }}
                        .download-btn:hover {{
                            background: #ee5a52;
                            transform: translateY(-2px);
                            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>📁 File Download</h1>
                        <p>This file type cannot be played in the browser.</p>
                        <a href="{media_url}" class="download-btn" download>📥 Download File</a>
                    </div>
                </body>
                </html>
                """
            
            return player_html
        except Exception as e:
            return f"Error decoding URL: {str(e)}", 400

    @flask_app.route("/health")
    def health():
        return jsonify({"status": "ok", "service": "wasabi_bot_player"})

    return flask_app

def run_flask_server(host='0.0.0.0', port=8000):
    """Run Flask app"""
    flask_app = create_flask_app()
    logger.info(f"Starting Flask server on {host}:{port}")
    flask_app.run(host=host, port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    run_flask_server()
