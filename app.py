from flask import Flask, request, redirect
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

@app.route('/')
def index():
    return '<a href="/login">Login with Spotify</a>'

@app.route('/login')
def login():
    sp_oauth = SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read"
    )
    return redirect(sp_oauth.get_authorize_url())

@app.route('/callback')
def callback():
    sp_oauth = SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read"
    )
    code = request.args.get('code')
    token_info = sp_oauth.get_access_token(code)
    return f"✅ Refresh Token: <code>{token_info['refresh_token']}</code>"



import subprocess

@app.route('/run-tracker')
def run_tracker():
    try:
        result = subprocess.run(['python', 'track_plays.py'], capture_output=True, text=True)
        return f"<pre>{result.stdout or result.stderr}</pre>"
    except Exception as e:
        return f"Error: {str(e)}"


@app.route('/init-db')
def init_db():
    import subprocess
    result = subprocess.run(['python', 'init_db.py'], capture_output=True, text=True)
    return f"<pre>{result.stdout or result.stderr}</pre>"

@app.route('/sync-library')
def sync_library():
    import subprocess
    result = subprocess.run(['python', 'sync_library.py'], capture_output=True, text=True)
    return f"<pre>{result.stdout or result.stderr}</pre>"




if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


# Trigger redeploy
