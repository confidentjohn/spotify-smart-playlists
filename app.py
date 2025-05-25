from flask import Flask, request, redirect
import os
import subprocess
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ─────────────────────────────────────────────────────
# Utilities for subprocess-triggered syncs (MOVED UP!)
# ─────────────────────────────────────────────────────
def run_script(script_name):
    try:
        result = subprocess.run(['python', script_name], capture_output=True, text=True)
        return f"<pre>{result.stdout or result.stderr}</pre>"
    except Exception as e:
        return f"<pre>Error: {str(e)}</pre>"

# ─────────────────────────────────────────────────────
# Spotify OAuth
# ─────────────────────────────────────────────────────
def get_spotify_oauth():
    return SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read"
    )

@app.route('/')
def index():
    return '<a href="/login">Login with Spotify</a>'

@app.route('/login')
def login():
    return redirect(get_spotify_oauth().get_authorize_url())

@app.route('/callback')
def callback():
    code = request.args.get('code')
    token_info = get_spotify_oauth().get_access_token(code)
    return f"✅ Refresh Token: <code>{token_info['refresh_token']}</code>"

@app.route('/run-tracker')
def run_tracker():
    return run_script('track_plays.py')

@app.route('/init-db')
def init_db():
    return run_script('init_db.py')

@app.route('/sync-albums')
def sync_albums():
    return run_script('sync_albums.py')

@app.route('/sync-liked-tracks')
def sync_liked_tracks():
    return run_script('sync_liked_tracks.py')

@app.route('/sync-library')
def sync_library():
    return run_script('sync_liked_tracks.py')

@app.route('/update-never-played-playlist')
def update_never_played_playlist():
    return run_script('update_playlist_never_played.py')


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
