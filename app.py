from flask import Flask, request, redirect
import os
import subprocess
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ─────────────────────────────────────────────────────
# 🔐 Token checker with debug
# ─────────────────────────────────────────────────────
def is_authorized(req):
    provided = req.args.get("token")
    expected = os.environ.get("ACCESS_TOKEN")

    print(f"🔐 DEBUG: Provided token = {provided}, Expected token = {expected}", flush=True)

    return provided == expected

# ─────────────────────────────────────────────────────
# 🛠 Script runner with optional auth
# ─────────────────────────────────────────────────────
def run_script(script_name, require_auth=False):
    print(f"🔧 Running {script_name} | Auth required: {require_auth}", flush=True)

    if require_auth:
        print("🔐 Authorization required. Checking token...", flush=True)
        if not is_authorized(request):
            print("❌ Token check failed", flush=True)
            return "❌ Unauthorized", 401
        else:
            print("✅ Token check passed", flush=True)

    try:
        result = subprocess.run(['python', script_name], capture_output=True, text=True)
        return f"<pre>{result.stdout or result.stderr}</pre>"
    except Exception as e:
        return f"<pre>Error: {str(e)}</pre>"

# ─────────────────────────────────────────────────────
# 🎧 Spotify OAuth Setup
# ─────────────────────────────────────────────────────
def get_spotify_oauth():
    return SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read playlist-modify-private playlist-modify-public"
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

# ─────────────────────────────────────────────────────
# 🔁 Script Endpoints (some protected)
# ─────────────────────────────────────────────────────
@app.route('/run-tracker')
def run_tracker():
    return run_script('track_plays.py')

@app.route('/init-db')
def init_db():
    return run_script('init_db.py', require_auth=True)

@app.route('/sync-albums')
def sync_albums():
    return run_script('sync_albums.py', require_auth=True)

@app.route('/sync-liked-tracks')
def sync_liked_tracks():
    return run_script('sync_liked_tracks.py', require_auth=True)

@app.route('/sync-library')
def sync_library():
    return run_script('sync_liked_tracks.py', require_auth=True)

@app.route('/update-never-played-playlist')
def update_never_played_playlist():
    return run_script('update_playlist_never_played.py')

@app.route('/update-played-once-playlist')
def update_played_once_playlist():
    return run_script('update_playlist_played_once.py')

@app.route('/update-oldest-played-playlist')
def update_oldest_played_playlist():
    return run_script('update_playlist_oldest_played.py')

@app.route('/update-playlist-most-played')
def update_playlist_most_played():
    return run_script('update_playlist_most_played.py')

@app.route('/update-playlist-loved-added-last-30-days')
def update_playlist_loved_added_last_30_days():
    return run_script('update_playlist_loved_added_last_30_days.py')

@app.route('/debug-env')
def debug_env():
    token_env = os.environ.get("ACCESS_TOKEN")
    return f"🔍 ACCESS_TOKEN from os.environ: {token_env or 'Not Set'}"


# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
