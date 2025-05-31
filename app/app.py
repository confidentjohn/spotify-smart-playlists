from flask import Flask, request, redirect
from markupsafe import escape
import os
import subprocess
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ─────────────────────────────────────────────────────
def check_auth(request):
    secret = request.args.get("key")
    expected = os.environ.get("ADMIN_KEY")
    return secret == expected

def run_script(script_name):
    print(f"🔧 Running {script_name}", flush=True)
    try:
        result = subprocess.run(['python', script_name], capture_output=True, text=True)
        return f"<pre>{result.stdout or result.stderr}</pre>"
    except Exception as e:
        return f"<pre>Error: {str(e)}</pre>"

def get_spotify_oauth():
    return SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read playlist-modify-private playlist-modify-public"
    )

# ─────────────────────────────────────────────────────
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
@app.route('/logs')
def view_logs():
    if not check_auth(request): return "❌ Unauthorized", 403

    try:
        conn = psycopg2.connect(
            dbname=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ.get('DB_PORT', 5432)
        )
        cur = conn.cursor()
        cur.execute("SELECT timestamp, source AS script_name, level, message FROM logs ORDER BY timestamp DESC LIMIT 100")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<pre>❌ DB Error: {e}</pre>"

    html = "<h2>📜 Recent Logs</h2><table border='1' cellpadding='5'><tr><th>Time</th><th>Script</th><th>Level</th><th>Message</th></tr>"
    for row in rows:
        html += "<tr>" + "".join(f"<td>{escape(str(col))}</td>" for col in row) + "</tr>"
    html += "</table>"
    return html

# ─────────────────────────────────────────────────────
@app.route('/init-db')
def init_db():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('db/init_db.py')

@app.route('/sync-saved-albums')
def sync_saved_albums():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('api_syncs/sync_saved_albums.py')

@app.route('/sync-album-tracks')
def sync_album_tracks():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('api_syncs/sync_album_tracks.py')    

@app.route('/sync-liked-tracks')
def sync_liked_tracks():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('api_syncs/sync_liked_tracks.py')

@app.route('/run-tracker')
def run_tracker():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('api_syncs/track_plays.py')   

@app.route('/check-track-availability')
def check_track_availability():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('api_syncs/update_track_availability.py')

@app.route('/update-never-played-playlist')
def update_never_played_playlist():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_never_played.py')

@app.route('/update-played-once-playlist')
def update_played_once_playlist():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_played_once.py')

@app.route('/update-oldest-played-playlist')
def update_oldest_played_playlist():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_oldest_played.py')

@app.route('/update-playlist-most-played')
def update_playlist_most_played():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_most_played.py')

@app.route('/update-playlist-loved-added-last-30-days')
def update_playlist_loved_added_last_30_days():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_loved_added_last_30_days.py')

@app.route('/update-playlist-never-played-new-tracks')
def update_playlist_never_played_new_tracks():
    if not check_auth(request): return "❌ Unauthorized", 403
    return run_script('playlists/update_playlist_never_played_new_tracks.py')

# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
