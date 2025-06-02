from flask import Flask, request, redirect, session
from flask import render_template
from markupsafe import escape
import os
import subprocess
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ─────────────────────────────────────────────────────
# Smart Playlist Dashboard Overview
@app.route('/dashboard/playlists')
def dashboard_playlists():
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

        cur.execute("""
            SELECT
                name,
                track_count,
                last_synced_at,
                status
            FROM playlist_mappings
            ORDER BY name;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<pre>❌ DB Error: {e}</pre>"

    html = "<h2>🎵 Smart Playlists Dashboard</h2>"
    html += "<table border='1' cellpadding='5'>"
    html += "<tr><th>Name</th><th>Size</th><th>Last Run</th><th>Status</th><th>Actions</th></tr>"

    for row in rows:
        name, size, last_run, status = row
        last_run_display = last_run.strftime('%Y-%m-%d %H:%M') if last_run else "Never"
        html += f"<tr><td>{escape(name)}</td><td>{size}</td><td>{last_run_display}</td><td>{escape(status)}</td>"
        html += f"<td><a href='/dashboard/playlists/{escape(name)}'>View</a> | "
        html += f"<a href='/dashboard/playlists/{escape(name)}/edit'>Edit</a> | "
        html += f"<a href='/dashboard/playlists/{escape(name)}/sync'>Sync Now</a> | "
        html += (
            f"<form method='post' action='/dashboard/playlists/{escape(name)}/delete' style='display:inline;' "
            f"onsubmit=\"return confirm('Are you sure?')\">"
            f"<button type='submit'>🗑️ Delete</button></form>"
        )
        html += "</td></tr>"

    html += "</table>"
    html += "<p><a href='/dashboard/playlists/new'>➕ Create New Playlist</a></p>"
    html += "<p><a href='/'>⬅️ Back to Home</a></p>"
    return html

# ─────────────────────────────────────────────────────
def check_auth(request):
    expected = os.environ.get("ADMIN_KEY")
    if not expected:
        return False

    # Check session
    if session.get("is_admin"):
        return True

    # Allow initial access via ?key=
    secret = request.args.get("key")
    if secret == expected:
        session["is_admin"] = True
        return True

    return False

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

    # Parse query parameters
    script = request.args.get("script")
    level = request.args.get("level")
    sort = request.args.get("sort", "desc").lower()
    if sort not in ("asc", "desc"):
        sort = "desc"
    page = int(request.args.get("page", 1))
    page_size = 50
    offset = (page - 1) * page_size

    try:
        conn = psycopg2.connect(
            dbname=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ.get('DB_PORT', 5432)
        )
        cur = conn.cursor()

        # Build dynamic query
        where_clauses = []
        params = []

        if script:
            where_clauses.append("source = %s")
            params.append(script)
        if level:
            where_clauses.append("level = %s")
            params.append(level)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"""
            SELECT timestamp, source, level, message
            FROM logs
            {where_sql}
            ORDER BY timestamp {sort.upper()}
            LIMIT {page_size} OFFSET {offset}
        """

        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<pre>❌ DB Error: {e}</pre>"

    # Build HTML response
    html = f"""
    <h2>📜 Recent Logs</h2>
    <form method='get' action='/logs'>
      <label>Script: <input name='script' value='{escape(script or "")}'></label>
      <label>Level: <input name='level' value='{escape(level or "")}'></label>
      <label>Sort:
        <select name='sort'>
          <option value='desc' {"selected" if sort=="desc" else ""}>Newest first</option>
          <option value='asc' {"selected" if sort=="asc" else ""}>Oldest first</option>
        </select>
      </label>
      <input type='hidden' name='page' value='1'>
      <button type='submit'>Filter</button>
    </form>
    <p>Page {page}</p>
    """
    html += "<table border='1' cellpadding='5'><tr><th>Time</th><th>Script</th><th>Level</th><th>Message</th></tr>"
    for row in rows:
        html += "<tr>" + "".join(f"<td>{escape(str(col))}</td>" for col in row) + "</tr>"
    html += "</table>"

    # Navigation
    base_url = "/logs?"
    if script:
        base_url += f"script={script}&"
    if level:
        base_url += f"level={level}&"
    if sort:
        base_url += f"sort={sort}&"
    
    html += f"<p><a href='{base_url}page={page + 1}'>▶️ Next</a>"
    if page > 1:
        html += f" | <a href='{base_url}page={page - 1}'>◀️ Prev</a>"
    html += "</p>"

    html += "<p><a href='/logout'>🚪 Logout</a></p>"
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

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

# ─────────────────────────────────────────────────────
# Create a new playlist (form and POST)
import requests
from spotipy import Spotify

@app.route('/dashboard/playlists/new', methods=['GET', 'POST'])
def create_playlist():
    if not check_auth(request): return "❌ Unauthorized", 403

    if request.method == 'POST':
        print("📝 Form submission received")
        print(f"Name: {request.form.get('name')}")
        print(f"Limit: {request.form.get('limit')}")
        print(f"Rules JSON: {request.form.get('rules_json')}")
        name = request.form.get('name')
        if not name:
            return "❌ Playlist name is required", 400

        # Create Spotify playlist
        token_response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
                "client_id": os.environ["SPOTIFY_CLIENT_ID"],
                "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
            }
        )
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]

        sp = Spotify(auth=access_token)
        user = sp.current_user()
        playlist = sp.user_playlist_create(user['id'], name)

        playlist_url = playlist["external_urls"]["spotify"]
        playlist_id = playlist["id"]

        limit_raw = request.form.get("limit")
        limit = 9000 if limit_raw == "no_limit" else int(limit_raw)
        rules_json = request.form.get("rules_json") or "{}"

        print(f"📤 Inserting playlist: slug={name.lower().replace(' ', '_')}, name={name}, playlist_id={playlist_url}, status=active, limit={limit}")
        print(f"📄 Rules JSON: {rules_json}")

        # Store in DB
        try:
            conn = psycopg2.connect(
                dbname=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                host=os.environ["DB_HOST"],
                port=os.environ.get("DB_PORT", 5432),
            )
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO playlist_mappings (slug, name, playlist_id, status, rules_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                name.lower().replace(" ", "_"),
                name,
                playlist_url,
                'active',
                limit,
                rules_json
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            return f"<pre>❌ DB Error: {e}</pre>"

        return redirect("/dashboard/playlists")

    return render_template("create_playlist.html")

# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
# ─────────────────────────────────────────────────────
# Delete a playlist
@app.route('/dashboard/playlists/<path:slug>/delete', methods=['POST'])
def delete_playlist(slug):
    if not check_auth(request): return "❌ Unauthorized", 403

    import requests
    from spotipy import Spotify
    try:
        # Look up playlist_id from DB
        conn = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
        )
        cur = conn.cursor()
        cur.execute("SELECT playlist_id FROM playlist_mappings WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return "❌ Playlist not found", 404
        playlist_url = row[0]
        playlist_id = playlist_url.split("/")[-1].split("?")[0]

        # Delete from Spotify
        token_response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
                "client_id": os.environ["SPOTIFY_CLIENT_ID"],
                "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
            }
        )
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]
        sp = Spotify(auth=access_token)
        sp.current_user_unfollow_playlist(playlist_id)

        # Delete from DB
        cur.execute("DELETE FROM playlist_mappings WHERE slug = %s", (slug,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<pre>❌ Error: {e}</pre>"

    return redirect("/dashboard/playlists")