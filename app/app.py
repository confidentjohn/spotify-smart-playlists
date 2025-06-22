from flask import Flask, request, redirect, session
from flask import render_template
from markupsafe import escape
import os
import subprocess
import psycopg2
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from utils.create_exclusions_playlist import ensure_exclusions_playlist
import requests
from routes import playlist_dashboard

# ─────────────────────────────────────────────────────
# Retrieve a fresh Spotify access token using the refresh token
def get_access_token():
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
    return token_response.json()["access_token"]

app = Flask(__name__)
app.register_blueprint(playlist_dashboard)
sp = Spotify(auth=get_access_token())
ensure_exclusions_playlist(sp)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ─────────────────────────────────────────────────────
from utils.auth import check_auth

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

@app.route("/")
def home():
    return render_template("home.html")

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

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)