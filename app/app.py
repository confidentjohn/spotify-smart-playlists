from flask import Flask, request, redirect, session
from flask import render_template
from flask import redirect, url_for, request, flash
from flask_login import LoginManager, login_user, logout_user, login_required, UserMixin
from app.users import validate_user
from app.users import get_user_by_username
from app.users import has_refresh_token
from markupsafe import escape
import os
import subprocess
import psycopg2
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from app import startup
from utils.spotify_auth import get_spotify_client, get_spotify_oauth
from utils.db_utils import get_db_connection
from utils.logger import log_event
import requests
from routes import playlist_dashboard
from routes.create_admin import create_admin_bp
from routes.metrics import metrics_bp



app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")  # or your preferred secure method
app.register_blueprint(playlist_dashboard)
app.register_blueprint(create_admin_bp)
app.register_blueprint(metrics_bp)


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ─────────────────────────────────────────────────────
# Enforce login on all routes by default, except allowed ones
from flask_login import current_user

@app.before_request
def require_login_for_all_routes():
    allowed_routes = {"login", "callback", "static", "setup.create_admin"}
    if request.endpoint and any(request.endpoint.startswith(route) for route in allowed_routes):
        return
    if not current_user.is_authenticated:
        return redirect(url_for("login", next=request.path))

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)


# ─────────────────────────────────────────────────────

def run_script(script_name):
    print(f"🔧 Running {script_name}", flush=True)
    try:
        result = subprocess.run(['python', script_name], capture_output=True, text=True)
        return f"<pre>{result.stdout or result.stderr}</pre>"
    except Exception as e:
        return f"<pre>Error: {str(e)}</pre>"


# ─────────────────────────────────────────────────────

@app.route("/")
def home():
    from flask_login import current_user
    user_id = current_user.get_id() if current_user.is_authenticated else None
    can_sync = bool(os.environ.get("SPOTIFY_REFRESH_TOKEN"))

    # Determine if unified_tracks has any records at all
    is_first_sync = True
    if user_id:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM unified_tracks LIMIT 1")
            is_first_sync = cur.fetchone() is None
            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ Failed unified_tracks check: {e}", flush=True)

    print(f"[DEBUG] user_id={user_id}, can_sync={can_sync}, is_first_sync={is_first_sync}", flush=True)
    return render_template("home.html", can_sync=can_sync, is_first_sync=is_first_sync)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        if validate_user(username, password):
            user_data = get_user_by_username(username)
            user = User(user_data["id"])
            login_user(user)
            return redirect(url_for("home"))
        flash("Invalid credentials", "error")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    user_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    show_create_admin_link = user_count == 0
    return render_template("login.html", show_create_admin_link=show_create_admin_link)

# ─────────────────────────────────────────────────────
# Spotify OAuth login route
@app.route("/login/spotify")
def login_spotify():
    from flask_login import current_user
    if current_user.is_authenticated:
        session["pending_user"] = current_user.id
    auth_url = get_spotify_oauth().get_authorize_url()
    return redirect(auth_url)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Missing Spotify code", 400

    token_info = get_spotify_oauth().get_access_token(code)
    refresh_token = token_info.get("refresh_token")

    if not refresh_token:
        return "Failed to get refresh token from Spotify", 400

    log_event("auth", "info", "OAuth success. Displaying refresh token.")

    return render_template("show_refresh_token.html", refresh_token=refresh_token)

# ─────────────────────────────────────────────────────
@app.route('/logs')
def view_logs():
    from urllib.parse import urlencode

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
        conn = get_db_connection()
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

    # Build next and previous page URLs
    base_params = {}
    if script:
        base_params['script'] = script
    if level:
        base_params['level'] = level
    if sort:
        base_params['sort'] = sort

    next_params = base_params.copy()
    next_params['page'] = page + 1
    next_url = url_for('view_logs') + '?' + urlencode(next_params)

    prev_params = base_params.copy()
    prev_params['page'] = max(page - 1, 1)
    prev_url = url_for('view_logs') + '?' + urlencode(prev_params)

    return render_template("logs.html", rows=rows, page=page, script=script, level=level, sort=sort, next_url=next_url, prev_url=prev_url)

@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))

# ─────────────────────────────────────────────────────
from utils.create_exclusions_playlist import ensure_exclusions_playlist

# ─────────────────────────────────────────────────────
if os.environ.get("SPOTIFY_REFRESH_TOKEN"):
    try:
        ensure_exclusions_playlist(get_spotify_client())
        log_event("startup", "✅ Ensured exclusions playlist exists")
    except Exception as e:
        log_event("startup", f"❌ Failed to ensure exclusions playlist: {e}", level="error")
else:
    log_event("startup", "⚠️ Skipping exclusions playlist check. No SPOTIFY_REFRESH_TOKEN found.", level="warning")

from utils.diagnostics import get_duplicate_album_track_counts, get_fuzzy_matched_plays

@app.route("/diagnostics")
def diagnostics():
    duplicates = get_duplicate_album_track_counts()
    fuzzy_matches = get_fuzzy_matched_plays()
    return render_template("diagnostics.html", duplicates=duplicates, fuzzy_matches=fuzzy_matches)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)