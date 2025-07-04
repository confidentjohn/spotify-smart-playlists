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
import requests
from routes import playlist_dashboard
from routes.create_admin import create_admin_bp
from utils.db_utils import get_db_connection


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")  # or your preferred secure method
app.register_blueprint(playlist_dashboard)
app.register_blueprint(create_admin_bp)

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
    can_sync = has_refresh_token(user_id) if user_id else False
    print(f"[DEBUG] user_id={user_id}, can_sync={can_sync}", flush=True)
    return render_template("home.html", can_sync=can_sync)


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

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return "❌ No authorization code provided", 400

    try:
        token_info = get_spotify_oauth().get_access_token(code)
    except Exception as e:
        return f"❌ Token exchange failed: {e}", 400

    refresh_token = token_info.get("refresh_token")
    access_token = token_info.get("access_token")

    if not refresh_token or not access_token:
        return "❌ Missing Spotify token(s)", 400

    # Use access token to get Spotify user ID
    sp = get_spotify_client(access_token)
    spotify_user = sp.current_user()
    spotify_user_id = spotify_user["id"]

    # Get current app user from session
    from flask_login import current_user
    app_user_id = current_user.id if current_user.is_authenticated else session.get("pending_user")

    # Store Spotify info in DB
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE users
            SET spotify_user_id = %s, spotify_refresh_token = %s
            WHERE username = %s
        """, (spotify_user_id, refresh_token, app_user_id))
        conn.commit()
        cur.close()
        conn.close()
        flash("Spotify authentication successful!", "success")
    except Exception as e:
        import traceback
        print("❌ Exception in /callback DB update:")
        traceback.print_exc()
        return f"&lt;pre&gt;❌ Failed to save Spotify info: {e}&lt;/pre&gt;", 500

    # Create exclusions playlist if not already present
    try:
        from utils.create_exclusions_playlist import ensure_exclusions_playlist
        ensure_exclusions_playlist(sp)
    except Exception as e:
        import traceback
        print("❌ Failed to create exclusions playlist:")
        traceback.print_exc()

    return redirect(url_for("home"))

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
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)