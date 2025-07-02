from flask import Flask, request, redirect, session
from flask import render_template
from flask import redirect, url_for, request, flash
from flask_login import LoginManager, login_user, logout_user, login_required, UserMixin
from app.users import validate_user
from markupsafe import escape
import os
import subprocess
import psycopg2
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from app import startup
import requests
from routes import playlist_dashboard
from routes.create_admin import create_admin_bp
from utils.db_utils import get_db_connection

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
    allowed_routes = {"login", "callback", "static", "create_admin"}
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

sp = Spotify(auth=get_access_token())

# ─────────────────────────────────────────────────────

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


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        if validate_user(username, password):
            user = User(username)
            login_user(user)
            return redirect(url_for("home"))
        flash("Invalid credentials", "error")
    return render_template("login.html")

# ─────────────────────────────────────────────────────
# Spotify OAuth login route
@app.route("/login/spotify")
def login_spotify():
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

    if not refresh_token:
        return "❌ No refresh token received", 400

    # You can store tokens here in session or database
    print("Access Token:", access_token)
    print("Refresh Token:", refresh_token)

    flash("Spotify authentication successful!", "success")
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