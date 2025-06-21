# routes/playlist_dashboard.py
import os
import psycopg2
from flask import Blueprint, render_template, request, redirect, url_for
from utils.playlist_builder import create_and_store_playlist
from markupsafe import escape
from utils.auth import check_auth
from utils.logger import log_event
from playlists.playlist_sync import sync_playlist

playlist_dashboard = Blueprint("playlist_dashboard", __name__)

@playlist_dashboard.route("/dashboard/playlists")
def dashboard_playlists():
    if not check_auth(request):
        return "❌ Unauthorized", 403
    try:
        conn = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432)
        )
        cur = conn.cursor()
        cur.execute("SELECT slug, name, status, track_count, last_synced_at FROM playlist_mappings")
        playlists = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<pre>❌ DB Error: {e}</pre>"

    return render_template("dashboard_playlists.html", playlists=playlists)

@playlist_dashboard.route("/dashboard/create-playlist", methods=["GET", "POST"])
def create_playlist():
    if request.method == "POST":
        name = request.form.get("name")
        limit = request.form.get("limit")
        rules = request.form.get("rules_json", "{}")
        try:
            result = create_and_store_playlist(name, rules_json=rules, limit=int(limit) if limit else None)
            log_event("playlist_dashboard", f"✅ Created playlist: {result['name']}")

            try:
                sync_playlist(result["slug"])
                log_event("playlist_dashboard", f"✅ Synced playlist: {result['slug']}")
            except Exception as sync_error:
                log_event("playlist_dashboard", f"❌ Failed to sync playlist {result['slug']}: {sync_error}", level="error")

            return redirect(url_for("playlist_dashboard.dashboard_playlists"))
        except Exception as e:
            log_event("playlist_dashboard", f"❌ Error creating playlist: {e}", level="error")
            return f"<pre>❌ Error creating playlist: {e}</pre>"

    return render_template("create_playlist.html")