# routes/playlist_dashboard.py
import os
import psycopg2
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from utils.playlist_builder import create_and_store_playlist
from utils.logger import log_event
from playlists.playlist_sync import sync_playlist
import json
from utils.db_utils import get_db_connection

def user_has_synced_before(user_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # No user_id filter since the unified_tracks table doesn't have that column
        cur.execute("SELECT 1 FROM unified_tracks LIMIT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        log_event("initial_sync", f"❌ Error checking sync status: {e}", level="error")
        return False

def run_initial_syncs(user_id: int, is_initial=True):
    import subprocess

    full_job_sequence = [
        "sync_saved_albums.py",
        "sync_album_tracks.py",
        "sync_liked_tracks.py" if not is_initial else "sync_liked_tracks_full.py",
        "sync_artists.py",
        "check_track_availability.py",
        "sync_exclusions.py",
        "materialized_views.py",
        "materialized_metrics.py",
        "check_canonical_albums.py"
    ]

    # Check if exclusions playlist exists; create it if missing
    from utils.create_exclusions_playlist import ensure_exclusions_playlist
    from utils.spotify_auth import get_spotify_client
    try:
        ensure_exclusions_playlist(get_spotify_client())
        log_event("initial_sync", f"✅ Ensured exclusions playlist exists for user {user_id}")
    except Exception as e:
        log_event("initial_sync", f"❌ Failed to ensure exclusions playlist: {e}", level="error")

    full_job_sequence.append("sync_exclusions.py")
    full_job_sequence.append("materialized_views.py")

    for job in full_job_sequence:
        try:
            log_event("initial_sync", f"🚀 Starting {job} for user {user_id}")
            result = subprocess.run(
                ["python", f"api_syncs/{job}", "--user_id", str(user_id)],
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ, "PYTHONPATH": "."}
            )
            log_event("initial_sync", f"✅ Completed {job} for user {user_id}\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            log_event("initial_sync", f"❌ Failed {job} for user {user_id}\n{e.stderr}", level="error")


playlist_dashboard = Blueprint("playlist_dashboard", __name__)

@playlist_dashboard.route("/dashboard/playlists")
def dashboard_playlists():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT slug, name, status, track_count, last_synced_at, playlist_id, rules FROM playlist_mappings")
        playlists = cur.fetchall()
        playlists = [
            {
                "slug": row[0],
                "name": row[1],
                "status": row[2],
                "track_count": row[3],
                "last_synced_at": row[4],
                "playlist_id": row[5],
                "rules": row[6],
                "edit_url": url_for("playlist_dashboard.edit_playlist", slug=row[0])
            }
            for row in playlists
        ]
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

@playlist_dashboard.route("/dashboard/playlists/<slug>/edit", methods=["GET"])
def edit_playlist(slug):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT name, rules FROM playlist_mappings WHERE slug = %s", (slug,))
    row = cur.fetchone()
    if not row:
        return "Playlist not found", 404

    name, rules = row
    log_event("edit_playlist", f"🎯 Raw rules from DB for '{slug}': {rules}")
    try:
        if isinstance(rules, str):
            rules_data = json.loads(rules)
        elif isinstance(rules, dict):
            rules_data = rules
        else:
            rules_data = {}
        log_event("edit_playlist", f"🧩 Parsed rules_data for '{slug}': {rules_data}")
    except Exception as e:
        log_event("edit_playlist", f"❌ JSON parsing error for rules: {e}", level="error")
        rules_data = {}

    rules_data.setdefault("conditions", [])
    rules_data.setdefault("sort", [])
    rules_data.setdefault("limit", "")
    rules_data.setdefault("match", "all")

    return render_template(
        "edit_playlist.html",
        editing=True,
        slug=slug,
        name=name,
        limit=rules_data.get("limit", ""),
        sort_rules=rules_data.get("sort", []),
        match=rules_data.get("match", "all"),
        conditions=rules_data.get("conditions", [])
    )


# Route to trigger initial sync for the current user
@playlist_dashboard.route("/run-initial-sync", methods=["POST"])
@login_required
def run_initial_sync():
    user_id = current_user.get_id()
    is_initial = not user_has_synced_before(user_id)
    log_event("initial_sync", f"🔍 Determined is_initial={is_initial} for user {user_id}")
    log_event("initial_sync", f"🔔 Triggered sync for user {user_id}")
    run_initial_syncs(user_id, is_initial=is_initial)
    flash("✅ Initial sync completed successfully.")
    log_event("initial_sync", f"✅ Sync finished for user {user_id}")
    return redirect(url_for("home"))