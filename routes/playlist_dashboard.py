# routes/playlist_dashboard.py
import os
import psycopg2
from flask import Blueprint, render_template, request, redirect, url_for, flash
from utils.playlist_builder import create_and_store_playlist
from markupsafe import escape
from utils.auth import check_auth
from utils.logger import log_event
from playlists.playlist_sync import sync_playlist
import json

playlist_dashboard = Blueprint("playlist_dashboard", __name__)

def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432)
    )

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
        playlists = [
            {
                "slug": row[0],
                "name": row[1],
                "status": row[2],
                "track_count": row[3],
                "last_synced_at": row[4],
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

@playlist_dashboard.route("/dashboard/playlists/<slug>/edit", methods=["GET", "POST"])
def edit_playlist(slug):
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        name = request.form.get("name")
        limit = request.form.get("limit")
        match = request.form.get("match")

        fields = request.form.getlist("field[]")
        operators = request.form.getlist("operator[]")
        values = request.form.getlist("value[]")
        is_group_flags = request.form.getlist("is_group[]")
        group_matches = request.form.getlist("group_match[]")

        sort_fields = request.form.getlist("sort_by[]")
        sort_directions = request.form.getlist("sort_direction[]")

        conditions = []
        i = 0
        group_index = 0

        while i < len(fields):
            if i < len(is_group_flags) and is_group_flags[i] == "true":
                if group_index < len(group_matches):
                    group_match = group_matches[group_index]
                    group_index += 1
                else:
                    group_match = "any"

                group_conditions = []
                while i < len(fields) and is_group_flags[i] == "true":
                    group_conditions.append({
                        "field": fields[i],
                        "operator": operators[i],
                        "value": values[i]
                    })
                    i += 1

                conditions.append({
                    "match": group_match,
                    "conditions": group_conditions
                })
            else:
                conditions.append({
                    "field": fields[i],
                    "operator": operators[i],
                    "value": values[i]
                })
                i += 1

        sort = []
        for s_field, s_dir in zip(sort_fields, sort_directions):
            if s_field:
                sort.append({"by": s_field, "direction": s_dir})

        rules = {
            "sort": sort,
            "limit": int(limit) if limit else None,
            "match": match,
            "conditions": conditions
        }

        log_event("edit_playlist", f"📝 Saving rules for '{slug}': {json.dumps(rules)}")
        cur.execute(
            "UPDATE playlist_mappings SET name = %s, rules = %s WHERE slug = %s",
            (name, json.dumps(rules), slug)
        )
        conn.commit()
        flash("Playlist updated successfully!", "success")
        return redirect(url_for("playlist_dashboard.edit_playlist", slug=slug))

    cur.execute("SELECT name, rules FROM playlist_mappings WHERE slug = %s", (slug,))
    row = cur.fetchone()
    if not row:
        return "Playlist not found", 404

    name, rules = row
    # Log raw rules from DB
    log_event("edit_playlist", f"🎯 Raw rules from DB for '{slug}': {rules}")
    try:
        if isinstance(rules, str):
            rules_data = json.loads(rules)
        elif isinstance(rules, dict):
            rules_data = rules
        else:
            rules_data = {}
        # Log parsed rules_data
        log_event("edit_playlist", f"🧩 Parsed rules_data for '{slug}': {rules_data}")
    except Exception as e:
        log_event("edit_playlist", f"❌ JSON parsing error for rules: {e}", level="error")
        rules_data = {}

    # Ensure default structure and prevent index errors
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