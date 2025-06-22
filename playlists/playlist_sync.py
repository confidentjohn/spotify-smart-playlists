import os
import json
import psycopg2
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from routes.rule_parser import build_track_query
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client
from datetime import datetime

def sync_playlist(slug):
    log_event("generate_playlist", f"🔁 Starting sync for playlist slug: '{slug}'")
    try:
        conn = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432)
        )
        cur = conn.cursor()

        cur.execute("SELECT name, playlist_id, rules, is_dynamic FROM playlist_mappings WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Playlist with slug '{slug}' not found")

        name, playlist_url, rules_json, is_dynamic = row
        playlist_id = playlist_url.split("/")[-1]

        sp = get_spotify_client()
        try:
            playlist_data = sp.playlist(playlist_id)
            owner_id = playlist_data["owner"]["id"]
            current_user_id = sp.current_user()["id"]
            if owner_id != current_user_id:
                log_event("generate_playlist", f"🗑 Playlist '{playlist_id}' no longer owned by user. Deleting from DB.")
                cur.execute("DELETE FROM playlist_mappings WHERE slug = %s", (slug,))
                conn.commit()
                return
        except Exception as e:
            log_event("generate_playlist", f"🗑 Playlist '{playlist_id}' not accessible. Deleting from DB. Error: {e}")
            cur.execute("DELETE FROM playlist_mappings WHERE slug = %s", (slug,))
            conn.commit()
            return

        if not is_dynamic:
            log_event("generate_playlist", f"⏭ Skipped legacy playlist '{name}' (not dynamic)")
            return

        if slug == "exclusions":
            log_event("generate_playlist", "⏭ Skipped 'exclusions' playlist (manually managed)")
            return

        try:
            log_event("generate_playlist", f"📥 Raw rules_json for '{slug}': {rules_json} (type: {type(rules_json)})")
            if isinstance(rules_json, dict):
                rules = rules_json
            else:
                rules = json.loads(rules_json or "{}")
            log_event("generate_playlist", f"📋 Successfully loaded rules for '{slug}': {rules} (type: {type(rules)})")
        except Exception as e:
            log_event("generate_playlist", f"❌ Failed to parse rules for '{slug}': {e} — rules_json was: {rules_json}", level="error")
            return

        try:
            query = build_track_query(rules)
            log_event("generate_playlist", f"🔍 Running query: {query}")
            log_event("generate_playlist", f"🛠 SQL Query: {query} | Params: []")
            cur.execute(query)
            rows = cur.fetchall()
            log_event("generate_playlist", f"📊 Fetched rows: {len(rows)} | Sample: {rows[:5]}")
            if not rows or not all(isinstance(row, (list, tuple)) and len(row) > 0 for row in rows):
                log_event("generate_playlist", f"❌ Fetched rows are empty or malformed: {rows}", level="error")
                return
            track_uris = [row[0] for row in rows if row and row[0]]
            log_event("generate_playlist", f"📦 Track URIs fetched: {track_uris}")
        except Exception as query_error:
            log_event("generate_playlist", f"❌ Error building/executing track query for '{slug}': {query_error} — rules: {rules}", level="error")
            return

        if not track_uris:
            log_event("generate_playlist", f"⚠️ No tracks found for '{slug}' — skipping playlist update.")
            return

        log_event("generate_playlist", f"🎧 Retrieved {len(track_uris)} tracks for '{slug}'")

        sp = get_spotify_client()
        user = sp.current_user()
        sp.user_playlist_replace_tracks(user["id"], playlist_id, [])
        for i in range(0, len(track_uris), 100):
            sp.playlist_add_items(playlist_id, track_uris[i:i + 100])

        cur.execute("UPDATE playlist_mappings SET track_count = %s, last_synced_at = %s WHERE slug = %s", (len(track_uris), datetime.utcnow(), slug))
        conn.commit()

        log_event("generate_playlist", f"✅ Synced {len(track_uris)} tracks to playlist '{name}'")

    except Exception as e:
        log_event("generate_playlist", f"❌ Failed to sync playlist '{slug}': {e}", level="error")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()