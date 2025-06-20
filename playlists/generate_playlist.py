import os
import psycopg2
import json
from datetime import datetime
from spotipy import Spotify
import requests
from utils.logger import log_event
from routes.rule_parser import build_track_query

def get_spotify_client():
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
    return Spotify(auth=token_response.json()["access_token"])

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

        if not is_dynamic:
            log_event("generate_playlist", f"⏭ Skipped legacy playlist '{name}' (not dynamic)")
            return

        if slug == "exclusions":
            log_event("generate_playlist", "⏭ Skipped 'exclusions' playlist (manually managed)")
            return

        playlist_id = playlist_url.split("/")[-1]
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
            query_result = build_track_query(rules)
            if isinstance(query_result, tuple):
                query, params = query_result
            else:
                query = query_result
                params = []
            log_event("generate_playlist", f"🔍 Running query: {query} with params: {params}")
            log_event("generate_playlist", f"🛠 SQL Query: {query} | Params: {params}")
            cur.execute(query, params)
            try:
                rows = cur.fetchall()
                log_event("generate_playlist", f"📊 Fetched rows: {len(rows)} | Sample: {rows[:5]}")
                track_ids = [row[0] for row in rows]
            except Exception as fetch_err:
                log_event("generate_playlist", f"❌ Failed to fetch rows or unpack results: {fetch_err}", level="error")
                return
            log_event("generate_playlist", f"📦 Track IDs fetched: {track_ids}")
        except Exception as query_error:
            log_event("generate_playlist", f"❌ Error building/executing track query for '{slug}': {query_error} — rules: {rules}", level="error")
            return

        if not track_ids:
            log_event("generate_playlist", f"⚠️ No tracks found for '{slug}' — skipping playlist update.")
            return
        log_event("generate_playlist", f"🎧 Retrieved {len(track_ids)} tracks for '{slug}'")

        sp = get_spotify_client()
        # TODO: Support syncing more than 100 tracks by batching
        log_event("generate_playlist", f"📤 Sending {min(len(track_ids), 100)} tracks to Spotify for '{slug}'")
        sp.playlist_replace_items(playlist_id, track_ids[:100])  # truncate to 100 tracks max

        cur.execute("UPDATE playlist_mappings SET track_count = %s, last_synced_at = %s WHERE slug = %s", (len(track_ids), datetime.utcnow(), slug))
        conn.commit()

        log_event("generate_playlist", f"✅ Synced {len(track_ids)} tracks to playlist '{name}'")

    except Exception as e:
        log_event("generate_playlist", f"❌ Failed to sync playlist '{slug}': {e}", level="error")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()