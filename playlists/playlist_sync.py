import os
import json
import psycopg2
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from routes.rule_parser import build_track_query
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

def sync_playlist(playlist_config):
    playlist_id = playlist_config["spotify_id"]
    playlist_name = playlist_config["name"]
    rules = playlist_config["rules"]

    log_event("generate_playlist", f"🔁 Syncing playlist: {playlist_name}")

    log_event("generate_playlist", f"Raw rules input: {rules}")

    if isinstance(rules, str):
        try:
            rules = json.loads(rules)
        except Exception as e:
            log_event("generate_playlist", f"❌ Error decoding rules JSON for '{playlist_name}': {e}", level="error")
            return

    try:
        query = build_track_query(rules)
    except Exception as e:
        log_event("generate_playlist", f"❌ Error building/executing track query for '{playlist_name}': {e}", level="error")
        return

    try:
        conn = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
            sslmode='require'
        )
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        uris = [row[0] for row in rows]
        log_event("generate_playlist", f"🎧 Fetched {len(uris)} tracks for playlist '{playlist_name}'")
        cur.close()
        conn.close()
    except Exception as e:
        log_event("generate_playlist", f"❌ Database error for '{playlist_name}': {e}", level="error")
        return

    try:
        sp = get_spotify_client()
        sp.playlist_replace_items(playlist_id, uris[:100])
        if len(uris) > 100:
            for i in range(100, len(uris), 100):
                sp.playlist_add_items(playlist_id, uris[i:i+100])
        log_event("generate_playlist", f"✅ Successfully updated playlist: {playlist_name}")
    except Exception as e:
        log_event("generate_playlist", f"❌ Spotify update error for '{playlist_name}': {e}", level="error")