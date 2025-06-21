import os
import psycopg2
from datetime import datetime
import requests
from spotipy import Spotify
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

def create_and_store_playlist(name, rules_json="{}", limit=None):
    """
    Creates a new Spotify playlist and stores metadata in the playlist_mappings table.

    Args:
        name (str): Playlist name.
        rules_json (str): JSON-encoded rules string.
        limit (int or None): Playlist limit; if None, defaults to 9000.
    
    Returns:
        dict: Playlist metadata including name, id, and URL.
    """
    try:
        sp = get_spotify_client()
        user = sp.current_user()
        playlist = sp.user_playlist_create(user['id'], name)
        playlist_url = playlist["external_urls"]["spotify"]

        slug = name.lower().replace(" ", "_")
        track_limit = 9000 if limit is None else limit

        # Insert into DB
        conn = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO playlist_mappings (slug, name, playlist_id, status, rules, track_count, last_synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            slug,
            name,
            playlist_url,
            'active',
            rules_json,
            0,
            datetime.utcnow()
        ))
        conn.commit()

        log_event("playlist_builder", f"✅ Created playlist and saved to DB: {playlist_url}")
        return {
            "slug": slug,
            "name": name,
            "playlist_url": playlist_url,
            "playlist_id": playlist["id"]
        }

    except Exception as e:
        log_event("playlist_builder", f"❌ Error creating/storing playlist: {e}", level="error")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()