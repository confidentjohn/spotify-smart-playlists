# utils/create_exclusions_playlist.py
import os
import psycopg2
from datetime import datetime
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection

def ensure_exclusions_playlist(sp):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT playlist_id FROM playlist_mappings WHERE slug = 'exclusions'")
        result = cur.fetchone()

        if result:
            log_event("init", "✅ Exclusions playlist already exists in DB.")
            return

        user = sp.current_user()
        playlist = sp.user_playlist_create(user["id"], "exclusions", public=False)
        playlist_url = playlist["external_urls"]["spotify"]

        cur.execute("""
            INSERT INTO playlist_mappings (slug, name, playlist_id, status, rules, track_count, last_synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            "exclusions",
            "Exclusions",
            playlist_url,
            "active",
            "{}",
            0,
            datetime.utcnow()
        ))
        conn.commit()

        log_event("init", f"🎯 Created exclusions playlist and added to DB: {playlist_url}")
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        log_event("init", f"❌ Error ensuring exclusions playlist: {e}", level="error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()