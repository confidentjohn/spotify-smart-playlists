import os
import sys
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from dateutil import parser
import datetime

# ─────────────────────────────────────────────
# Fix import path for utils
# ─────────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

# ─────────────────────────────────────────────
# Safe Spotify API Wrapper
# ─────────────────────────────────────────────
def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                retries += 1
                log_event("sync_saved_albums", f"Rate limit hit. Retry #{retries} in {retry_after}s")
                time.sleep(retry_after)
            else:
                log_event("sync_saved_albums", f"Spotify error: {e}", level="error")
                raise

# ─────────────────────────────────────────────
# Setup Spotify + DB connections
# ─────────────────────────────────────────────
sp = get_spotify_client()

from utils.db_utils import get_db_connection

conn = get_db_connection()

cur = conn.cursor()

limit = 50
offset = 0
current_album_ids = set()

log_event("sync_saved_albums", "Starting saved albums sync")

# ─────────────────────────────────────────────
# Check Spotify and local saved album counts, exit early if up to date
# ─────────────────────────────────────────────
initial_result = safe_spotify_call(sp.current_user_saved_albums, limit=1)
spotify_total = initial_result['total']
log_event("sync_saved_albums", f"📊 Spotify reports {spotify_total} saved albums")

cur.execute("SELECT COUNT(*) FROM albums WHERE is_saved = TRUE")
local_total = cur.fetchone()[0]
log_event("sync_saved_albums", f"📁 Local DB has {local_total} saved albums")

if spotify_total == local_total:
    log_event("sync_saved_albums", "✅ Saved albums are up to date — skipping sync.")
    cur.close()
    conn.close()
    sys.exit(0)

# ─────────────────────────────────────────────
# Sync saved albums from Spotify
# ─────────────────────────────────────────────
while True:
    results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=offset)
    items = results['items']
    if not items:
        break

    for item in items:
        album = item['album']
        album_id = album['id']
        current_album_ids.add(album_id)
        added_at = item.get('added_at')

        # Extract new album data
        album_type = album.get('album_type')
        album_image_url = album['images'][0]['url'] if album.get('images') else None
        artist_id = album['artists'][0]['id']

        cur.execute("""
            INSERT INTO albums (id, name, artist, artist_id, release_date, total_tracks, is_saved, added_at, tracks_synced, album_type, album_image_url)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, FALSE, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET is_saved = TRUE,
                added_at = EXCLUDED.added_at,
                artist_id = EXCLUDED.artist_id,
                album_type = EXCLUDED.album_type,
                album_image_url = EXCLUDED.album_image_url;
        """, (
            album_id,
            album['name'],
            album['artists'][0]['name'],
            artist_id,
            album.get('release_date'),
            album.get('total_tracks'),
            added_at,
            album_type,
            album_image_url
        ))

    offset += len(items)
    if len(items) < limit:
        break

log_event("sync_saved_albums", f"{len(current_album_ids)} saved albums synced")

# ─────────────────────────────────────────────
# Mark removed albums & cleanup
# ─────────────────────────────────────────────
cur.execute("""
    UPDATE albums 
    SET is_saved = FALSE, tracks_synced = FALSE 
    WHERE id NOT IN %s
""", (tuple(current_album_ids),))

log_event("sync_saved_albums", "Cleaning up removed albums with no valid tracks")
cur.execute("""
    SELECT id FROM albums
    WHERE is_saved = FALSE
      AND id NOT IN (SELECT DISTINCT album_id FROM tracks WHERE from_album = TRUE)
""")
albums_to_remove = cur.fetchall()

for (album_id,) in albums_to_remove:
    log_event("sync_saved_albums", f"Removing album and orphaned tracks: {album_id}")
    cur.execute("""
        DELETE FROM tracks
        WHERE album_id = %s AND from_album = TRUE
    """, (album_id,))
    cur.execute("""
        DELETE FROM albums
        WHERE id = %s
    """, (album_id,))

conn.commit()
cur.close()
conn.close()

log_event("sync_saved_albums", "Saved albums sync complete")
