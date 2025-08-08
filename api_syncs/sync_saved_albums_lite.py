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
# Lite window: only process albums added in the last N days (default 10)
# ─────────────────────────────────────────────
days_window = int(os.getenv("ALBUM_LITE_WINDOW_DAYS", "10"))
cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days_window)
log_event("sync_saved_albums", f"Lite sync: processing albums added on/after {cutoff.isoformat()} (last {days_window} days)")

# ─────────────────────────────────────────────
# Sync saved albums from Spotify
# ─────────────────────────────────────────────
while True:
    results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=offset)
    items = results['items']
    if not items:
        break

    stop_paging = False

    for item in items:
        album = item['album']
        album_id = album['id']
        added_at = item.get('added_at')
        # Parse added_at and compare to cutoff (Spotify returns newest-first)
        added_dt = parser.parse(added_at) if added_at else None
        if added_dt is not None:
            # Normalize to naive UTC for comparison
            if added_dt.tzinfo is not None:
                added_dt = added_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        # If this album's added date is older than our cutoff, we can stop paging
        if added_dt is not None and added_dt < cutoff:
            stop_paging = True
            break

        # Only upsert albums within the cutoff window
        if added_dt is not None and added_dt >= cutoff:
            current_album_ids.add(album_id)
            # Extract new album data
            album_type = album.get('album_type')
            album_image_url = album['images'][0]['url'] if album.get('images') else None
            artist_id = album['artists'][0]['id']

            cur.execute(
                """
                INSERT INTO albums (id, name, artist, artist_id, release_date, total_tracks, is_saved, added_at, tracks_synced, album_type, album_image_url)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, FALSE, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET is_saved = TRUE,
                    added_at = EXCLUDED.added_at,
                    artist_id = EXCLUDED.artist_id,
                    album_type = EXCLUDED.album_type,
                    album_image_url = EXCLUDED.album_image_url;
                """,
                (
                    album_id,
                    album['name'],
                    album['artists'][0]['name'],
                    artist_id,
                    album.get('release_date'),
                    album.get('total_tracks'),
                    added_at,
                    album_type,
                    album_image_url,
                ),
            )

    if stop_paging:
        break

    offset += len(items)

log_event("sync_saved_albums", f"{len(current_album_ids)} saved albums synced")

# ─────────────────────────────────────────────
# Mark albums as unsaved if they were added in the last N days but not in current_album_ids
# ─────────────────────────────────────────────
if current_album_ids:
    cur.execute(
        """
        UPDATE albums
           SET is_saved = FALSE
         WHERE added_at >= %s
           AND is_saved = TRUE
           AND NOT (id = ANY(%s))
        """,
        (cutoff, list(current_album_ids)),
    )
    unsaved_count = cur.rowcount
    log_event("sync_saved_albums", f"{unsaved_count} recent album(s) marked as unsaved (not present in Spotify).")
else:
    log_event(
        "sync_saved_albums",
        "⚠️ Skipping 'mark unsaved' step because no recent Spotify album IDs were collected. This prevents accidental mass-unsave when the Spotify API returns no items.",
    )
    unsaved_count = 0

conn.commit()
cur.close()
conn.close()

log_event("sync_saved_albums", "Saved albums sync complete")
