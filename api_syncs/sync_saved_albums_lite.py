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
# Lite window: only process the most recent N saved albums (default 50)
# Compare Spotify to the DB and insert ONLY what is missing.
# Removals are handled by the full sync.
# ─────────────────────────────────────────────
RECENT_COUNT = int(os.getenv("ALBUM_LITE_RECENT_COUNT", "50"))

offset = 0
collected = []
per_page = 50

log_event("sync_saved_albums", f"Lite sync: fetching the last {RECENT_COUNT} saved albums from Spotify")

while len(collected) < RECENT_COUNT:
    page_limit = min(per_page, RECENT_COUNT - len(collected))
    results = safe_spotify_call(sp.current_user_saved_albums, limit=page_limit, offset=offset)
    items = results.get('items', [])
    if not items:
        break
    collected.extend(items)
    offset += len(items)
    log_event("sync_saved_albums", f"Fetched {len(collected)} / {RECENT_COUNT}")

# If nothing fetched, exit early
if not collected:
    log_event("sync_saved_albums", "No saved albums returned from Spotify. Exiting sync.")
    conn.commit()
    cur.close()
    conn.close()
    sys.exit(0)

# Build a list of album records from the collected items
spotify_recent_albums = []
spotify_recent_ids = []
for item in collected:
    album = item.get('album') or {}
    album_id = album.get('id')
    if not album_id:
        continue
    spotify_recent_ids.append(album_id)
    album_type = album.get('album_type')
    album_image_url = album['images'][0]['url'] if album.get('images') else None
    artist = (album.get('artists') or [{}])[0]
    artist_id = artist.get('id')
    artist_name = artist.get('name')
    spotify_recent_albums.append({
        "id": album_id,
        "name": album.get('name'),
        "artist": artist_name,
        "artist_id": artist_id,
        "release_date": album.get('release_date'),
        "total_tracks": album.get('total_tracks'),
        "added_at": item.get('added_at'),
        "album_type": album_type,
        "album_image_url": album_image_url
    })

log_event("sync_saved_albums", f"Collected {len(spotify_recent_albums)} recent album records from Spotify")

# Check which of these already exist in the DB
existing_ids = set()
if spotify_recent_ids:
    cur.execute(
        """
        SELECT id
          FROM albums
         WHERE id = ANY(%s)
        """,
        (spotify_recent_ids,)
    )
    existing_ids = {row[0] for row in cur.fetchall()}

missing_ids = [a["id"] for a in spotify_recent_albums if a["id"] not in existing_ids]
log_event("sync_saved_albums", f"Existing in DB: {len(existing_ids)} | Missing: {len(missing_ids)}")

# Insert only missing ones (no updates). Use ON CONFLICT DO NOTHING for safety.
to_insert = [a for a in spotify_recent_albums if a["id"] in missing_ids]

if to_insert:
    insert_values = [
        (
            a["id"],
            a["name"],
            a["artist"],
            a["artist_id"],
            a["release_date"],
            a["total_tracks"],
            True,                     # is_saved
            a["added_at"],            # added_at (Spotify's added time)
            False,                    # tracks_synced
            a["album_type"],
            a["album_image_url"],
        )
        for a in to_insert
    ]

    # executemany insert
    cur.executemany(
        """
        INSERT INTO albums (
            id, name, artist, artist_id, release_date, total_tracks, is_saved, added_at, tracks_synced, album_type, album_image_url
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO NOTHING
        """,
        insert_values
    )
    log_event("sync_saved_albums", f"Inserted {cur.rowcount} new album(s) from the last {RECENT_COUNT}.")
else:
    log_event("sync_saved_albums", "No new albums to insert from the recent set.")

log_event("sync_saved_albums", f"Lite sync complete: Spotify recent={len(spotify_recent_albums)}, DB existing={len(existing_ids)}, inserted={cur.rowcount if to_insert else 0}")

conn.commit()
cur.close()
conn.close()
log_event("sync_saved_albums", "Saved albums lite sync complete")
