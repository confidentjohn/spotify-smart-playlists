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

# Fetch first page of saved albums
results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=0)
items = results['items']
if not items:
    log_event("sync_saved_albums", "No saved albums found in Spotify. Exiting sync.")
    cur.close()
    conn.close()
    sys.exit(0)

# Determine anchor_date from first item's added_at (UTC date only)
first_added_at = items[0].get('added_at')
if first_added_at is None:
    log_event("sync_saved_albums", "First saved album has no added_at date. Exiting sync.")
    cur.close()
    conn.close()
    sys.exit(0)

anchor_dt = parser.parse(first_added_at)
if anchor_dt.tzinfo is not None:
    anchor_dt = anchor_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
anchor_date = anchor_dt.date()

# Create window_dates set: dates from anchor_date going back days_window - 1 days
window_dates = set(anchor_date - datetime.timedelta(days=i) for i in range(days_window))

min_window_date = min(window_dates)
max_window_date = max(window_dates)

log_event("sync_saved_albums", f"Lite sync: processing albums added between {min_window_date.isoformat()} and {max_window_date.isoformat()} (last {days_window} days)")

offset += len(items)
stop_paging = False

# ─────────────────────────────────────────────
# Sync saved albums from Spotify
# ─────────────────────────────────────────────
while True:
    # Process current items
    for item in items:
        album = item['album']
        album_id = album['id']
        added_at = item.get('added_at')
        if added_at is None:
            continue
        added_dt = parser.parse(added_at)
        if added_dt.tzinfo is not None:
            added_dt = added_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        added_date = added_dt.date()

        # If added_date is earlier than min_window_date, stop paging
        if added_date < min_window_date:
            stop_paging = True
            break

        # Only upsert albums whose added_date is in window_dates
        if added_date in window_dates:
            current_album_ids.add(album_id)
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

    # Fetch next page
    results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=offset)
    items = results['items']
    if not items:
        break
    offset += len(items)

log_event("sync_saved_albums", f"{len(current_album_ids)} saved albums synced")

# ─────────────────────────────────────────────
# Mark albums as unsaved only if they are in DB (recent window) but not returned by Spotify
# ─────────────────────────────────────────────
if not current_album_ids:
    log_event(
        "sync_saved_albums",
        "⚠️ Skipping 'mark unsaved' step because no recent Spotify album IDs were collected (likely transient API issue)."
    )
else:
    # 1) Fetch recent album IDs from DB that are still marked saved and where DATE(added_at) in window_dates
    cur.execute(
        f"""
        SELECT id
          FROM albums
         WHERE is_saved = TRUE
           AND DATE(added_at) = ANY(%s)
        """,
        (list(window_dates),)
    )
    _db_rows = cur.fetchall()
    db_recent_ids = {r[0] for r in _db_rows}

    # 2) Compute precise set difference: present in DB recent set, missing from Spotify response
    to_unsave = db_recent_ids - current_album_ids

    log_event(
        "sync_saved_albums",
        f"Recent DB albums: {len(db_recent_ids)} | Recent Spotify albums: {len(current_album_ids)} | Will mark unsaved: {len(to_unsave)}"
    )

    # 3) Update only those specific IDs
    if to_unsave:
        cur.execute(
            """
            UPDATE albums
               SET is_saved = FALSE
             WHERE id = ANY(%s)
            """,
            (list(to_unsave),)
        )
        unsaved_count = cur.rowcount
        log_event("sync_saved_albums", f"{unsaved_count} recent album(s) marked as unsaved (missing from Spotify).")
    else:
        unsaved_count = 0
        log_event("sync_saved_albums", "No recent albums needed to be marked unsaved.")

conn.commit()
cur.close()
conn.close()

log_event("sync_saved_albums", "Saved albums sync complete")
