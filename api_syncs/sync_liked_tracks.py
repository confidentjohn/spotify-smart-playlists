import os
import psycopg2
import requests
import fcntl
import time
from datetime import datetime, timedelta, timezone
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.logger import log_event
from dateutil import parser

LOCK_FILE = "/tmp/sync_library.lock"

def get_access_token():
    auth_response = requests.post(
        'https://accounts.spotify.com/api/token',
        data={
            'grant_type': 'refresh_token',
            'refresh_token': os.environ['SPOTIFY_REFRESH_TOKEN'],
            'client_id': os.environ['SPOTIFY_CLIENT_ID'],
            'client_secret': os.environ['SPOTIFY_CLIENT_SECRET']
        }
    )
    return auth_response.json()['access_token']

def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                retries += 1
                log_event("sync_liked_tracks", f"Rate limit hit. Retry #{retries} in {retry_after}s")
                time.sleep(retry_after)
            else:
                log_event("sync_liked_tracks", f"Spotify error: {e}", level="error")
                raise

# Acquire lock to avoid overlap
with open(LOCK_FILE, 'w') as lock_file:
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log_event("sync_liked_tracks", "Another sync is already running", level="warning")
        exit(1)

    access_token = get_access_token()
    sp = Spotify(auth=access_token)

    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ.get('DB_PORT', 5432),
        sslmode='require'
    )
    cur = conn.cursor()

    from datetime import timezone
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(days=88)
    fresh_cutoff = now - timedelta(days=15)

    limit = 50
    offset = 0
    batch_size = 50
    counter = 0
    liked_track_ids = set()
    skipped_due_to_freshness = 0
    updated_liked_tracks = 0

    stop_fetching = False

    log_event("sync_liked_tracks", "Starting liked tracks sync")

    while True:
        results = safe_spotify_call(sp.current_user_saved_tracks, limit=limit, offset=offset)
        items = results['items']
        log_event("sync_liked_tracks", f"Processing batch: offset={offset}, size={len(items)}")

        if not items:
            break

        for item in items:
            track = item['track']
            if not track:
                continue

            track_id = track['id']
            liked_added_at = parser.isoparse(item['added_at'])
            if liked_added_at.tzinfo is None:
                from datetime import timezone
                liked_added_at = liked_added_at.replace(tzinfo=timezone.utc)
            liked_track_ids.add(track_id)

            if liked_added_at < fresh_cutoff:
                stop_fetching = True
                break

            # Skip if not recently added or due for recheck
            if liked_added_at < fresh_cutoff:
                cur.execute("""
                    SELECT date_liked_checked FROM tracks WHERE id = %s
                """, (track_id,))
                row = cur.fetchone()
                if row and row[0]:
                    last_checked = row[0]
                    if last_checked.tzinfo is None:
                        last_checked = last_checked.replace(tzinfo=timezone.utc)
                    if last_checked > stale_cutoff:
                        log_event("sync_liked_tracks", f"Skipping track {track_id} — checked recently on {last_checked.date()}")
                        skipped_due_to_freshness += 1
                        continue

            name = track['name']
            artist = track['artists'][0]['name']
            album = track['album']['name']
            album_id = track['album']['id']

            cur.execute("SELECT added_at FROM albums WHERE id = %s", (album_id,))
            album_row = cur.fetchone()
            album_added_at = album_row[0] if album_row else None
            final_added_at = album_added_at if album_added_at else liked_added_at

            cur.execute("""
                INSERT INTO tracks (id, name, artist, album, album_id, is_liked, added_at, date_liked_at, date_liked_checked)
                VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE 
                SET is_liked = TRUE,
                    album_id = EXCLUDED.album_id,
                    added_at = COALESCE(
                        (SELECT added_at FROM albums WHERE id = EXCLUDED.album_id),
                        EXCLUDED.added_at
                    ),
                    date_liked_at = EXCLUDED.date_liked_at,
                    date_liked_checked = EXCLUDED.date_liked_checked;
            """, (track_id, name, artist, album, album_id, final_added_at, liked_added_at, now))

            updated_liked_tracks += 1
            counter += 1
            if counter % 500 == 0:
                log_event("sync_liked_tracks", f"Updated {counter} tracks so far")
            if counter % batch_size == 0:
                conn.commit()

        if stop_fetching:
            break

        offset += len(items)
        if len(items) < limit:
            break

    if stop_fetching:
        log_event("sync_liked_tracks", "Stopping fetch early: reached tracks older than fresh_cutoff")

    log_event("sync_liked_tracks", f"{len(liked_track_ids)} liked tracks synced")
    log_event("sync_liked_tracks", f"Finished scanning liked tracks. Total fetched: {counter}")

    # ─────────────────────────────────────────────
    # Recheck stale tracks in DB not updated in 60+ days
    # ─────────────────────────────────────────────
    log_event("sync_liked_tracks", "Checking stale tracks from local DB")
    cur.execute("""
        SELECT id FROM tracks
        WHERE is_liked = FALSE AND (date_liked_checked IS NULL OR date_liked_checked < %s)
    """, (stale_cutoff,))
    stale_rows = cur.fetchall()

    for (track_id,) in stale_rows:
        try:
            track_data = safe_spotify_call(sp.track, track_id)
            # If we get here, the track still exists and is playable
            log_event("sync_liked_tracks", f"Checked stale track {track_id} from DB — not liked")
        except SpotifyException as e:
            if e.http_status == 404:
                log_event("sync_liked_tracks", f"Track {track_id} no longer available", level="warning")
            else:
                raise
        finally:
            cur.execute("""
                UPDATE tracks SET date_liked_checked = %s WHERE id = %s
            """, (now, track_id))
            conn.commit()  # Commit immediately to persist progress in case of failure

    # Update unliked tracks
    log_event("sync_liked_tracks", "Updating unliked tracks")
    cur.execute("""
        UPDATE tracks
        SET is_liked = FALSE
        WHERE id NOT IN %s
    """, (tuple(liked_track_ids),))

    # Remove orphaned unliked tracks
    log_event("sync_liked_tracks", "Removing orphaned tracks")
    cur.execute("""
        DELETE FROM tracks
        WHERE is_liked = FALSE AND from_album = FALSE
    """)
    cur.execute("SELECT COUNT(*) FROM tracks WHERE is_liked = FALSE AND from_album = FALSE")
    count = cur.fetchone()[0]
    log_event("sync_liked_tracks", f"Found {count} orphaned tracks to remove")

    conn.commit()
    log_event("sync_liked_tracks", f"✅ {updated_liked_tracks} tracks updated")
    log_event("sync_liked_tracks", f"⏭️ {skipped_due_to_freshness} tracks skipped due to recent check")
    cur.close()
    conn.close()
    log_event("sync_liked_tracks", "Liked tracks sync complete")