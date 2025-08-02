import os
from utils.db_utils import get_db_connection
import requests
import time
from datetime import datetime, timedelta
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                retries += 1
                log_event("check_track_availability", f"Rate limit hit. Retry #{retries} in {retry_after}s")
                time.sleep(retry_after)
            else:
                log_event("check_track_availability", f"Spotify error: {e}", level="error")
                raise
        except requests.exceptions.ReadTimeout as e:
            retries += 1
            log_event("check_track_availability", f"Timeout hit. Retry #{retries} in 5s... ({e})")
            time.sleep(5)
        except Exception as e:
            log_event("check_track_availability", f"Unexpected error: {e}", level="error")
            raise

# ─────────────────────────────────────────────
sp = get_spotify_client()

# 🌍 Get user country
try:
    user_profile = safe_spotify_call(sp.current_user)
    user_country = user_profile.get("country", "US")
    log_event("check_track_availability", f"User country detected: {user_country}")
except Exception as e:
    log_event("check_track_availability", f"Could not retrieve user country, defaulting to 'US': {e}", level="warning")
    user_country = "US"

conn = get_db_connection()
cur = conn.cursor()

now = datetime.utcnow()

# ─────────────────────────────────────────────
# Step 1: Build unified view of track IDs with last check timestamp
cur.execute("""
    SELECT combined.track_id, COALESCE(ta.checked_at, '1970-01-01') AS last_check
    FROM (
        SELECT track_id FROM liked_tracks
        UNION
        SELECT id AS track_id FROM tracks
    ) AS combined
    LEFT JOIN track_availability ta ON combined.track_id = ta.track_id
    ORDER BY COALESCE(ta.checked_at, '1970-01-01') ASC
    LIMIT 100
""")

rows = cur.fetchall()
to_check = [track_id for track_id, _ in rows]

log_event("check_track_availability", f"Eligible tracks to check: {len(to_check)}")

# Step 2: Query Spotify and update records
for i, track_id in enumerate(to_check, start=1):
    log_event("check_track_availability", f"[{i}/{len(to_check)}] Checking track: {track_id}")
    try:
        track = safe_spotify_call(sp.track, track_id, market=user_country)
        is_playable = track.get('is_playable')
        if is_playable is None:
            available_markets = track.get('available_markets', [])
            is_playable = user_country in available_markets
    except Exception as e:
        log_event("check_track_availability", f"Error retrieving track {track_id}: {e}", level="error")
        is_playable = False

    try:
        cur.execute("""
            INSERT INTO track_availability (track_id, is_playable, checked_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (track_id) DO UPDATE SET
                is_playable = EXCLUDED.is_playable,
                checked_at = EXCLUDED.checked_at;
        """, (track_id, is_playable, now))
    except Exception as db_err:
        log_event("check_track_availability", f"Database error for track {track_id}: {db_err}", level="error")

    if i % 50 == 0 or i == len(to_check):
        conn.commit()
        log_event("check_track_availability", f"Committed batch up to track #{i}")


# ─────────────────────────────────────────────
# Cleanup: remove orphaned entries from track_availability
cur.execute("""
    DELETE FROM track_availability
    WHERE track_id NOT IN (
        SELECT track_id FROM liked_tracks
        UNION
        SELECT id FROM tracks
    );
""")
conn.commit()
log_event("check_track_availability", "🧹 Removed orphaned rows from track_availability")

cur.close()
conn.close()
log_event("check_track_availability", "✅ Finished checking availability")
