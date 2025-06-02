import os
import psycopg2
import requests
import time
from datetime import datetime, timedelta
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.logger import log_event

# ─────────────────────────────────────────────
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
access_token = get_access_token()
sp = Spotify(auth=access_token)

# 🌍 Get user country
try:
    user_profile = safe_spotify_call(sp.current_user)
    user_country = user_profile.get("country", "US")
    log_event("check_track_availability", f"User country detected: {user_country}")
except Exception as e:
    log_event("check_track_availability", f"Could not retrieve user country, defaulting to 'US': {e}", level="warning")
    user_country = "US"

conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432),
)
cur = conn.cursor()

now = datetime.utcnow()
cutoff = now - timedelta(days=60)

# ─────────────────────────────────────────────
# Step 1: Get all known track_ids
cur.execute("""
    SELECT id FROM tracks
    UNION
    SELECT track_id FROM liked_tracks
""")
known_ids = {row[0] for row in cur.fetchall()}
log_event("check_track_availability", f"Total unique known track IDs: {len(known_ids)}")

# Step 2: Get current track_availability records
cur.execute("SELECT track_id, checked_at FROM track_availability")
availability = dict(cur.fetchall())

# Step 3: Compute new, stale, and removed track IDs
new_ids = known_ids - availability.keys()
stale_ids = {tid for tid, dt in availability.items() if dt is None or dt < cutoff}

# We recompute known_ids here to include only truly known current items
cur.execute("""
    SELECT id FROM tracks
    UNION
    SELECT track_id FROM liked_tracks
""")
current_known_ids = {row[0] for row in cur.fetchall()}
removed_ids = availability.keys() - current_known_ids

log_event("check_track_availability", f"New: {len(new_ids)}, Stale: {len(stale_ids)}, Removed: {len(removed_ids)}")

# Step 4: Delete removed IDs
if removed_ids:
    cur.execute("DELETE FROM track_availability WHERE track_id = ANY(%s)", (list(removed_ids),))
    log_event("check_track_availability", f"Deleted {len(removed_ids)} removed tracks from availability table")

# Step 5: Query Spotify and update records
to_check = list(new_ids | stale_ids)
log_event("check_track_availability", f"Checking availability for {len(to_check)} tracks")

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

cur.close()
conn.close()
log_event("check_track_availability", "✅ Finished checking availability")
