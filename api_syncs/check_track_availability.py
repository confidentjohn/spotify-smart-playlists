import os
import psycopg2
import requests
import time
import json
from datetime import datetime
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

log_event("check_track_availability", "Checking track availability for outdated or missing records")

cur.execute("""
    SELECT t.id
    FROM tracks t
    LEFT JOIN track_availability a ON t.id = a.track_id
    WHERE a.checked_at IS NULL
       OR a.checked_at < NOW() - INTERVAL '60 days'
""")
track_ids = [row[0] for row in cur.fetchall()]
total = len(track_ids)
log_event("check_track_availability", f"Found {total} track(s) to check")

now = datetime.utcnow()

for i, track_id in enumerate(track_ids, start=1):
    log_event("check_track_availability", f"[{i}/{total}] Checking track: {track_id}")
    try:
        track = safe_spotify_call(sp.track, track_id, market=user_country)

        is_playable = track.get('is_playable')
        if is_playable is None:
            available_markets = track.get('available_markets', [])
            is_playable = user_country in available_markets
            log_event("check_track_availability", f"Fallback check: available in {user_country}? {is_playable}")

        log_event("check_track_availability", f"Track {track_id} → is_playable: {is_playable}")

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

    if i % 50 == 0 or i == total:
        conn.commit()
        log_event("check_track_availability", f"Committed batch up to track #{i}")

cur.close()
conn.close()
log_event("check_track_availability", "Finished checking availability")
