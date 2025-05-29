import os
import psycopg2
import requests
import time
import json
from datetime import datetime
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

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
                print(f"⚠️ Rate limit hit. Retry #{retries} in {retry_after}s...", flush=True)
                time.sleep(retry_after)
            else:
                print(f"❌ Spotify error: {e}", flush=True)
                raise
        except requests.exceptions.ReadTimeout as e:
            retries += 1
            print(f"⏳ Timeout hit. Retry #{retries} in 5s... ({e})", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e}", flush=True)
            raise

# ─────────────────────────────────────────────
access_token = get_access_token()
sp = Spotify(auth=access_token)

# 🌍 Get user country
try:
    user_profile = safe_spotify_call(sp.current_user)
    user_country = user_profile.get("country", "US")
    print(f"🌍 User country detected: {user_country}", flush=True)
except Exception as e:
    print(f"⚠️ Could not retrieve user country, defaulting to 'US': {e}", flush=True)
    user_country = "US"

# ─────────────────────────────────────────────
conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432),
)
cur = conn.cursor()

print("🔍 Checking track availability for outdated or missing records...", flush=True)

cur.execute("""
    SELECT t.id
    FROM tracks t
    LEFT JOIN track_availability a ON t.id = a.track_id
    WHERE a.checked_at IS NULL
       OR a.checked_at < NOW() - INTERVAL '60 days'
       OR a.is_playable = FALSE
""")
track_ids = [row[0] for row in cur.fetchall()]
total = len(track_ids)
print(f"📦 Found {total} track(s) to check", flush=True)

now = datetime.utcnow()

for i, track_id in enumerate(track_ids, start=1):
    print(f"🎯 [{i}/{total}] Checking track: {track_id}", flush=True)
    try:
        # Now using market param based on user country
        track = safe_spotify_call(sp.track, track_id, market=user_country)
        print(json.dumps(track, indent=2), flush=True)

        is_playable = track.get('is_playable')
        if is_playable is None:
            available_markets = track.get('available_markets', [])
            is_playable = user_country in available_markets
            print(f"📌 Fallback: Available in user's country ({user_country}): {is_playable}", flush=True)

        print(f"✅ Track {track_id} → is_playable: {is_playable}", flush=True)

    except Exception as e:
        print(f"⚠️ Error retrieving track {track_id}: {e}", flush=True)
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
        print(f"❌ Database error for track {track_id}: {db_err}", flush=True)

    if i % 50 == 0 or i == total:
        conn.commit()
        print(f"💾 Committed batch up to track #{i}", flush=True)

cur.close()
conn.close()
print("✅ Finished checking availability.", flush=True)
