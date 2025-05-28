import os
import time
import requests
import psycopg2
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from datetime import datetime

# ─────────────────────────────────────────────
# Get Access Token
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
                print(f"⚠️ Rate limit hit. Retry #{retries} in {retry_after}s...", flush=True)
                time.sleep(retry_after)
            else:
                print(f"❌ Spotify error: {e}", flush=True)
                raise
        except requests.exceptions.ReadTimeout as e:
            retries += 1
            print(f"⏳ Timeout hit. Retry #{retries} in 5s... ({e})", flush=True)
            time.sleep(5)

# ─────────────────────────────────────────────
# Setup Spotify client
# ─────────────────────────────────────────────
access_token = get_access_token()
sp = Spotify(auth=access_token)

# ─────────────────────────────────────────────
# Connect to PostgreSQL
# ─────────────────────────────────────────────
conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
)
cur = conn.cursor()

# ─────────────────────────────────────────────
# Sync recently played tracks
# ─────────────────────────────────────────────
print("🎧 Tracking recently played tracks...", flush=True)
results = safe_spotify_call(sp.current_user_recently_played, limit=50)
recent_plays = results["items"]

for item in recent_plays:
    track = item["track"]
    track_id = track["id"]
    played_at = item["played_at"]
    played_at_dt = datetime.strptime(played_at, "%Y-%m-%dT%H:%M:%S.%fZ")

    cur.execute("SELECT 1 FROM plays WHERE track_id = %s AND played_at = %s", (track_id, played_at_dt))
    if cur.fetchone():
        continue

    is_liked = safe_spotify_call(sp.current_user_saved_tracks_contains, [track_id])[0]
    time.sleep(0.2)  # gentle throttle

    cur.execute("""
        INSERT INTO plays (track_id, played_at)
        VALUES (%s, %s)
        ON CONFLICT (track_id, played_at) DO NOTHING;
    """, (track_id, played_at_dt))

    cur.execute("""
        INSERT INTO tracks (id, name, artist, album, is_liked)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET is_liked = EXCLUDED.is_liked;
    """, (
        track_id,
        track["name"],
        track["artists"][0]["name"],
        track["album"]["name"],
        is_liked
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Tracked recent plays.", flush=True)
