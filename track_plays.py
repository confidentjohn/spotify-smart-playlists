import os
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

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
    """Retry Spotify API call if rate limited. Logs retry attempts."""
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                retries += 1
                print(f"⚠️ Spotify rate limited. Retry #{retries} in {retry_after} seconds...", flush=True)
                time.sleep(retry_after)
            else:
                print(f"❌ Spotify API error: {e}", flush=True)
                raise

access_token = get_access_token()
sp = Spotify(auth=access_token)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432),
)
cur = conn.cursor()

# Fetch recent plays (max 50)
results = safe_spotify_call(sp.current_user_recently_played, limit=50)

for item in results['items']:
    track = item['track']
    track_id = track['id']
    name = track['name']
    artist = track['artists'][0]['name']
    album = track['album']['name']
    played_at = item['played_at']
    played_at = datetime.fromisoformat(played_at.replace('Z', '+00:00')).astimezone(ZoneInfo('UTC'))

    # Check if track is saved in your library
    is_liked = safe_spotify_call(sp.current_user_saved_tracks_contains, [track_id])[0]

    # Insert or update track metadata
    cur.execute("""
        INSERT INTO tracks (id, name, artist, album, is_liked)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET is_liked = EXCLUDED.is_liked;
    """, (track_id, name, artist, album, is_liked))

    # Insert play history (skip if already exists)
    cur.execute("""
        INSERT INTO plays (track_id, played_at)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """, (track_id, played_at))

conn.commit()
cur.close()
conn.close()

print("✅ Recent plays tracked and stored.")
