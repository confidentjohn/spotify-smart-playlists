import os
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

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

access_token = get_access_token()
sp = Spotify(auth=access_token)

conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432),
)
cur = conn.cursor()

limit = 20
offset = 0
current_album_ids = set()

print("📀 Starting saved albums sync...", flush=True)

while True:
    results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=offset)
    items = results['items']
    if not items:
        break

    for item in items:
        album = item['album']
        album_id = album['id']
        current_album_ids.add(album_id)
        added_at = item.get('added_at')

        cur.execute("""
            INSERT INTO albums (id, name, artist, release_date, total_tracks, is_saved, added_at, tracks_synced)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, FALSE)
            ON CONFLICT (id) DO UPDATE
            SET is_saved = TRUE, added_at = EXCLUDED.added_at;
        """, (
            album_id,
            album['name'],
            album['artists'][0]['name'],
            album.get('release_date'),
            album.get('total_tracks'),
            added_at
        ))

    offset += len(items)
    if len(items) < limit:
        break

# 🚫 Mark albums no longer saved
cur.execute("""
    UPDATE albums 
    SET is_saved = FALSE, tracks_synced = FALSE 
    WHERE id NOT IN %s
""", (tuple(current_album_ids),))

# 🗑️ Delete albums that are no longer saved AND have no associated tracks
print("🗑️ Cleaning up removed albums with no remaining tracks...", flush=True)
cur.execute("""
    DELETE FROM albums
    WHERE is_saved = FALSE
      AND id NOT IN (SELECT DISTINCT album_id FROM tracks)
""")

conn.commit()
cur.close()
conn.close()

print("✅ Saved albums updated.", flush=True)
