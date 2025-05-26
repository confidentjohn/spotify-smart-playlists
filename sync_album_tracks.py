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

print("🎼 Syncing album tracks for unsynced albums...", flush=True)

cur.execute("SELECT id, name FROM albums WHERE is_saved = TRUE AND (tracks_synced = FALSE OR tracks_synced IS NULL)")
albums = cur.fetchall()

for album_id, album_name in albums:
    print(f"🎵 Fetching tracks for: {album_name} ({album_id})", flush=True)
    album_tracks = safe_spotify_call(sp.album, album_id)['tracks']['items']

    for track in album_tracks:
        track_id = track['id']
        cur.execute("""
            INSERT INTO tracks (id, name, artist, album, album_id, is_liked, from_album, track_number)
            VALUES (%s, %s, %s, %s, %s, FALSE, TRUE, %s)
            ON CONFLICT (id) DO UPDATE SET
                from_album = TRUE,
                track_number = EXCLUDED.track_number,
                album_id = EXCLUDED.album_id;
        """, (
            track_id,
            track['name'],
            track['artists'][0]['name'],
            album_name,
            album_id,
            track.get('track_number') or 1
        ))

    cur.execute("UPDATE albums SET tracks_synced = TRUE WHERE id = %s", (album_id,))
    conn.commit()

cur.close()
conn.close()
print("✅ Album tracks synced.", flush=True)