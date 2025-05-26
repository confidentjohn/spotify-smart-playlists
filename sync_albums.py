import os
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

# ─────────────────────────────────────────────────────
# 🔐 Get Access Token
# ─────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────
# 🎯 Rate-limit safe wrapper for API calls
# ─────────────────────────────────────────────────────
def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                retries += 1
                print(f"⚠️ Rate limited by Spotify. Retry #{retries} in {retry_after} sec", flush=True)
                time.sleep(retry_after)
            else:
                print(f"❌ Spotify API error: {e}", flush=True)
                raise
        except Exception as e:
            print(f"❌ General error: {e}", flush=True)
            raise

# ─────────────────────────────────────────────────────
print("🚀 Starting album sync...", flush=True)
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
print("🔗 Connected to PostgreSQL", flush=True)

limit = 20
offset = 0
current_album_ids = set()
total_albums = 0
total_tracks = 0

# ─────────────────────────────────────────────────────
while True:
    print(f"📦 Fetching albums offset {offset}", flush=True)
    results = safe_spotify_call(sp.current_user_saved_albums, limit=limit, offset=offset)
    items = results['items']

    if not items:
        print("✅ No more albums found.", flush=True)
        break

    for item in items:
        total_albums += 1
        album = item['album']
        album_id = album['id']
        name = album['name']
        artist = album['artists'][0]['name']
        release_date = album.get('release_date')
        total_tracks_expected = album.get('total_tracks')
        added_at = item.get('added_at')
        current_album_ids.add(album_id)

        print(f"\n🎶 Album [{total_albums}]: {name} ({album_id})", flush=True)

        # Insert/update album
        cur.execute("""
            INSERT INTO albums (id, name, artist, release_date, total_tracks, is_saved, added_at)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (id) DO UPDATE 
                SET is_saved = TRUE, added_at = EXCLUDED.added_at;
        """, (album_id, name, artist, release_date, total_tracks_expected, added_at))

        # Fetch album tracks
        try:
            print(f"🎯 Fetching tracks for album: {name}", flush=True)
            album_tracks_data = safe_spotify_call(sp.album, album_id)
            album_tracks = album_tracks_data['tracks']['items']
        except Exception as e:
            print(f"❌ Failed to fetch tracks for album {name}: {e}", flush=True)
            continue

        for track in album_tracks:
            total_tracks += 1
            track_id = track['id']
            track_name = track['name']
            track_artist = track['artists'][0]['name']
            track_number = track.get("track_number") or 1

            print(f"   🎵 Track: {track_name} (#{track_number})", flush=True)

            cur.execute("""
                INSERT INTO tracks (id, name, artist, album, album_id, is_liked, from_album, track_number, added_at)
                VALUES (%s, %s, %s, %s, %s, FALSE, TRUE, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    from_album = TRUE,
                    track_number = EXCLUDED.track_number,
                    album_id = EXCLUDED.album_id,
                    added_at = EXCLUDED.added_at;
            """, (track_id, track_name, track_artist, name, album_id, track_number, added_at))

    offset += len(items)
    if len(items) < limit:
        break

# ─────────────────────────────────────────────────────
print("🧹 Cleaning up removed albums and tracks...", flush=True)

cur.execute("""
    UPDATE albums
    SET is_saved = FALSE
    WHERE id NOT IN %s
""", (tuple(current_album_ids),))

cur.execute("""
    UPDATE tracks
    SET from_album = FALSE
    WHERE album_id IN (
        SELECT id FROM albums WHERE is_saved = FALSE
    )
""")

conn.commit()
cur.close()
conn.close()

print(f"\n✅ Sync complete: {total_albums} albums, {total_tracks} tracks", flush=True)
