import os
import psycopg2
import requests
import fcntl
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

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
                print(f"⚠️ Spotify rate limited. Retry #{retries} in {retry_after} seconds...", flush=True)
                time.sleep(retry_after)
            else:
                print(f"❌ Spotify API error: {e}", flush=True)
                raise

# Acquire exclusive lock to avoid overlap with album sync
with open(LOCK_FILE, 'w') as lock_file:
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("❌ Another sync is already running.")
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

    # Reset all liked flags before re-syncing
    cur.execute("UPDATE tracks SET is_liked = FALSE")

    limit = 50
    offset = 0
    batch_size = 50
    counter = 0

    while True:
        results = safe_spotify_call(sp.current_user_saved_tracks, limit=limit, offset=offset)
        tracks = results['items']

        if not tracks:
            break

        for item in tracks:
            track = item['track']
            track_id = track['id']
            name = track['name']
            artist = track['artists'][0]['name']
            album = track['album']['name']
            album_id = track['album']['id']
            liked_added_at = item['added_at']

            # Get album's added_at from albums table, prefer it over liked date
            cur.execute("SELECT added_at FROM albums WHERE id = %s", (album_id,))
            album_row = cur.fetchone()
            album_added_at = album_row[0] if album_row else None
            # Album date has priority if it exists
            final_added_at = album_added_at if album_added_at else liked_added_at

            cur.execute("""
                INSERT INTO tracks (id, name, artist, album, album_id, is_liked, added_at)
                VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                ON CONFLICT (id) DO UPDATE 
                SET is_liked = TRUE,
                    album_id = EXCLUDED.album_id,
                    added_at = COALESCE(
                        (SELECT added_at FROM albums WHERE id = EXCLUDED.album_id),
                        EXCLUDED.added_at
                    );
            """, (track_id, name, artist, album, album_id, final_added_at))

            counter += 1
            if counter % batch_size == 0:
                conn.commit()

        offset += len(tracks)
        if len(tracks) < limit:
            break

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Synced saved tracks from library.")
