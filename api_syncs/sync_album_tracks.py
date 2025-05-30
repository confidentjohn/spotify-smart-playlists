import os
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.logger import log_event

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
                log_event("sync_album_tracks", f"Rate limit hit. Retry #{retries} in {retry_after}s")
                time.sleep(retry_after)
            else:
                log_event("sync_album_tracks", f"Spotify error: {e}", level="error")
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

log_event("sync_album_tracks", "Syncing album tracks for unsynced albums")

# 1️⃣ Sync tracks for still-saved albums
cur.execute("""
    SELECT id, name, added_at FROM albums
    WHERE is_saved = TRUE AND (tracks_synced = FALSE OR tracks_synced IS NULL)
""")
saved_albums = cur.fetchall()

for album_id, album_name, album_added_at in saved_albums:
    log_event("sync_album_tracks", f"Syncing tracks for: {album_name} ({album_id})")
    album_tracks = safe_spotify_call(sp.album, album_id)['tracks']['items']

    for track in album_tracks:
        track_id = track['id']
        track_name = track['name']
        track_artist = track['artists'][0]['name']
        track_number = track.get('track_number') or 1

        cur.execute("""
            INSERT INTO tracks (id, name, artist, album, album_id, is_liked, from_album, track_number, added_at)
            VALUES (%s, %s, %s, %s, %s, FALSE, TRUE, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                from_album = TRUE,
                track_number = EXCLUDED.track_number,
                album_id = EXCLUDED.album_id,
                added_at = COALESCE(tracks.added_at, EXCLUDED.added_at)
        """, (track_id, track_name, track_artist, album_name, album_id, track_number, album_added_at))

    cur.execute("UPDATE albums SET tracks_synced = TRUE WHERE id = %s", (album_id,))

# 2️⃣ Handle removed albums (mark tracks as not from_album)
cur.execute("""
    SELECT id FROM albums
    WHERE is_saved = FALSE AND (tracks_synced = FALSE OR tracks_synced IS NULL)
""")
removed_albums = cur.fetchall()

for album_id, in removed_albums:
    log_event("sync_album_tracks", f"Marking tracks from removed album as not from_album: {album_id}")
    cur.execute("""
        UPDATE tracks
        SET from_album = FALSE
        WHERE album_id = %s
    """, (album_id,))
    cur.execute("UPDATE albums SET tracks_synced = TRUE WHERE id = %s", (album_id,))

# 3️⃣ Remove orphaned tracks (not liked + not from album)
log_event("sync_album_tracks", "Removing orphaned tracks")
cur.execute("""
    DELETE FROM tracks
    WHERE is_liked = FALSE AND from_album = FALSE
""")

# 4️⃣ Clean up availability data for deleted tracks
log_event("sync_album_tracks", "Cleaning orphaned availability data")
cur.execute("""
    DELETE FROM track_availability
    WHERE track_id NOT IN (SELECT id FROM tracks)
""")

conn.commit()
cur.close()
conn.close()
log_event("sync_album_tracks", "Album tracks sync complete")
