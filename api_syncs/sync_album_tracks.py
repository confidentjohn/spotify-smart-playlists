import os
import psycopg2
import requests
import time
import requests.exceptions
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection

def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while retries < 5:
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
        except requests.exceptions.ConnectionError as e:
            retries += 1
            log_event("sync_album_tracks", f"Connection error: {e}. Retry #{retries} in 5s", level="warning")
            time.sleep(5)
    raise Exception("safe_spotify_call failed after 5 retries")


sp = get_spotify_client()

# Helper to get all tracks for an album (handles pagination)
def get_all_album_tracks(album_id):
    album_data = safe_spotify_call(sp.album, album_id)
    tracks = album_data['tracks']['items']
    next_url = album_data['tracks']['next']

    while next_url:
        next_page = safe_spotify_call(sp._get, next_url)
        tracks.extend(next_page['items'])
        next_url = next_page['next']

    return tracks

conn = get_db_connection()
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
    album_tracks = get_all_album_tracks(album_id)
    if not album_tracks:
        log_event("sync_album_tracks", f"No tracks found for album: {album_name} ({album_id})")
        continue

    track_ids = [t['id'] for t in album_tracks if t.get('id')]
    enriched_metadata = {}
    for i in range(0, len(track_ids), 50):
        batch_ids = track_ids[i:i+50]
        response = safe_spotify_call(sp.tracks, batch_ids)
        for item in response.get('tracks', []):
            enriched_metadata[item['id']] = item

    for track in album_tracks:
        track_id = track['id']
        track_name = track['name']
        track_artist = track['artists'][0]['name']
        track_number = track.get('track_number') or 1
        disc_number = track.get('disc_number') or 1

        duration_ms = track.get('duration_ms')

        cur.execute("""
            INSERT INTO tracks (
                id, name, artist, album, album_id,
                from_album, track_number, disc_number, added_at,
                duration_ms
            )
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s,
                    %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                artist = EXCLUDED.artist,
                album = EXCLUDED.album,
                album_id = EXCLUDED.album_id,
                from_album = TRUE,
                track_number = EXCLUDED.track_number,
                disc_number = EXCLUDED.disc_number,
                added_at = COALESCE(tracks.added_at, EXCLUDED.added_at),
                duration_ms = EXCLUDED.duration_ms
        """, (
            track_id, track_name, track_artist, album_name, album_id,
            track_number, disc_number, album_added_at,
            duration_ms
        ))

    cur.execute("""
        UPDATE albums SET
            tracks_synced = TRUE
        WHERE id = %s
    """, (album_id,))
    conn.commit()

# 2️⃣ Remove albums and their tracks if they are no longer saved
cur.execute("""
    SELECT id FROM albums
    WHERE is_saved = FALSE
""")
removed_albums = cur.fetchall()

for album_id, in removed_albums:
    log_event("sync_album_tracks", f"Deleting tracks and album: {album_id}")

    cur.execute("SELECT COUNT(*) FROM liked_tracks WHERE track_id IN (SELECT id FROM tracks WHERE album_id = %s)", (album_id,))
    deleted_liked_count = cur.fetchone()[0]
    log_event("sync_album_tracks", f"Deleting {deleted_liked_count} liked tracks associated with album: {album_id}")

    cur.execute("""
        DELETE FROM liked_tracks
        WHERE track_id IN (
            SELECT id FROM tracks WHERE album_id = %s
        )
    """, (album_id,))

    cur.execute("DELETE FROM tracks WHERE album_id = %s", (album_id,))
    cur.execute("DELETE FROM albums WHERE id = %s", (album_id,))
    conn.commit()

conn.commit()

cur.close()
conn.close()
log_event("sync_album_tracks", "Album tracks sync complete")