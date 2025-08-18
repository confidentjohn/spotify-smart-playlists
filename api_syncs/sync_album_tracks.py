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

sp = get_spotify_client()
# How many albums to integrity-check per run (can override via env ALBUM_INTEGRITY_BATCH)
INTEGRITY_CHECK_COUNT = int(os.getenv("ALBUM_INTEGRITY_BATCH", "50"))

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
# Safety: ensure columns exist (no-op if already present)
cur.execute("ALTER TABLE albums ADD COLUMN IF NOT EXISTS tracks_checked_at TIMESTAMP")
cur.execute("ALTER TABLE tracks ADD COLUMN IF NOT EXISTS popularity INTEGER")
conn.commit()
log_event("sync_album_tracks", "Schema check complete for tracks_checked_at and popularity")

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

        popularity = None
        if track_id and track_id in enriched_metadata:
            popularity = enriched_metadata[track_id].get('popularity')

        cur.execute("""
            INSERT INTO tracks (
                id, name, artist, album, album_id,
                from_album, track_number, disc_number, added_at,
                duration_ms, popularity
            )
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s,
                    %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                artist = EXCLUDED.artist,
                album = EXCLUDED.album,
                album_id = EXCLUDED.album_id,
                from_album = TRUE,
                track_number = EXCLUDED.track_number,
                disc_number = EXCLUDED.disc_number,
                added_at = COALESCE(tracks.added_at, EXCLUDED.added_at),
                duration_ms = EXCLUDED.duration_ms,
                popularity = COALESCE(EXCLUDED.popularity, tracks.popularity)
        """, (
            track_id, track_name, track_artist, album_name, album_id,
            track_number, disc_number, album_added_at,
            duration_ms, popularity
        ))

    cur.execute("""
        UPDATE albums SET
            tracks_synced = TRUE,
            tracks_checked_at = NOW()
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

# 3️⃣ Background integrity check: verify oldest N albums each run (N=INTEGRITY_CHECK_COUNT)
log_event("sync_album_tracks", f"Running background integrity check for oldest {INTEGRITY_CHECK_COUNT} albums")

cur.execute("""
    SELECT id, name, added_at
    FROM albums
    WHERE is_saved = TRUE
    ORDER BY tracks_checked_at NULLS FIRST, added_at ASC
    LIMIT %s
""", (INTEGRITY_CHECK_COUNT,))
oldest_albums = cur.fetchall()

for chk_album_id, chk_album_name, chk_album_added_at in oldest_albums:
    try:
        log_event("sync_album_tracks", f"Integrity check: {chk_album_name} ({chk_album_id})")
        album_tracks = get_all_album_tracks(chk_album_id)
        if not album_tracks:
            log_event("sync_album_tracks", f"No tracks returned during integrity check for album: {chk_album_name} ({chk_album_id})", level="warning")
            # Still record check to avoid hammering the same album
            cur.execute("UPDATE albums SET tracks_checked_at = NOW() WHERE id = %s", (chk_album_id,))
            conn.commit()
            continue

        # Enrich with popularity
        sp_track_ids = [t.get('id') for t in album_tracks if t.get('id')]
        enriched = {}
        for i in range(0, len(sp_track_ids), 50):
            batch_ids = sp_track_ids[i:i+50]
            response = safe_spotify_call(sp.tracks, batch_ids)
            for item in response.get('tracks', []):
                enriched[item['id']] = item

        # Upsert tracks + popularity; do not delete missing here
        for t in album_tracks:
            tid = t.get('id')
            if not tid:
                continue
            t_name = t.get('name')
            t_artist = t.get('artists', [{}])[0].get('name')
            t_num = t.get('track_number') or 1
            d_num = t.get('disc_number') or 1
            dur = t.get('duration_ms')
            pop = enriched.get(tid, {}).get('popularity')

            cur.execute("""
                INSERT INTO tracks (
                    id, name, artist, album, album_id,
                    from_album, track_number, disc_number, added_at,
                    duration_ms, popularity
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s,
                        %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    artist = EXCLUDED.artist,
                    album = EXCLUDED.album,
                    album_id = EXCLUDED.album_id,
                    from_album = TRUE,
                    track_number = EXCLUDED.track_number,
                    disc_number = EXCLUDED.disc_number,
                    duration_ms = EXCLUDED.duration_ms,
                    popularity = COALESCE(EXCLUDED.popularity, tracks.popularity)
            """, (tid, t_name, t_artist, chk_album_name, chk_album_id, t_num, d_num, chk_album_added_at, dur, pop))

        # Mark album as checked
        cur.execute("UPDATE albums SET tracks_checked_at = NOW() WHERE id = %s", (chk_album_id,))
        updated = cur.rowcount
        log_event("sync_album_tracks", f"Stamped tracks_checked_at for {chk_album_id}; rows updated: {updated}")
        if updated == 0:
            # Re-attempt with explicit type cast (rare, but helpful if id encoding mismatch)
            cur.execute("UPDATE albums SET tracks_checked_at = NOW() WHERE id::text = %s::text", (chk_album_id,))
            log_event("sync_album_tracks", f"Re-attempted stamp for {chk_album_id}; rows updated: {cur.rowcount}")
        conn.commit()
    except Exception as e:
        log_event("sync_album_tracks", f"Integrity check failed for album {chk_album_id}: {e}", level="error")
        conn.rollback()

conn.commit()

cur.close()
conn.close()
log_event("sync_album_tracks", "Album tracks sync complete")