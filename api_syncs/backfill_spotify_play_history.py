"""
Manual backfill of artist_id, album_id, and album_type into spotify_play_history
in small, resumable batches.

Usage examples:
  python api_syncs/backfill_spotify_play_history.py                # defaults to --batch-size 500
  python api_syncs/backfill_spotify_play_history.py --batch-size 250

Behavior:
- Selects up to N unique track_ids that are still missing any of (artist_id, album_id, album_type).
- Looks up metadata via Spotify API in chunks of 50 (tracks endpoint limit).
- Updates spotify_play_history rows for those track_ids.
- Upserts minimal rows into artists and albums to keep the catalog consistent.
- Commits once per run; rerun as many times as needed. Already-filled tracks will not be selected again.
"""
import os
import sys as _sys
# Ensure project root is on sys.path when running as a script
_sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse
import sys
import time

import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from utils.db_utils import get_db_connection
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

CHUNK = 50  # Spotify API max batch for tracks
JOB_NAME = "backfill_spotify_play_history"


def fetch_missing_track_ids(cur, limit):
    """Return up to `limit` unique track_ids that still need enrichment.

    We group by track_id so each appears once, and order by earliest play so
    the backfill proceeds oldest-first.
    """
    cur.execute(
        """
        SELECT track_id
          FROM spotify_play_history
         WHERE track_id IS NOT NULL
           AND (
                artist_id   IS NULL OR
                album_id    IS NULL OR
                album_type  IS NULL OR
                duration_ms IS NULL
           )
         GROUP BY track_id
         ORDER BY MIN(checked_at) NULLS FIRST, MIN(played_at) ASC
         LIMIT %s
        """,
        (limit,),
    )
    return [r[0] for r in cur.fetchall()]


def upsert_artist(cur, artist_id, artist_name):
    if not artist_id:
        return
    cur.execute(
        """
        INSERT INTO artists (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, artists.name)
        """,
        (artist_id, artist_name),
    )


def upsert_album(cur, album_id, album_name, primary_artist_name, primary_artist_id, release_date, album_type, image_url):
    if not album_id:
        return
    cur.execute(
        """
        INSERT INTO albums (id, name, artist, artist_id, release_date, album_type, album_image_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, albums.name),
            artist = COALESCE(EXCLUDED.artist, albums.artist),
            artist_id = COALESCE(EXCLUDED.artist_id, albums.artist_id),
            release_date = COALESCE(EXCLUDED.release_date, albums.release_date),
            album_type = COALESCE(EXCLUDED.album_type, albums.album_type),
            album_image_url = COALESCE(EXCLUDED.album_image_url, albums.album_image_url)
        """,
        (album_id, album_name, primary_artist_name, primary_artist_id, release_date, album_type, image_url),
    )


def update_history_rows(cur, track_id, artist_id, album_id, album_type, duration_ms):
    cur.execute(
        """
        UPDATE spotify_play_history
           SET artist_id   = COALESCE(%s, artist_id),
               album_id    = COALESCE(%s, album_id),
               album_type  = COALESCE(%s, album_type),
               duration_ms = COALESCE(%s, duration_ms),
               checked_at  = NOW()
         WHERE track_id = %s
           AND (
                artist_id   IS NULL OR
                album_id    IS NULL OR
                album_type  IS NULL OR
                duration_ms IS NULL
           )
        """,
        (artist_id, album_id, album_type, duration_ms, track_id),
    )


def backfill_history(batch_size: int = 500):
    log_event(JOB_NAME, f"Starting backfill run with batch_size={batch_size}.")
    conn = get_db_connection()
    cur = conn.cursor()

    # Select a batch of missing track_ids
    track_ids = fetch_missing_track_ids(cur, batch_size)
    if not track_ids:
        log_event(JOB_NAME, "Nothing to backfill. All rows have artist_id/album_id/album_type.")
        print("✅ Nothing to backfill. All rows have artist_id/album_id/album_type.")
        cur.close(); conn.close()
        return

    log_event(JOB_NAME, f"Selected {len(track_ids)} track_ids; processing in chunks of {CHUNK}…")
    print(f"🔎 Selected {len(track_ids)} track_ids needing metadata. Processing in chunks of {CHUNK}…")

    sp = get_spotify_client()

    processed = 0
    for i in range(0, len(track_ids), CHUNK):
        chunk = track_ids[i:i+CHUNK]
        try:
            resp = sp.tracks(chunk)
        except spotipy.SpotifyException as e:
            log_event(JOB_NAME, f"Spotify API error: {e}. Retrying after 5s.")
            print(f"⚠️ Spotify API error: {e}. Sleeping 5s and continuing…")
            time.sleep(5)
            continue

        tracks = (resp or {}).get("tracks", []) or []
        for t in tracks:
            if not t:
                continue
            tid = t.get("id")
            album = t.get("album") or {}
            artists = t.get("artists") or []

            album_id = album.get("id")
            album_name = album.get("name")
            album_type = album.get("album_type")  # 'album' | 'single' | 'compilation'
            release_date = album.get("release_date")
            image_url = None
            images = album.get("images") or []
            if images:
                image_url = images[0].get("url")

            primary_artist_id = artists[0].get("id") if artists else None
            primary_artist_name = artists[0].get("name") if artists else None
            track_duration = t.get("duration_ms")

            # Upsert catalog rows
            upsert_artist(cur, primary_artist_id, primary_artist_name)
            upsert_album(cur, album_id, album_name, primary_artist_name, primary_artist_id, release_date, album_type, image_url)

            # Update all history rows for this track_id that are still missing data
            update_history_rows(cur, tid, primary_artist_id, album_id, album_type, track_duration)

        processed += len(chunk)
        # Gentle pacing; spotipy handles retry headers but we avoid hammering
        time.sleep(0.2)

    conn.commit()
    log_event(JOB_NAME, f"Batch committed. Tracks processed this run: {processed}.")
    cur.close(); conn.close()

    print(f"✅ Backfill committed. Tracks processed this run: {processed}. Run again to continue.")
    log_event(JOB_NAME, "Run complete.")


def parse_args():
    ap = argparse.ArgumentParser(description="Backfill spotify_play_history metadata in batches")
    ap.add_argument("--batch-size", type=int, default=500, help="Number of distinct track_ids to enrich per run (default 500)")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log_event(JOB_NAME, "Invocation received.")
    backfill_history(batch_size=args.batch_size)