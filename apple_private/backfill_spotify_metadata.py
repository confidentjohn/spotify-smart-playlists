#!/usr/bin/env python3
import os
import sys
import time
import psycopg2
import requests
from spotipy.exceptions import SpotifyException

# ─────────────────────────────────────────────
# Ensure utils is in path
# ─────────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import get_db_connection
from utils.spotify_auth import get_spotify_client
from utils.logger import log_event

# ─────────────────────────────────────────────
# Safe Spotify API Wrapper
# ─────────────────────────────────────────────

def safe_spotify_call(func, *args, **kwargs):
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if getattr(e, "http_status", None) == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 5))
                retries += 1
                log_event("apple_spotify_backfill", f"Rate limit hit. Retry #{retries} in {retry_after}s")
                time.sleep(retry_after)
            elif getattr(e, "http_status", None) == 401:
                # token expired – bubble up so caller can refresh client once
                raise
            else:
                log_event("apple_spotify_backfill", f"Spotify error: {e}", level="error")
                raise
        except requests.exceptions.ReadTimeout as e:
            retries += 1
            log_event("apple_spotify_backfill", f"Timeout hit. Retry #{retries} in 5s... ({e})")
            time.sleep(5)

# ─────────────────────────────────────────────
# Setup Spotify client
# ─────────────────────────────────────────────
sp = get_spotify_client()
MARKET = os.getenv("ISRC_LINK_MARKET", "US")

def fetch_pending_tracks(limit=100):
    """Fetch Apple rows with spotify_track_id but missing metadata and not yet checked"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT apple_track_id, spotify_track_id
        FROM apple_unique_track_ids
        WHERE spotify_track_id IS NOT NULL
          AND spotify_track_name IS NULL
          AND checked_at_spotify_back_fill IS NULL
        LIMIT %s;
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_spotify_metadata(track_id):
    global sp
    try:
        tr = safe_spotify_call(sp.track, track_id, market=MARKET)
    except SpotifyException as e:
        if getattr(e, "http_status", None) == 401:
            log_event("apple_spotify_backfill", "Spotify token expired; refreshing and retrying once")
            sp = get_spotify_client()
            tr = safe_spotify_call(sp.track, track_id, market=MARKET)
        else:
            raise
    return tr

def update_row(conn, apple_track_id, data, success=True):
    cur = conn.cursor()
    if success:
        cur.execute("""
            UPDATE apple_unique_track_ids
            SET spotify_track_name = %s,
                spotify_artist_id = %s,
                spotify_duration_ms = %s,
                spotify_artist_name = %s,
                spotify_album_id = %s,
                spotify_album_name = %s,
                spotify_album_type = %s,
                spotify_checked_at = NOW(),
                checked_at_spotify_back_fill = NOW()
            WHERE apple_track_id = %s;
        """, (
            data["spotify_track_name"],
            data["spotify_artist_id"],
            data["spotify_duration_ms"],
            data["spotify_artist_name"],
            data["spotify_album_id"],
            data["spotify_album_name"],
            data["spotify_album_type"],
            apple_track_id,
        ))
    else:
        # mark as checked even if it failed (avoid endless retries)
        cur.execute("""
            UPDATE apple_unique_track_ids
            SET checked_at_spotify_back_fill = NOW()
            WHERE apple_track_id = %s;
        """, (apple_track_id,))
    cur.close()

def main():
    pending = fetch_pending_tracks(limit=100)
    log_event("apple_spotify_backfill", f"Found {len(pending)} tracks to backfill")
    if not pending:
        return

    conn = get_db_connection()
    processed = 0

    for apple_id, spotify_id in pending:
        try:
            track = fetch_spotify_metadata(spotify_id)
        except Exception as e:
            log_event("apple_spotify_backfill", f"Failed to fetch metadata for {spotify_id}: {e}", level="error")
            track = None
        if not track:
            update_row(conn, apple_id, None, success=False)
            processed += 1
            if processed % 100 == 0:
                conn.commit()
                log_event("apple_spotify_backfill", f"Committed batch at {processed}")
            continue

        data = {
            "spotify_track_name": track.get("name"),
            "spotify_artist_id": track["artists"][0]["id"] if track.get("artists") else None,
            "spotify_duration_ms": track.get("duration_ms"),
            "spotify_artist_name": track["artists"][0]["name"] if track.get("artists") else None,
            "spotify_album_id": track["album"]["id"] if track.get("album") else None,
            "spotify_album_name": track["album"]["name"] if track.get("album") else None,
            "spotify_album_type": track["album"]["album_type"] if track.get("album") else None,
        }

        update_row(conn, apple_id, data, success=True)
        processed += 1
        if processed % 100 == 0:
            conn.commit()
            log_event("apple_spotify_backfill", f"Committed batch at {processed}")

    conn.commit()
    log_event("apple_spotify_backfill", f"Done. processed={processed}")
    conn.close()

if __name__ == "__main__":
    main()