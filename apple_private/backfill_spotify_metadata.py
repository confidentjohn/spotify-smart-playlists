#!/usr/bin/env python3
import os
import psycopg2
import requests
from utils.db_utils import get_db_connection
from utils.spotify_auth import get_spotify_token  # assumes you have this working

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

def fetch_spotify_metadata(token, track_id):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"[error] Spotify fetch failed for {track_id}: {r.status_code} {r.text}")
        return None

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
    conn.commit()
    cur.close()

def main():
    token = get_spotify_token()
    pending = fetch_pending_tracks(limit=100)
    print(f"Found {len(pending)} tracks to backfill")
    if not pending:
        return

    conn = get_db_connection()

    for apple_id, spotify_id in pending:
        track = fetch_spotify_metadata(token, spotify_id)
        if not track:
            update_row(conn, apple_id, None, success=False)
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
        print(f"✅ Updated Apple ID {apple_id} with Spotify metadata")

    conn.close()

if __name__ == "__main__":
    main()