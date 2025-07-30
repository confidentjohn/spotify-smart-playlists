

import time
import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from utils.db_utils import get_db_connection as get_conn
from utils.spotify_auth import get_spotify_client

logging.basicConfig(level=logging.INFO)

sp = get_spotify_client()

BATCH_SIZE = 50

def get_missing_track_ids(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT track_id
            FROM plays
            WHERE track_name IS NULL
               OR artist_id IS NULL
               OR artist_name IS NULL
               OR duration_ms IS NULL
               OR album_id IS NULL
               OR album_name IS NULL
               OR album_type IS NULL
        """)
        return [row[0] for row in cur.fetchall()]

def update_track_metadata(conn, metadata):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE plays
            SET track_name = %s,
                artist_id = %s,
                artist_name = %s,
                duration_ms = %s,
                album_id = %s,
                album_name = %s,
                album_type = %s
            WHERE track_id = %s
        """, (
            metadata['track_name'],
            metadata['artist_id'],
            metadata['artist_name'],
            metadata['duration_ms'],
            metadata['album_id'],
            metadata['album_name'],
            metadata['album_type'],
            metadata['track_id']
        ))

def fetch_track_metadata(track_id):
    try:
        track = sp.track(track_id)
        return {
            'track_id': track['id'],
            'track_name': track['name'],
            'artist_id': track['artists'][0]['id'] if track['artists'] else None,
            'artist_name': track['artists'][0]['name'] if track['artists'] else None,
            'duration_ms': track['duration_ms'],
            'album_id': track['album']['id'],
            'album_name': track['album']['name'],
            'album_type': track['album']['album_type'],
        }
    except Exception as e:
        logging.warning(f"Failed to fetch metadata for track {track_id}: {e}")
        return None

def main():
    conn = get_conn()
    try:
        track_ids = get_missing_track_ids(conn)
        logging.info(f"Found {len(track_ids)} track(s) with missing metadata.")

        for i, track_id in enumerate(track_ids):
            metadata = fetch_track_metadata(track_id)
            if metadata:
                update_track_metadata(conn, metadata)
                conn.commit()
                logging.info(f"Updated track {i+1}/{len(track_ids)}: {track_id}")
            time.sleep(0.1)  # Be gentle to avoid rate limits
    finally:
        conn.close()

if __name__ == "__main__":
    main()