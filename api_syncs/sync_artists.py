from utils.logger import log_event


import os
from datetime import datetime
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection

def fetch_artists_metadata(sp, artist_ids):
    metadata = []
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i+50]
        valid_batch = [a for a in batch if a]  # Skip None or empty strings
        if not valid_batch:
            log_event("sync_artists", "⚠️ Skipping empty artist batch")
            continue
        response = sp.artists(valid_batch)
        for artist in response['artists']:
            genres = artist.get('genres', [])
            images = artist.get('images', [])
            image_url = images[0]['url'] if images else None
            metadata.append({
                'id': artist['id'],
                'name': artist['name'],
                'genres': genres,
                'image_url': image_url,
            })
    return metadata

def main():
    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.utcnow()

    # Get all unique artist IDs from albums, liked_tracks, spotify_play_history, apple_music_play_history, and plays
    cur.execute("""
        SELECT DISTINCT artist_id FROM (
            SELECT artist_id FROM albums
            UNION
            SELECT artist_id FROM liked_tracks
            UNION
            SELECT artist_id FROM spotify_play_history
            UNION
            SELECT artist_id FROM apple_music_play_history
            UNION
            SELECT artist_id FROM plays
        ) AS combined
    """)
    all_known_artist_ids = {row[0] for row in cur.fetchall()}

    # Artists not yet in the artists table
    cur.execute("SELECT id FROM artists")
    existing_artist_ids = {row[0] for row in cur.fetchall()}
    new_artist_ids = list(all_known_artist_ids - existing_artist_ids)

    # Oldest 100 existing artists
    cur.execute("""
        SELECT id FROM artists
        ORDER BY COALESCE(last_checked_at, '2000-01-01') ASC
        LIMIT 100
    """)
    oldest_artist_ids = [row[0] for row in cur.fetchall()]

    # Combine new and old artist IDs
    all_artist_ids = list(set(new_artist_ids + oldest_artist_ids))

    if not all_artist_ids:
        log_event("sync_artists", "No artist IDs found to sync.")
        return

    sp = get_spotify_client()
    artists = fetch_artists_metadata(sp, all_artist_ids)

    for artist in artists:
        cur.execute("""
            INSERT INTO artists (id, name, genres, image_url, last_checked_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                genres = EXCLUDED.genres,
                image_url = EXCLUDED.image_url,
                last_checked_at = EXCLUDED.last_checked_at;
        """, (artist['id'], artist['name'], artist['genres'], artist['image_url'], now))

    conn.commit()
    cur.close()
    conn.close()
    log_event("sync_artists", f"✔️ Synced {len(artists)} artists.")

if __name__ == "__main__":
    main()