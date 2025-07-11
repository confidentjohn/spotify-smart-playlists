from utils.logger import log_event


import os
from datetime import datetime
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection

def fetch_artists_metadata(sp, artist_ids):
    metadata = []
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i+50]
        response = sp.artists(batch)
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

    # Get all unique artist IDs from albums and liked_tracks
    cur.execute("SELECT DISTINCT artist_id FROM albums")
    album_artists = {row[0] for row in cur.fetchall()}

    cur.execute("SELECT DISTINCT artist_id FROM liked_tracks")
    liked_artists = {row[0] for row in cur.fetchall()}

    all_artist_ids = list(album_artists.union(liked_artists))
    if not all_artist_ids:
        log_event("No artist IDs found.")
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
    log_event(f"✔️ Synced {len(artists)} artists.")

if __name__ == "__main__":
    main()