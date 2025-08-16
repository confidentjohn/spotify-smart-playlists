import time
import logging
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection
from utils.logger import log_event

BATCH_SIZE = 30

def get_stale_artists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            (
              SELECT id, name FROM artists
              WHERE id IN (SELECT DISTINCT artist_id FROM outdated_albums)
            )
            UNION
            (
              SELECT id, name FROM artists
              ORDER BY COALESCE(last_album_checked_at, '2000-01-01') ASC
              LIMIT %s
            );
        """, (BATCH_SIZE,))
        return cur.fetchall()

def get_albums_by_artist(sp, artist_id):
    albums = []
    results = sp.artist_albums(artist_id, album_type='album', limit=50)
    albums.extend(results['items'])
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])
        time.sleep(0.1)
    return albums

def main():
    conn = get_db_connection()
    sp = get_spotify_client()

    stale_artists = get_stale_artists(conn)

    for artist_id, artist_name in stale_artists:
        logging.info(f"Checking albums for: {artist_name}")
        log_event("check_canonical_albums", f"Checking albums for: {artist_name}")
        try:
            albums = get_albums_by_artist(sp, artist_id)

            with conn.cursor() as cur:
                # Get saved albums for this artist
                cur.execute("""
                    SELECT id, name FROM albums WHERE artist_id = %s AND is_saved = TRUE;
                """, (artist_id,))
                saved_albums = cur.fetchall()

                # Get current outdated entries for this artist
                cur.execute("""
                    SELECT saved_album_id, album_name, newer_album_id
                    FROM outdated_albums
                    WHERE artist_id = %s;
                """, (artist_id,))
                outdated_rows = cur.fetchall()
                outdated_lookup = {(row[1], row[2]): row[0] for row in outdated_rows}  # {(album_name, newer_album_id): saved_album_id}

            saved_album_ids = {row[0] for row in saved_albums}
            saved_album_names = {row[1] for row in saved_albums}
            remote_album_lookup = {
                (album['name'], album['album_type'], album['release_date'][:4]): album['id']
                for album in albums if 'release_date' in album
            }

            # Identify new outdated albums
            with conn.cursor() as cur:
                for name in saved_album_names:
                    matching_saved = [id_ for id_, nm in saved_albums if nm == name]
                    for id_, nm in saved_albums:
                        if nm != name:
                            continue
                        # Get album_type and release_year from local DB
                        cur.execute("""
                            SELECT album_type, release_date FROM albums WHERE id = %s;
                        """, (id_,))
                        row = cur.fetchone()
                        if not row:
                            continue
                        album_type, release_date = row
                        release_year = release_date[:4] if release_date else None
                        if not release_year:
                            continue

                        remote_id = remote_album_lookup.get((name, album_type, str(release_year)))
                        if remote_id and remote_id not in matching_saved:
                            if (name, remote_id) not in outdated_lookup:
                                logging.info(f"Inserting outdated album: {name} from {id_} to {remote_id}")
                                log_event("check_canonical_albums", f"Inserting outdated album: {name} from {id_} to {remote_id}")
                                cur.execute("""
                                    INSERT INTO outdated_albums (artist_id, artist_name, album_name, saved_album_id, newer_album_id)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (saved_album_id, newer_album_id) DO NOTHING;
                                """, (artist_id, artist_name, name, id_, remote_id))

                # Check for resolved outdated albums
                for (album_name, newer_album_id), saved_album_id in outdated_lookup.items():
                    if album_name in saved_album_names:
                        matching_saved = [id_ for id_, nm in saved_albums if nm == album_name]
                        if newer_album_id in matching_saved:
                            logging.info(f"Removing resolved outdated album: {album_name}")
                            log_event("check_canonical_albums", f"Resolved outdated album: {album_name}")
                            cur.execute("""
                                DELETE FROM outdated_albums
                                WHERE saved_album_id = %s AND newer_album_id = %s;
                            """, (saved_album_id, newer_album_id))

            # Update timestamp
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE artists SET last_album_checked_at = NOW() WHERE id = %s;
                """, (artist_id,))
            conn.commit()

        except Exception as e:
            logging.error(f"Error checking artist {artist_name}: {e}")
            log_event("check_canonical_albums", f"Error checking artist {artist_name}: {e}")

    conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()