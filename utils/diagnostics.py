from utils.db_utils import get_db_connection

def get_duplicate_album_track_counts():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            a.id AS album_id,
            a.name AS album_name,
            a.total_tracks,
            COUNT(t.id) AS track_count,
            COUNT(t.id) - a.total_tracks AS extra_tracks
        FROM albums a
        JOIN tracks t ON t.album_id = a.id
        GROUP BY a.id, a.name, a.total_tracks
        HAVING COUNT(t.id) > a.total_tracks
        ORDER BY extra_tracks DESC
    """)
    results = cur.fetchall()

    cur.close()
    conn.close()
    return results


def get_fuzzy_matched_plays():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            p.id AS play_id,
            p.track_id AS original_track_id,
            p.track_name AS original_track_name,
            p.artist_name AS original_artist_name,
            p.played_at,

            t.id AS library_track_id,
            t.name AS library_track_name,
            t.artist AS library_artist_name,
            t.album_id AS library_album_id,
            a.name AS library_album_name

        FROM plays p
        JOIN (
            SELECT 
                p.id AS play_id,
                t.id AS matched_track_id
            FROM plays p
            JOIN tracks t
              ON LOWER(p.track_name) = LOWER(t.name)
             AND LOWER(p.artist_name) = LOWER(t.artist)
             AND ABS(COALESCE(p.duration_ms, 0) - COALESCE(t.duration_ms, 0)) <= 1000
            WHERE p.track_id != t.id
        ) fmt ON fmt.play_id = p.id

        JOIN tracks t ON t.id = fmt.matched_track_id
        LEFT JOIN albums a ON t.album_id = a.id
        LEFT JOIN resolved_fuzzy_matches rfm ON rfm.track_id = t.id
        WHERE rfm.track_id IS NULL
        ORDER BY p.played_at DESC;
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


# Function to get outdated albums
def get_outdated_albums():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            artist_id,
            artist_name,
            album_name,
            saved_album_id,
            newer_album_id,
            first_detected_at,
            last_checked_at
        FROM outdated_albums
        ORDER BY last_checked_at DESC
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def get_track_count_mismatches():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            a.id AS album_id,
            a.name AS album_name,
            ar.name AS artist_name,
            a.total_tracks AS expected_track_count,
            COUNT(t.id) AS actual_track_count,
            (a.total_tracks - COUNT(t.id)) AS track_count_difference
        FROM albums a
        LEFT JOIN tracks t ON t.album_id = a.id
        LEFT JOIN artists ar ON ar.id = a.artist_id
        WHERE a.is_saved = TRUE AND a.tracks_synced = TRUE
        GROUP BY a.id, a.name, ar.name, a.total_tracks
        HAVING COUNT(t.id) != a.total_tracks
        ORDER BY track_count_difference DESC
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()
    return results
def get_pending_deletion_playlists():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT name, slug, missing_count, last_missing_at
        FROM playlist_mappings
        WHERE pending_delete = TRUE
        ORDER BY last_missing_at DESC
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

