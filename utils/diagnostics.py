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