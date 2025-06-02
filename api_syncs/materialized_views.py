import os
import psycopg2
from utils.logger import log_event

UNIFIED_TRACKS_VIEW = """
CREATE MATERIALIZED VIEW IF NOT EXISTS unified_tracks AS
-- Tracks from albums (with enriched metadata)
SELECT 
    t.id AS track_id,
    t.name AS track_name,
    COALESCE(a.artist, lt.track_artist) AS artist,
    t.album_id,
    a.name AS album_name,
    a.release_date,
    t.track_number,
    t.added_at,
    COALESCE(lt.liked_at, t.date_liked_at) AS liked_at,
    COALESCE(lt.last_checked_at, t.date_liked_checked) AS last_checked_at,
    ta.is_playable,
    COUNT(p.played_at) AS play_count,
    MIN(p.played_at) AS first_played_at,
    MAX(p.played_at) AS last_played_at,
    CASE 
        WHEN COALESCE(lt.liked_at, t.date_liked_at) IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_liked
FROM tracks t
JOIN albums a ON t.album_id = a.id
LEFT JOIN liked_tracks lt ON lt.track_id = t.id
LEFT JOIN track_availability ta ON ta.track_id = t.id
LEFT JOIN plays p ON p.track_id = t.id
WHERE a.is_saved = TRUE
GROUP BY 
    t.id, t.name, a.artist, lt.track_artist, t.album_id, a.name, a.release_date,
    t.track_number, t.added_at, lt.liked_at, lt.last_checked_at, ta.is_playable,
    t.date_liked_at, t.date_liked_checked

UNION ALL

-- Liked tracks not in album-based `tracks` table
SELECT 
    lt.track_id,
    lt.track_name,
    lt.track_artist AS artist,
    NULL AS album_id,
    NULL AS album_name,
    NULL AS release_date,
    NULL AS track_number,
    lt.added_at,
    lt.liked_at,
    lt.last_checked_at,
    ta.is_playable,
    COUNT(p.played_at) AS play_count,
    MIN(p.played_at) AS first_played_at,
    MAX(p.played_at) AS last_played_at,
    TRUE AS is_liked
FROM liked_tracks lt
LEFT JOIN tracks t ON lt.track_id = t.id
LEFT JOIN track_availability ta ON ta.track_id = lt.track_id
LEFT JOIN plays p ON p.track_id = lt.track_id
WHERE t.id IS NULL
GROUP BY lt.track_id, lt.track_name, lt.track_artist, lt.added_at, lt.liked_at, lt.last_checked_at, ta.is_playable;
"""

if __name__ == "__main__":
    log_event("build_unified_tracks", "Starting unified_tracks materialized view build...")
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ.get('DB_PORT', 5432),
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS unified_tracks;")
    cur.execute(UNIFIED_TRACKS_VIEW)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM unified_tracks;")
    row_count = cur.fetchone()[0]
    log_event("build_unified_tracks", f"✅ unified_tracks view built successfully with {row_count} rows.")
    cur.close()
    conn.close()
    # log_event("build_unified_tracks", "✅ unified_tracks view built successfully.")