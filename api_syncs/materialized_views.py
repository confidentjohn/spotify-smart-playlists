import os
from utils.logger import log_event
from utils.db_utils import get_db_connection

UNIFIED_TRACKS_VIEW = """
CREATE MATERIALIZED VIEW unified_tracks AS
-- Step 0: Classify plays with resume detection
WITH classified_plays AS (
  SELECT
    p.*,
    COALESCE(t.duration_ms, lt.duration_ms) AS duration_ms,
    LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) AS previous_played_at,
    CASE
      WHEN LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) IS NOT NULL
           AND EXTRACT(EPOCH FROM (p.played_at - LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at))) * 1000
               < COALESCE(t.duration_ms, lt.duration_ms)
      THEN TRUE
      ELSE FALSE
    END AS is_resume
  FROM plays p
  LEFT JOIN tracks t ON p.track_id = t.id
  LEFT JOIN liked_tracks lt ON p.track_id = lt.track_id
)

-- Step 1: Tracks from albums (with enriched metadata and merged liked info)
SELECT 
    t.id AS track_id,
    t.name AS track_name,
    COALESCE(a.artist, lt.track_artist) AS artist,
    COALESCE(a.artist_id, lt.artist_id) AS artist_id,
    t.album_id,
    a.name AS album_name,
    a.album_type,
    a.album_image_url AS album_image_url,
    a.release_date,
    ar.genres,
    ar.image_url AS artist_image,
    t.track_number,
    COALESCE(t.disc_number, 1) AS disc_number,
    t.added_at,
    t.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
    t.duration_ms,
    COALESCE(lt.popularity, NULL) AS popularity,
    lt.liked_at AS liked_at,
    lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
    lt.last_checked_at AS last_checked_at,
    ta.is_playable,
    COUNT(p.played_at) AS real_play_count,
    SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END) AS resume_count,
    (COUNT(p.played_at) - SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END)) AS play_count,
    MIN(p.played_at) AS first_played_at,
    MIN(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS first_played_at_est,
    MAX(p.played_at) AS last_played_at,
    MAX(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS last_played_at_est,
    CASE 
        WHEN lt.liked_at IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_liked,
    EXISTS (
        SELECT 1 FROM excluded_tracks et WHERE et.track_id = t.id
    ) AS excluded
FROM tracks t
JOIN albums a ON t.album_id = a.id
LEFT JOIN liked_tracks lt ON lt.track_id = t.id
LEFT JOIN track_availability ta ON ta.track_id = t.id
LEFT JOIN classified_plays p ON p.track_id = t.id
LEFT JOIN artists ar ON ar.id = COALESCE(a.artist_id, lt.artist_id)
WHERE a.is_saved = TRUE
GROUP BY 
    t.id, t.name, a.artist, lt.track_artist, COALESCE(a.artist_id, lt.artist_id), t.album_id, a.name, a.album_type, a.album_image_url, a.release_date,
    ar.genres, ar.image_url, t.track_number, COALESCE(t.disc_number, 1), t.added_at, t.duration_ms, lt.liked_at, lt.last_checked_at, ta.is_playable, popularity, excluded

UNION ALL

-- Step 2: Liked tracks not in album-based `tracks` table
SELECT 
    lt.track_id,
    lt.track_name,
    lt.track_artist AS artist,
    lt.artist_id,
    NULL AS album_id,
    NULL AS album_name,
    NULL AS album_type,
    NULL AS album_image_url,
    NULL AS release_date,
    ar.genres,
    ar.image_url AS artist_image,
    NULL AS track_number,
    1 AS disc_number,
    lt.added_at,
    lt.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
    lt.duration_ms,
    lt.popularity,
    lt.liked_at,
    lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
    lt.last_checked_at,
    ta.is_playable,
    COUNT(p.played_at) AS real_play_count,
    SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END) AS resume_count,
    (COUNT(p.played_at) - SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END)) AS play_count,
    MIN(p.played_at) AS first_played_at,
    MIN(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS first_played_at_est,
    MAX(p.played_at) AS last_played_at,
    MAX(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS last_played_at_est,
    TRUE AS is_liked,
    EXISTS (
        SELECT 1 FROM excluded_tracks et WHERE et.track_id = lt.track_id
    ) AS excluded
FROM liked_tracks lt
LEFT JOIN tracks t ON lt.track_id = t.id
LEFT JOIN track_availability ta ON ta.track_id = lt.track_id
LEFT JOIN classified_plays p ON p.track_id = lt.track_id
LEFT JOIN artists ar ON ar.id = lt.artist_id
WHERE t.id IS NULL
GROUP BY lt.track_id, lt.track_name, lt.track_artist, lt.artist_id, lt.added_at, lt.liked_at, lt.last_checked_at, ta.is_playable, ar.genres, ar.image_url, lt.duration_ms, lt.popularity, excluded;
"""

if __name__ == "__main__":
    log_event("build_unified_tracks", "Starting unified_tracks materialized view build...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS unified_tracks;")
    cur.execute(UNIFIED_TRACKS_VIEW)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM unified_tracks;")
    row_count = cur.fetchone()[0]
    log_event("build_unified_tracks", f"✅ unified_tracks view built successfully with {row_count} rows.")
    cur.close()
    conn.close()