import os
from utils.logger import log_event
from utils.db_utils import get_db_connection

UNIFIED_TRACKS_VIEW = """
CREATE MATERIALIZED VIEW unified_tracks AS
-- Step 0: Fuzzy match plays to tracks
WITH fuzzy_matched_tracks AS (
  SELECT 
    p.id AS play_id,
    t.id AS matched_track_id
  FROM plays p
  JOIN tracks t
    ON LOWER(p.track_name) = LOWER(t.name)
   AND LOWER(p.artist_name) = LOWER(t.artist)
   AND ABS(COALESCE(p.duration_ms, 0) - COALESCE(t.duration_ms, 0)) <= 1000
  WHERE p.track_id != t.id
),

-- Step 1: Classify plays with resume and skip detection
classified_plays AS (
  SELECT
    p.*,
    fmt.matched_track_id,
    COALESCE(t.duration_ms, lt.duration_ms) AS duration_ms,
    LAG(p.played_at) OVER (PARTITION BY COALESCE(fmt.matched_track_id, p.track_id) ORDER BY p.played_at) AS previous_played_at,
    LEAD(p.track_id) OVER (ORDER BY p.played_at) AS next_track_id,
    LEAD(p.played_at) OVER (ORDER BY p.played_at) AS next_played_at,
    CASE
      WHEN LAG(p.played_at) OVER (PARTITION BY COALESCE(fmt.matched_track_id, p.track_id) ORDER BY p.played_at) IS NOT NULL
           AND EXTRACT(EPOCH FROM (p.played_at - LAG(p.played_at) OVER (PARTITION BY COALESCE(fmt.matched_track_id, p.track_id) ORDER BY p.played_at))) * 1000
               < COALESCE(t.duration_ms, lt.duration_ms)
      THEN TRUE ELSE FALSE
    END AS is_resume,
    CASE
      WHEN LEAD(p.track_id) OVER (ORDER BY p.played_at) IS NOT NULL
           AND LEAD(p.track_id) OVER (ORDER BY p.played_at) != p.track_id
           AND EXTRACT(EPOCH FROM (LEAD(p.played_at) OVER (ORDER BY p.played_at) - p.played_at)) * 1000
               < (COALESCE(t.duration_ms, lt.duration_ms) * 0.3)
      THEN TRUE ELSE FALSE
    END AS is_skipped
  FROM plays p
  LEFT JOIN fuzzy_matched_tracks fmt ON fmt.play_id = p.id
  LEFT JOIN tracks t ON COALESCE(fmt.matched_track_id, p.track_id) = t.id
  LEFT JOIN liked_tracks lt ON COALESCE(fmt.matched_track_id, p.track_id) = lt.track_id
)

-- Step 2: Tracks from albums (with enriched metadata and merged liked info)
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
    lt.liked_at,
    lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
    lt.last_checked_at,
    ta.is_playable,
    COUNT(CASE WHEN p.matched_track_id IS NULL THEN 1 END) AS real_play_count,
    SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END) AS resume_count,
    SUM(CASE WHEN p.is_skipped THEN 1 ELSE 0 END) AS skip_count,
    COUNT(CASE WHEN p.matched_track_id IS NOT NULL THEN 1 END) AS fuzzy_match_play_count,
    GREATEST(
    COUNT(p.played_at) - 
    SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END),
    0
    ) AS play_count,
    MIN(p.played_at) AS first_played_at,
    MIN(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS first_played_at_est,
    MAX(p.played_at) AS last_played_at,
    MAX(p.played_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS last_played_at_est,
    CASE WHEN lt.liked_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_liked,
    EXISTS (
        SELECT 1 FROM excluded_tracks et WHERE et.track_id = t.id
    ) AS excluded
FROM tracks t
JOIN albums a ON t.album_id = a.id
LEFT JOIN liked_tracks lt ON lt.track_id = t.id
LEFT JOIN track_availability ta ON ta.track_id = t.id
LEFT JOIN classified_plays p ON COALESCE(p.matched_track_id, p.track_id) = t.id
LEFT JOIN artists ar ON ar.id = COALESCE(a.artist_id, lt.artist_id)
WHERE a.is_saved = TRUE
GROUP BY 
    t.id, t.name, a.artist, lt.track_artist, COALESCE(a.artist_id, lt.artist_id), t.album_id, a.name, a.album_type, a.album_image_url, a.release_date,
    ar.genres, ar.image_url, t.track_number, COALESCE(t.disc_number, 1), t.added_at, t.duration_ms, lt.liked_at, lt.last_checked_at, ta.is_playable, popularity, excluded

UNION ALL

-- Step 3: Liked tracks not in album-based `tracks` table
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
    COUNT(CASE WHEN p.matched_track_id IS NULL THEN 1 END) AS real_play_count,
    SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END) AS resume_count,
    SUM(CASE WHEN p.is_skipped THEN 1 ELSE 0 END) AS skip_count,
    COUNT(CASE WHEN p.matched_track_id IS NOT NULL THEN 1 END) AS fuzzy_match_play_count,
    GREATEST(
    COUNT(p.played_at) - SUM(CASE WHEN p.is_resume THEN 1 ELSE 0 END),
    0
    ) AS play_count,
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
LEFT JOIN classified_plays p ON COALESCE(p.matched_track_id, p.track_id) = lt.track_id
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