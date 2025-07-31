import os
from utils.logger import log_event
from utils.db_utils import get_db_connection

UNIFIED_TRACKS_VIEW = """
CREATE MATERIALIZED VIEW unified_tracks AS
-- Step 1: Merge tracks and liked_tracks into a unified base
WITH base_tracks AS (
    SELECT
        t.id AS track_id,
        t.name AS track_name,
        COALESCE(a.artist, lt.track_artist) AS artist,
        COALESCE(a.artist_id, lt.artist_id) AS artist_id,
        t.album_id,
        a.name AS album_name,
        a.album_type,
        a.album_image_url,
        a.release_date,
        ar.genres,
        ar.image_url AS artist_image,
        t.track_number,
        COALESCE(t.disc_number, 1) AS disc_number,
        t.added_at,
        t.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
        t.duration_ms,
        lt.popularity,
        lt.liked_at,
        lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
        lt.last_checked_at,
        ta.is_playable,
        CASE WHEN lt.liked_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_liked,
        EXISTS (
            SELECT 1 FROM excluded_tracks et WHERE et.track_id = t.id
        ) AS excluded
    FROM tracks t
    JOIN albums a ON t.album_id = a.id
    LEFT JOIN liked_tracks lt ON lt.track_id = t.id
    LEFT JOIN track_availability ta ON ta.track_id = t.id
    LEFT JOIN artists ar ON ar.id = COALESCE(a.artist_id, lt.artist_id)
    WHERE a.is_saved = TRUE

    UNION ALL

    SELECT
        lt.track_id,
        lt.track_name,
        lt.track_artist,
        lt.artist_id,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        ar.genres,
        ar.image_url,
        NULL,
        1,
        lt.added_at,
        lt.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        lt.duration_ms,
        lt.popularity,
        lt.liked_at,
        lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        lt.last_checked_at,
        ta.is_playable,
        TRUE,
        EXISTS (
            SELECT 1 FROM excluded_tracks et WHERE et.track_id = lt.track_id
        )
    FROM liked_tracks lt
    LEFT JOIN tracks t ON lt.track_id = t.id
    LEFT JOIN track_availability ta ON ta.track_id = lt.track_id
    LEFT JOIN artists ar ON ar.id = lt.artist_id
    WHERE t.id IS NULL
),

-- Step 2: Aggregate play stats from exact track_id matches
play_stats AS (
    SELECT
        track_id,
        COUNT(*) AS library_play_count,
        MIN(played_at) AS library_play_count_first_played,
        MAX(played_at) AS library_play_count_last_played
    FROM plays
    WHERE played_at IS NOT NULL
    GROUP BY track_id
),

-- Step 3: Classify skips and resumes from plays
classified_plays AS (
    SELECT
        p.track_id,
        COALESCE(t.duration_ms, lt.duration_ms) AS duration_ms,
        LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) AS prev_played,
        LEAD(p.track_id) OVER (ORDER BY p.played_at) AS next_track_id,
        LEAD(p.played_at) OVER (ORDER BY p.played_at) AS next_played,
        CASE
            WHEN LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) IS NOT NULL
                 AND EXTRACT(EPOCH FROM (p.played_at - LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at))) * 1000
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
    LEFT JOIN tracks t ON p.track_id = t.id
    LEFT JOIN liked_tracks lt ON p.track_id = lt.track_id
    WHERE p.played_at IS NOT NULL
),

-- Step 4: Aggregate resume/skip counts
play_behavior_stats AS (
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE is_resume) AS resume_play_count,
        COUNT(*) FILTER (WHERE is_skipped) AS skip_play_count
    FROM classified_plays
    GROUP BY track_id
),

-- Step 5: Fuzzy match plays to tracks
fuzzy_matched_tracks AS (
  SELECT 
    p.id AS play_id,
    t.id AS matched_track_id,
    p.played_at AS fuzzy_played_at
  FROM plays p
  JOIN tracks t
    ON LOWER(p.track_name) = LOWER(t.name)
   AND LOWER(p.artist_name) = LOWER(t.artist)
   AND ABS(COALESCE(p.duration_ms, 0) - COALESCE(t.duration_ms, 0)) <= 1000
  WHERE p.played_at IS NOT NULL
    AND p.track_id IS DISTINCT FROM t.id
),

-- Step 6: Aggregate fuzzy match stats
fuzzy_play_stats AS (
  SELECT
    matched_track_id AS track_id,
    COUNT(*) AS fuzz_play_count,
    MIN(fuzzy_played_at) AS fuzz_play_count_first_played,
    MAX(fuzzy_played_at) AS fuzz_play_count_last_played
  FROM fuzzy_matched_tracks
  GROUP BY matched_track_id
)

-- Final SELECT: Merge everything together
SELECT
    bt.*,
    COALESCE(ps.library_play_count, 0) AS library_play_count,
    ps.library_play_count_first_played,
    ps.library_play_count_last_played,
    COALESCE(pb.resume_play_count, 0) AS resume_play_count,
    COALESCE(pb.skip_play_count, 0) AS skip_play_count,
    COALESCE(fp.fuzz_play_count, 0) AS fuzz_play_count,
    fp.fuzz_play_count_first_played,
    fp.fuzz_play_count_last_played,

    -- Combined logic
    GREATEST(
      COALESCE(ps.library_play_count, 0) +
      COALESCE(fp.fuzz_play_count, 0) -
      COALESCE(pb.resume_play_count, 0),
    0) AS play_count,

    LEAST(
      ps.library_play_count_first_played,
      fp.fuzz_play_count_first_played
    ) AS first_played_at,

    LEAST(
      ps.library_play_count_first_played,
      fp.fuzz_play_count_first_played
    ) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS first_played_at_est,

    GREATEST(
      ps.library_play_count_last_played,
      fp.fuzz_play_count_last_played
    ) AS last_played_at,

    GREATEST(
      ps.library_play_count_last_played,
      fp.fuzz_play_count_last_played
    ) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS last_played_at_est

FROM base_tracks bt
LEFT JOIN play_stats ps ON ps.track_id = bt.track_id
LEFT JOIN play_behavior_stats pb ON pb.track_id = bt.track_id
LEFT JOIN fuzzy_play_stats fp ON fp.track_id = bt.track_id
ORDER BY bt.artist, bt.album_id, bt.disc_number, bt.track_number;
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