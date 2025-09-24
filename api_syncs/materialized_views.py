import os
from utils.logger import log_event
from utils.db_utils import get_db_connection

UNIFIED_TRACKS_VIEW = """
CREATE MATERIALIZED VIEW unified_tracks AS
-- Step 0: Merge plays + spotify_play_history into a unified plays set
WITH all_plays AS (
    SELECT
        p.id,
        p.track_id,
        p.track_name,
        p.artist_id,
        p.artist_name,
        p.album_id,
        p.album_name,
        p.album_type,
        p.duration_ms,
        p.played_at
    FROM plays p
    WHERE p.played_at IS NOT NULL

    UNION ALL

    SELECT
        sph.id,
        sph.track_id,
        sph.track_name,
        sph.artist_id,
        sph.artist_name,
        sph.album_id,
        sph.album_name,
        sph.album_type,
        sph.duration_ms,
        sph.played_at
    FROM spotify_play_history sph
    WHERE sph.played_at IS NOT NULL
    AND sph.artist_id IS NOT NULL   -- exclude until backfilled
),

-- Step 1: Merge tracks and liked_tracks into a unified base (library source)
base_tracks AS (
    SELECT
        t.id AS track_id,
        t.name AS track_name,
        COALESCE(a.artist, lt.track_artist) AS artist,
        COALESCE(a.artist_id, lt.artist_id) AS artist_id,
        t.album_id,
        a.name AS album_name,
        a.album_type,
        a.album_image_url,
        a.release_date::text AS release_date,         -- cast to text
        ar.genres,
        ar.image_url AS artist_image,
        t.track_number,
        COALESCE(t.disc_number, 1) AS disc_number,
        t.added_at,                                    -- timestamptz
        t.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
        t.duration_ms,
        COALESCE(lt.popularity, t.popularity) AS popularity,
        lt.liked_at,                                   -- timestamptz
        lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
        lt.last_checked_at,                            -- timestamptz
        ta.is_playable,
        CASE WHEN lt.liked_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_liked,
        EXISTS (SELECT 1 FROM excluded_tracks et WHERE et.track_id = t.id) AS excluded,
        'library'::text AS track_source,
        'album'::text   AS library_origin              -- NEW
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
        lt.album_id,
        NULL,
        NULL,
        NULL,
        NULL::text AS release_date,                    -- match type to text
        ar.genres,
        ar.image_url AS artist_image,
        NULL,
        1,
        lt.added_at,                                   -- timestamptz
        lt.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        lt.duration_ms,
        COALESCE(lt.popularity, t.popularity) AS popularity,
        lt.liked_at,                                   -- timestamptz
        lt.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        lt.last_checked_at,                            -- timestamptz
        ta.is_playable,
        TRUE,
        EXISTS (SELECT 1 FROM excluded_tracks et WHERE et.track_id = lt.track_id),
        'library'::text    AS track_source,
        'liked_only'::text AS library_origin           -- NEW
    FROM liked_tracks lt
    LEFT JOIN tracks t ON lt.track_id = t.id
    LEFT JOIN track_availability ta ON ta.track_id = lt.track_id
    LEFT JOIN artists ar ON ar.id = lt.artist_id
    WHERE t.id IS NULL
),

-- Step 2: Aggregate play stats from exact track_id matches (library + non-library)
play_stats AS (
    SELECT
        track_id,
        COUNT(*) AS library_play_count,
        MIN(played_at) AS library_play_count_first_played,
        MAX(played_at) AS library_play_count_last_played
    FROM all_plays
    GROUP BY track_id
),

-- Step 3: Classify skips and resumes from plays (fallback to plays.duration_ms)
classified_plays AS (
    SELECT
        p.track_id,
        COALESCE(t.duration_ms, lt.duration_ms, p.duration_ms) AS duration_ms,
        LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) AS prev_played,
        LEAD(p.track_id) OVER (ORDER BY p.played_at) AS next_track_id,
        LEAD(p.played_at) OVER (ORDER BY p.played_at) AS next_played,
        CASE
            WHEN LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) IS NOT NULL
                 AND EXTRACT(EPOCH FROM (p.played_at - LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at))) * 1000
                     < COALESCE(t.duration_ms, lt.duration_ms, p.duration_ms)
            THEN TRUE ELSE FALSE
        END AS is_resume,
        CASE
            WHEN LEAD(p.track_id) OVER (ORDER BY p.played_at) IS NOT NULL
                 AND LEAD(p.track_id) OVER (ORDER BY p.played_at) != p.track_id
                 AND EXTRACT(EPOCH FROM (LEAD(p.played_at) OVER (ORDER BY p.played_at) - p.played_at)) * 1000
                     < (COALESCE(t.duration_ms, lt.duration_ms, p.duration_ms) * 0.3)
            THEN TRUE ELSE FALSE
        END AS is_skipped
    FROM all_plays p
    LEFT JOIN tracks t ON p.track_id = t.id
    LEFT JOIN liked_tracks lt ON p.track_id = lt.track_id
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

-- Step 5: Narrow to fuzzy candidates (no exact id in library/liked)
fuzzy_candidates AS (
  SELECT p.*
  FROM all_plays p
  LEFT JOIN tracks t  ON t.id = p.track_id
  LEFT JOIN liked_tracks l ON l.track_id = p.track_id
  WHERE p.track_id IS NOT NULL
    AND t.id IS NULL
    AND l.track_id IS NULL
),

-- Step 6: Fuzzy match only those candidates
fuzzy_matched_tracks AS (
  SELECT 
    c.id        AS play_id,
    t.id        AS matched_track_id,
    c.played_at AS fuzzy_played_at
  FROM fuzzy_candidates c
  JOIN tracks t
    ON LOWER(c.track_name) = LOWER(t.name)
   AND LOWER(c.artist_name) = LOWER(t.artist)
   AND ABS(COALESCE(c.duration_ms, 0) - COALESCE(t.duration_ms, 0)) <= 1000
),

-- Step 7: Aggregate fuzzy match stats
fuzzy_play_stats AS (
  SELECT
    matched_track_id AS track_id,
    COUNT(*) AS fuzz_play_count,
    MIN(fuzzy_played_at) AS fuzz_play_count_first_played,
    MAX(fuzzy_played_at) AS fuzz_play_count_last_played
  FROM fuzzy_matched_tracks
  GROUP BY matched_track_id
),

-- Step 8: Identify non-library track_ids from plays not in library/liked AND not used by a fuzzy match play
non_library_candidates AS (
    SELECT
        p.track_id
    FROM all_plays p
    WHERE p.track_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM tracks t WHERE t.id = p.track_id)
      AND NOT EXISTS (SELECT 1 FROM liked_tracks l WHERE l.track_id = p.track_id)
      AND NOT EXISTS (SELECT 1 FROM fuzzy_matched_tracks fmt WHERE fmt.play_id = p.id)
    GROUP BY p.track_id
),

-- Step 9: Pick the most recent play per track once (avoid per-row sorts)
latest_play_per_track AS (
    SELECT DISTINCT ON (p.track_id)
        p.*
    FROM all_plays p
    WHERE p.track_id IS NOT NULL
    ORDER BY p.track_id, p.played_at DESC
),

-- Step 10: For each candidate, pick a representative (most recent play) and enrich if available
non_library_base AS (
    SELECT
        p.track_id,
        p.track_name,
        p.artist_name AS artist,
        p.artist_id           AS artist_id,
        p.album_id            AS album_id,
        COALESCE(a2.name, p.album_name)       AS album_name,
        COALESCE(a2.album_type, p.album_type) AS album_type,
        a2.album_image_url    AS album_image_url,
        a2.release_date::text AS release_date,
        ar2.genres            AS genres,
        ar2.image_url         AS artist_image,
        NULL::int             AS track_number,
        1                     AS disc_number,
        NULL::timestamptz     AS added_at,
        NULL::timestamp       AS added_at_est,
        p.duration_ms,
        NULL::int             AS popularity,
        NULL::timestamptz     AS liked_at,
        NULL::timestamp       AS liked_at_est,
        NULL::timestamptz     AS last_checked_at,
        TRUE                  AS is_playable,
        FALSE                 AS is_liked,
        EXISTS (SELECT 1 FROM excluded_tracks et WHERE et.track_id = p.track_id) AS excluded,
        'non_library'::text   AS track_source,
        'non_library'::text   AS library_origin
    FROM non_library_candidates c
    JOIN latest_play_per_track p ON p.track_id = c.track_id
    LEFT JOIN artists ar2 ON ar2.id = p.artist_id
    LEFT JOIN albums  a2  ON a2.id = p.album_id
    LEFT JOIN track_availability ta ON ta.track_id = p.track_id
),

-- Step 11: Combine library and non-library rows before stats joins
all_base AS (
    SELECT * FROM base_tracks
    UNION ALL
    SELECT * FROM non_library_base
)

-- Final SELECT: Merge everything together
SELECT
    ab.*,
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

FROM all_base ab
LEFT JOIN play_stats ps ON ps.track_id = ab.track_id
LEFT JOIN play_behavior_stats pb ON pb.track_id = ab.track_id
LEFT JOIN fuzzy_play_stats fp ON fp.track_id = ab.track_id
;
"""

if __name__ == "__main__":
    log_event("build_unified_tracks", "Starting unified_tracks materialized view build...")
    conn = get_db_connection()
    cur = conn.cursor()
    # Prefer refreshing the MV to preserve existing MV indexes
    try:
        cur.execute("REFRESH MATERIALIZED VIEW unified_tracks;")
        conn.commit()
        log_event("build_unified_tracks", "Refreshed existing unified_tracks MV.")
    except Exception as e:
        # If the MV does not exist or definition changed, recreate once
        log_event("build_unified_tracks", f"REFRESH failed ({e}); recreating MV…")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS unified_tracks;")
        cur.execute(UNIFIED_TRACKS_VIEW)
        conn.commit()
        log_event("build_unified_tracks", "Recreated unified_tracks MV.")

    # Ensure helpful indexes exist on the materialized view
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_unified_tracks_track_id     ON unified_tracks(track_id);
        CREATE INDEX IF NOT EXISTS idx_unified_tracks_artist       ON unified_tracks(artist);
        CREATE INDEX IF NOT EXISTS idx_unified_tracks_album_id     ON unified_tracks(album_id);
        CREATE INDEX IF NOT EXISTS idx_unified_tracks_last_played  ON unified_tracks(last_played_at);
        -- accelerates the final ORDER BY for browsing
        CREATE INDEX IF NOT EXISTS idx_unified_tracks_browse_order ON unified_tracks(artist, album_id, disc_number, track_number);
        """
    )
    conn.commit()

    # Ensure helpful indexes exist on source tables used by the view
    cur.execute(
        """
        -- Plays & history: speed grouping/windowing and candidate scans
        CREATE INDEX IF NOT EXISTS idx_plays_track_time        ON plays(track_id, played_at);
        CREATE INDEX IF NOT EXISTS idx_hist_track_time         ON spotify_play_history(track_id, played_at);

        -- Functional indexes for fuzzy matching
        CREATE INDEX IF NOT EXISTS idx_plays_name_artist_lower ON plays(LOWER(track_name), LOWER(artist_name));
        CREATE INDEX IF NOT EXISTS idx_hist_name_artist_lower  ON spotify_play_history(LOWER(track_name), LOWER(artist_name));
        CREATE INDEX IF NOT EXISTS idx_tracks_name_artist_lower ON tracks(LOWER(name), LOWER(artist));

        -- Common FK/lookup helpers
        CREATE INDEX IF NOT EXISTS idx_tracks_album            ON tracks(album_id);
        CREATE INDEX IF NOT EXISTS idx_liked_tracks_track      ON liked_tracks(track_id);
        CREATE INDEX IF NOT EXISTS idx_albums_id               ON albums(id);
        CREATE INDEX IF NOT EXISTS idx_artists_id              ON artists(id);
        CREATE INDEX IF NOT EXISTS idx_availability_track      ON track_availability(track_id);
        """
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM unified_tracks;")
    row_count = cur.fetchone()[0]
    log_event("build_unified_tracks", f"✅ unified_tracks view built successfully with {row_count} rows.")
    cur.close()
    conn.close()