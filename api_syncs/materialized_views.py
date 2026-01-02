import os
from utils.logger import log_event
from utils.db_utils import get_db_connection

UNIFIED_TRACKS_VIEW = """
DROP MATERIALIZED VIEW IF EXISTS unified_tracks;

CREATE MATERIALIZED VIEW unified_tracks AS
-- Step 0: Use unified_plays_mv (canonicalized track_id via track_id_equivalents)
WITH all_plays AS (
    SELECT
        -- unified_plays_mv has no stable source id; generate a deterministic row id for internal matching
        ROW_NUMBER() OVER (
            ORDER BY
                played_at,
                track_id,
                COALESCE(duration_ms, 0),
                COALESCE(album_id, ''),
                COALESCE(track_name, ''),
                COALESCE(artist_name, '')
        ) AS play_row_id,
        track_id,
        track_name,
        artist_id,
        artist_name,
        album_id,
        album_name,
        album_type,
        duration_ms,
        played_at
    FROM unified_plays_mv
    WHERE played_at IS NOT NULL
      AND track_id IS NOT NULL
),

-- Step 1a: Canonicalize track IDs for tracks and liked_tracks using track_id_equivalents
tracks_canon_raw AS (
    SELECT
        t.*,
        COALESCE(eq.canonical_track_id, t.id) AS canonical_track_id
    FROM tracks t
    LEFT JOIN track_id_equivalents eq
      ON eq.alias_track_id = t.id
),
tracks_canon AS (
    -- One row per canonical_track_id.
    -- Prefer the true canonical row (id == canonical_track_id), then most recent added_at.
    SELECT DISTINCT ON (canonical_track_id)
        *
    FROM tracks_canon_raw
    ORDER BY
        canonical_track_id,
        (id = canonical_track_id) DESC,
        added_at DESC NULLS LAST
),
liked_tracks_canon_raw AS (
    SELECT
        lt.*,
        COALESCE(eq.canonical_track_id, lt.track_id) AS canonical_track_id
    FROM liked_tracks lt
    LEFT JOIN track_id_equivalents eq
      ON eq.alias_track_id = lt.track_id
),
liked_tracks_canon AS (
    -- One row per canonical_track_id.
    -- Prefer a like that already matches the canonical id, then most recent liked_at.
    SELECT DISTINCT ON (canonical_track_id)
        *
    FROM liked_tracks_canon_raw
    ORDER BY
        canonical_track_id,
        (track_id = canonical_track_id) DESC,
        liked_at DESC NULLS LAST
),

-- Step 1b: Merge tracks and liked_tracks into a unified base (library source), keyed by canonical track_id
base_tracks AS (
    SELECT
        tc.canonical_track_id AS track_id,
        tc.name AS track_name,
        COALESCE(a.artist, ltc.track_artist) AS artist,
        COALESCE(a.artist_id, ltc.artist_id) AS artist_id,
        tc.album_id,
        a.name AS album_name,
        a.album_type,
        a.album_image_url,
        a.release_date::text AS release_date,         -- cast to text
        ar.genres,
        ar.image_url AS artist_image,
        tc.track_number,
        COALESCE(tc.disc_number, 1) AS disc_number,
        tc.added_at,                                    -- timestamptz
        tc.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
        tc.duration_ms,
        COALESCE(ltc.popularity, tc.popularity) AS popularity,
        ltc.liked_at,                                   -- timestamptz
        ltc.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS liked_at_est,
        ltc.last_checked_at,                            -- timestamptz
        ta.is_playable,
        CASE WHEN ltc.liked_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_liked,
        EXISTS (
            SELECT 1 FROM excluded_tracks et
            WHERE et.track_id = tc.canonical_track_id OR et.track_id = tc.id
        ) AS excluded,
        'library'::text AS track_source,
        'album'::text   AS library_origin
    FROM tracks_canon tc
    JOIN albums a ON tc.album_id = a.id
    LEFT JOIN liked_tracks_canon ltc ON ltc.canonical_track_id = tc.canonical_track_id
    LEFT JOIN track_availability ta ON ta.track_id = tc.canonical_track_id
    LEFT JOIN artists ar ON ar.id = COALESCE(a.artist_id, ltc.artist_id)
    WHERE a.is_saved = TRUE

    UNION ALL

    SELECT
        ltc.canonical_track_id AS track_id,
        ltc.track_name,
        ltc.track_artist,
        ltc.artist_id,
        ltc.album_id,
        NULL,
        NULL,
        NULL,
        NULL::text AS release_date,
        ar.genres,
        ar.image_url AS artist_image,
        NULL,
        1,
        ltc.added_at,
        ltc.added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        ltc.duration_ms,
        COALESCE(ltc.popularity, tc.popularity) AS popularity,
        ltc.liked_at,
        ltc.liked_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
        ltc.last_checked_at,
        ta.is_playable,
        TRUE,
        EXISTS (
            SELECT 1 FROM excluded_tracks et
            WHERE et.track_id = ltc.canonical_track_id OR et.track_id = ltc.track_id
        ),
        'library'::text    AS track_source,
        'liked_only'::text AS library_origin
    FROM liked_tracks_canon ltc
    LEFT JOIN tracks_canon tc ON tc.canonical_track_id = ltc.canonical_track_id
    LEFT JOIN track_availability ta ON ta.track_id = ltc.canonical_track_id
    LEFT JOIN artists ar ON ar.id = ltc.artist_id
    WHERE tc.id IS NULL
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
        COALESCE(tc.duration_ms, ltc.duration_ms, p.duration_ms) AS duration_ms,
        LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) AS prev_played,
        LEAD(p.track_id) OVER (ORDER BY p.played_at) AS next_track_id,
        LEAD(p.played_at) OVER (ORDER BY p.played_at) AS next_played,
        CASE
            WHEN LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at) IS NOT NULL
                 AND EXTRACT(EPOCH FROM (p.played_at - LAG(p.played_at) OVER (PARTITION BY p.track_id ORDER BY p.played_at))) * 1000
                     < COALESCE(tc.duration_ms, ltc.duration_ms, p.duration_ms)
            THEN TRUE ELSE FALSE
        END AS is_resume,
        CASE
            WHEN LEAD(p.track_id) OVER (ORDER BY p.played_at) IS NOT NULL
                 AND LEAD(p.track_id) OVER (ORDER BY p.played_at) != p.track_id
                 AND EXTRACT(EPOCH FROM (LEAD(p.played_at) OVER (ORDER BY p.played_at) - p.played_at)) * 1000
                     < (COALESCE(tc.duration_ms, ltc.duration_ms, p.duration_ms) * 0.3)
            THEN TRUE ELSE FALSE
        END AS is_skipped
    FROM all_plays p
    LEFT JOIN tracks_canon tc ON tc.canonical_track_id = p.track_id
    LEFT JOIN liked_tracks_canon ltc ON ltc.canonical_track_id = p.track_id
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
  LEFT JOIN tracks_canon tc  ON tc.canonical_track_id = p.track_id
  LEFT JOIN liked_tracks_canon ltc ON ltc.canonical_track_id = p.track_id
  WHERE p.track_id IS NOT NULL
    AND tc.id IS NULL
    AND ltc.track_id IS NULL
),

-- Step 6: Fuzzy match only those candidates
fuzzy_matched_tracks AS (
  SELECT 
    c.play_row_id AS play_id,
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
      AND NOT EXISTS (SELECT 1 FROM tracks_canon tc WHERE tc.canonical_track_id = p.track_id)
      AND NOT EXISTS (SELECT 1 FROM liked_tracks_canon ltc WHERE ltc.canonical_track_id = p.track_id)
      AND NOT EXISTS (SELECT 1 FROM fuzzy_matched_tracks fmt WHERE fmt.play_id = p.play_row_id)
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

SELECT
    ab.track_id,
    ab.track_name,
    ab.artist,
    ab.artist_id,
    ab.album_id,
    ab.album_name,
    ab.album_type,
    ab.album_image_url,
    ab.release_date,
    ab.genres,
    ab.artist_image,
    ab.track_number,
    ab.disc_number,
    combined_dates.earliest_added_at AS added_at,
    combined_dates.earliest_added_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS added_at_est,
    ab.duration_ms,
    ab.popularity,
    ab.liked_at,
    ab.liked_at_est,
    ab.last_checked_at,
    ab.is_playable,
    ab.is_liked,
    ab.excluded,
    ab.track_source,
    ab.library_origin,

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

    first_play.first_played_at,
    first_play.first_played_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS first_played_at_est,

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
CROSS JOIN LATERAL (
    SELECT
        NULLIF(
            LEAST(
                COALESCE(ps.library_play_count_first_played, 'infinity'::timestamptz),
                COALESCE(fp.fuzz_play_count_first_played, 'infinity'::timestamptz)
            ),
            'infinity'::timestamptz
        ) AS first_played_at
) AS first_play
CROSS JOIN LATERAL (
    SELECT
        NULLIF(
            LEAST(
                COALESCE(ab.added_at, 'infinity'::timestamptz),
                COALESCE(ab.liked_at, 'infinity'::timestamptz),
                COALESCE(first_play.first_played_at, 'infinity'::timestamptz)
            ),
            'infinity'::timestamptz
        ) AS earliest_added_at
) AS combined_dates
;
"""

if __name__ == "__main__":
    log_event("build_unified_tracks", "Starting unified_tracks materialized view build...")
    conn = get_db_connection()
    cur = conn.cursor()
    # Force drop and recreate the materialized view each run
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
        CREATE INDEX IF NOT EXISTS idx_unified_plays_track_time ON unified_plays_mv(track_id, played_at);
        CREATE INDEX IF NOT EXISTS idx_unified_plays_name_artist_lower ON unified_plays_mv(LOWER(track_name), LOWER(artist_name));
        CREATE INDEX IF NOT EXISTS idx_plays_track_time        ON plays(track_id, played_at);
        CREATE INDEX IF NOT EXISTS idx_hist_track_time         ON spotify_play_history(track_id, played_at);
        CREATE INDEX IF NOT EXISTS idx_amph_track_time         ON apple_music_play_history(track_id, played_at);

        -- Functional indexes for fuzzy matching
        CREATE INDEX IF NOT EXISTS idx_plays_name_artist_lower ON plays(LOWER(track_name), LOWER(artist_name));
        CREATE INDEX IF NOT EXISTS idx_hist_name_artist_lower  ON spotify_play_history(LOWER(track_name), LOWER(artist_name));
        CREATE INDEX IF NOT EXISTS idx_amph_name_artist_lower  ON apple_music_play_history(LOWER(track_name), LOWER(artist_name));
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
