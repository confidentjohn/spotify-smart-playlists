"""materialized_plays.py

Creates/refreshes a unified plays materialized view that:
- Unions play rows from multiple sources (plays, spotify_play_history, apple_music_play_history)
- Applies manual Track ID equivalence overrides via track_id_equivalents

This keeps raw play tables immutable while giving the app one fast, canonical place to query.

Usage:
  python api_syncs/materialized_plays.py

Notes:
- The MV name is `unified_plays_mv`.
- Refresh strategy:
  - Tries CONCURRENTLY first (non-blocking reads) if possible.
  - Falls back to a standard refresh if concurrently fails.
"""

import time


def _get_db_connection():
    """Import the project's DB connection helper with a couple of common fallbacks."""
    # Your project memory indicates you renamed the reusable helper to `db_utils.py`.
    try:
        from db_utils import get_db_connection  # type: ignore
        return get_db_connection
    except Exception:
        pass

    # Common alternatives in this repo pattern
    try:
        from utils.db_utils import get_db_connection  # type: ignore
        return get_db_connection
    except Exception:
        pass

    try:
        from utils.db import get_db_connection  # type: ignore
        return get_db_connection
    except Exception:
        pass

    raise ImportError(
        "Could not import get_db_connection. Tried db_utils.get_db_connection, "
        "utils.db_utils.get_db_connection, utils.db.get_db_connection."
    )


def _log(job: str, level: str, message: str):
    """Use centralized logger if available; otherwise print."""
    try:
        from utils.logger import log_event  # type: ignore

        # Many of your scripts use: log_event(job, level, message)
        log_event(job, level, message)
    except Exception:
        print(f"[{level.upper()}] {job}: {message}")


MV_NAME = "unified_plays_mv"


MV_SQL = f"""
CREATE MATERIALIZED VIEW {MV_NAME} AS
WITH all_plays AS (
    -- Primary plays table
    SELECT
        'plays'::text AS source,
        p.id::text AS source_row_id,
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

    -- Spotify play history table
    SELECT
        'spotify_play_history'::text AS source,
        sph.id::text AS source_row_id,
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

    UNION ALL

    -- Apple Music play history table
    SELECT
        'apple_music_play_history'::text AS source,
        amph.id::text AS source_row_id,
        amph.track_id,
        amph.track_name,
        amph.artist_id,
        amph.artist_name,
        amph.album_id,
        amph.album_name,
        amph.album_type,
        amph.duration_ms,
        amph.played_at
    FROM apple_music_play_history amph
    WHERE amph.played_at IS NOT NULL
),
resolved AS (
    SELECT
        ap.*,
        COALESCE(eq.canonical_track_id, ap.track_id) AS resolved_track_id
    FROM all_plays ap
    LEFT JOIN track_id_equivalents eq
      ON eq.alias_track_id = ap.track_id
)
SELECT *
FROM resolved;
"""


def ensure_materialized_view(cur):
    """Create the MV if it doesn't exist; otherwise leave it in place."""
    # CREATE MATERIALIZED VIEW IF NOT EXISTS isn't available in all PG versions,
    # so we do an existence check via pg_matviews.
    cur.execute(
        """
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = current_schema()
          AND matviewname = %s
        """,
        (MV_NAME,),
    )
    exists = cur.fetchone() is not None

    if exists:
        return False

    cur.execute(MV_SQL)
    return True


def ensure_indexes(cur):
    """Create supporting indexes (safe to re-run)."""
    # Unique index allows REFRESH CONCURRENTLY.
    cur.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{MV_NAME}_source_row
          ON {MV_NAME}(source, source_row_id);
        """
    )

    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS ix_{MV_NAME}_played_at
          ON {MV_NAME}(played_at);
        """
    )

    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS ix_{MV_NAME}_resolved_track_id
          ON {MV_NAME}(resolved_track_id);
        """
    )


def refresh_materialized_view(cur):
    """Refresh MV; try concurrently first, fall back to standard refresh."""
    try:
        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {MV_NAME};")
        return "concurrently"
    except Exception:
        # Fallback: standard refresh (blocking)
        cur.execute(f"REFRESH MATERIALIZED VIEW {MV_NAME};")
        return "standard"


def build_unified_plays_mv():
    job = "materialized_plays"
    start = time.time()

    get_db_connection = _get_db_connection()
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            created = ensure_materialized_view(cur)
            ensure_indexes(cur)
            strategy = refresh_materialized_view(cur)

        conn.commit()

        elapsed = round(time.time() - start, 2)
        if created:
            _log(job, "info", f"✅ Created and refreshed {MV_NAME} ({strategy}) in {elapsed}s")
        else:
            _log(job, "info", f"✅ Refreshed {MV_NAME} ({strategy}) in {elapsed}s")

    except Exception as e:
        conn.rollback()
        _log(job, "error", f"❌ Failed to build/refresh {MV_NAME}: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    build_unified_plays_mv()
