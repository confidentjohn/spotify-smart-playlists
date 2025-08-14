

import os
import psycopg2
from utils.db_utils import get_db_connection
from playlists.generate_playlist import sync_playlist
from utils.logger import log_event

def main():
    log_event("update_dynamic_playlists", "🚀 Starting dynamic playlist updater")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT slug FROM playlist_mappings WHERE is_dynamic = TRUE")
        slugs = [row[0] for row in cur.fetchall()]
        log_event("update_dynamic_playlists", f"🧾 Found {len(slugs)} dynamic playlists to update: {slugs}")

        for slug in slugs:
            try:
                log_event("update_dynamic_playlists", f"🔁 Updating playlist: {slug}")
                sync_playlist(slug)
                # Mark playlist as seen in Spotify this run
                cur.execute(
                    """
                    UPDATE playlist_mappings
                    SET last_seen_spotify_at = NOW(),
                        pending_delete = FALSE,
                        missing_count = 0,
                        last_missing_reason = NULL
                    WHERE slug = %s
                    """,
                    (slug,)
                )
                conn.commit()
            except Exception as e:
                reason = str(e)
                cur.execute(
                    """
                    UPDATE playlist_mappings
                    SET pending_delete = TRUE,
                        missing_count = COALESCE(missing_count, 0) + 1,
                        last_missing_at = NOW(),
                        last_missing_reason = %s
                    WHERE slug = %s
                    """,
                    (reason[:500], slug)
                )
                conn.commit()
                log_event("update_dynamic_playlists", f"❌ Error syncing playlist '{slug}': {e}", level="error")

        # Diagnostics summary of delete-candidates
        cur.execute(
            """
            SELECT slug, missing_count, last_missing_at
            FROM playlist_mappings
            WHERE pending_delete = TRUE
            ORDER BY last_missing_at DESC NULLS LAST
            """
        )
        candidates = cur.fetchall()
        if candidates:
            summary = ", ".join([f"{row[0]}(misses={row[1]})" for row in candidates])
            log_event("update_dynamic_playlists", f"⚠️ {len(candidates)} playlists flagged as delete-candidates (pending confirmation): {summary}")
        else:
            log_event("update_dynamic_playlists", "✅ No delete-candidates this run")

    except Exception as db_error:
        log_event("update_dynamic_playlists", f"❌ Failed to connect to DB or fetch playlists: {db_error}", level="error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        log_event("update_dynamic_playlists", "✅ Finished updating dynamic playlists")

if __name__ == "__main__":
    main()