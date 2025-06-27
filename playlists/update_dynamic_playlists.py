

import os
import psycopg2
from utils.db_auth import get_db_connection
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
            except Exception as e:
                log_event("update_dynamic_playlists", f"❌ Error syncing playlist '{slug}': {e}", level="error")

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