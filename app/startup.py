from app.db.init_db import run_init_db
import os

def run_startup_tasks():
    run_init_db()

    from utils.db_utils import get_db_connection
    from utils.spotify_auth import get_spotify_client
    from utils.create_exclusions_playlist import ensure_exclusions_playlist
    from utils.logger import log_event

    try:
        refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")
        if refresh_token:
            log_event("init", "🔑 Refresh token found in environment. Checking exclusions playlist.")
            sp = get_spotify_client()
            ensure_exclusions_playlist(sp)
            log_event("init", "ℹ️ Exclusions playlist already exists. No action taken.")
            log_event("init", "✅ Exclusions playlist check complete. No action needed.")
        else:
            log_event("init", "⚠️ Skipping exclusions playlist check. No SPOTIFY_REFRESH_TOKEN found.")

    except Exception as e:
        import traceback
        print("⚠️ Error during exclusions playlist check:")
        traceback.print_exc()

run_startup_tasks()