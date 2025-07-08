from app.db.init_db import run_init_db

def run_startup_tasks():
    run_init_db()

    from utils.db_utils import get_db_connection
    from utils.spotify_auth import get_spotify_client
    from utils.create_exclusions_playlist import ensure_exclusions_playlist
    from utils.logger import log_event

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, spotify_refresh_token FROM users")
        users = cur.fetchall()
        cur.close()
        conn.close()

        if len(users) == 1 and users[0][1]:
            log_event("init", "👤 Valid user with refresh token found. Checking exclusions playlist.")
            sp = get_spotify_client()
            ensure_exclusions_playlist(sp)
            log_event("init", "ℹ️ Exclusions playlist already exists. No action taken.")
            log_event("init", "✅ Exclusions playlist check complete. No action needed.")
        else:
            log_event("init", "⚠️ Skipping exclusions playlist check. Need exactly one user with a refresh token.")

    except Exception as e:
        import traceback
        print("⚠️ Error during exclusions playlist check:")
        traceback.print_exc()

run_startup_tasks()