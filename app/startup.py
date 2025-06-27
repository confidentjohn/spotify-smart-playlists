from app.db.init_db import run_init_db
from utils.create_exclusions_playlist import ensure_exclusions_playlist
from utils.spotify_auth import get_spotify_client

def run_startup_tasks():
    run_init_db()
    try:
        sp = get_spotify_client()
        ensure_exclusions_playlist(sp)
    except Exception as e:
        print(f"⚠️ Spotify client init failed (ignored): {e}")

run_startup_tasks()