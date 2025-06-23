from app.db.init_db import run_init_db
from utils.create_exclusions_playlist import ensure_exclusions_playlist

def run_startup_tasks():
    run_init_db()
    ensure_exclusions_playlist()

run_startup_tasks()