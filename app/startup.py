from app.db.init_db import run_init_db

def run_startup_tasks():
    run_init_db()

run_startup_tasks()