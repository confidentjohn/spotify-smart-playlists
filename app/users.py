from werkzeug.security import check_password_hash
from utils.logger import log_event
import psycopg2
import os

def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
    )

def get_user(username):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, email, password_hash FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        user = {
            "id": row[0],
            "username": row[1],
            "email": row[2],
            "password_hash": row[3]
        }
        log_event("auth", "debug", f"User '{username}' found in database.")
        return user
    else:
        log_event("auth", "debug", f"User '{username}' not found in database.")
        return None

def validate_user(username, password):
    user = get_user(username)
    if user and check_password_hash(user["password_hash"], password):
        log_event("auth", "info", f"User '{username}' logged in successfully.", {"user_id": user["id"]})
        return True
    else:
        log_event("auth", "warning", f"Failed login attempt for user '{username}'.")
        return False