import os
import psycopg2
from werkzeug.security import check_password_hash
from utils.logger import log_event
from utils.db_auth import get_db_connection

def get_user(username):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, email, password_hash FROM users WHERE username = %s", (username,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        log_event("auth", "debug", f"User '{username}' found in database.")
        return {
            "id": result[0],
            "username": result[1],
            "email": result[2],
            "password_hash": result[3],
        }
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