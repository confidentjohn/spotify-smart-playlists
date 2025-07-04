from werkzeug.security import check_password_hash, generate_password_hash
from utils.logger import log_event
import psycopg2
import os
from utils.db_utils import get_db_connection

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


# Function to create a new user
def create_user(username, password, email=None):
    conn = get_db_connection()
    cur = conn.cursor()
    # Enforce max of 1 user
    cur.execute("SELECT COUNT(*) FROM users")
    user_count = cur.fetchone()[0]

    if user_count >= 1:
        cur.close()
        conn.close()
        log_event("auth", "warning", f"Attempt to create user '{username}' blocked: user limit reached.")
        raise Exception("User limit reached. Only one user allowed.")

    password_hash = generate_password_hash(password)
    cur.execute(
        "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
        (username, password_hash, email)
    )
    user_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    log_event("auth", "info", f"New user '{username}' created.", {"user_id": user_id})
    return user_id


# Check if user has a Spotify refresh token
def has_refresh_token(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT spotify_refresh_token FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row and row[0] is not None