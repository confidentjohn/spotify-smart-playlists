from werkzeug.security import check_password_hash
from utils.logger import log_event

# Hardcoded user
HARDCODED_USER = {
    "id": 1,
    "username": "admin",
    "email": "admin@example.com",
    # Provided password hash
    "password_hash": "scrypt:32768:8:1$dxRjboeqkJf2XdGP$fab4ec1b741def46189ad1d02ae1c02a70acc523719d761457270fdf1569876a8c50b8107d02774e41c57b24f0f1c16ddcaeac0da89bd12105fc38f53b711912"
}

def get_user(username):
    if username == HARDCODED_USER["username"]:
        log_event("auth", "debug", f"User '{username}' found in hardcoded config.")
        return HARDCODED_USER
    else:
        log_event("auth", "debug", f"User '{username}' not found in hardcoded config.")
    return None

def validate_user(username, password):
    user = get_user(username)
    if user and check_password_hash(user["password_hash"], password):
        log_event("auth", "info", f"User '{username}' logged in successfully.", {"user_id": user["id"]})
        return True
    else:
        log_event("auth", "warning", f"Failed login attempt for user '{username}'.")
        return False