from werkzeug.security import check_password_hash

# Hardcoded users (replace with DB later if needed)
USERS = {
    "admin": {
        "username": "admin",
        "password_hash": "scrypt:32768:8:1$dxRjboeqkJf2XdGP$fab4ec1b741def46189ad1d02ae1c02a70acc523719d761457270fdf1569876a8c50b8107d02774e41c57b24f0f1c16ddcaeac0da89bd12105fc38f53b711912"  # Replace with real hash
    }
}

def get_user(username):
    return USERS.get(username)

def validate_user(username, password):
    user = get_user(username)
    if user and check_password_hash(user["password_hash"], password):
        return True
    return False