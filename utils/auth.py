from flask import request, session
import os

def check_auth(request):
    expected = os.environ.get("ADMIN_KEY")
    if not expected:
        return False

    if session.get("is_admin"):
        return True

    secret = request.args.get("key")
    if secret == expected:
        session["is_admin"] = True
        return True

    return False