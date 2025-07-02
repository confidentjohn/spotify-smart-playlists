from flask import Blueprint, render_template, request, redirect, session, url_for
from app.users import create_user
from utils.logger import log_event

setup_bp = Blueprint("setup", __name__)
create_admin_bp = setup_bp

@setup_bp.route("/create-admin", methods=["GET", "POST"])
def create_admin():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        email = request.form.get("email")

        try:
            user_id = create_user(username, password, email)
            session["user_id"] = user_id
            session["username"] = username
            log_event("auth", "info", f"User '{username}' auto-logged in after setup.", {"user_id": user_id})
            return redirect(url_for("playlist_dashboard.dashboard_playlists"))
        except Exception as e:
            error = str(e)
            return render_template("create_admin.html", error=error)

    return render_template("create_admin.html")