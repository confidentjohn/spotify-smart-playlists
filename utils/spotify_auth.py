import os
import requests
from spotipy import Spotify
import psycopg2
from utils.db_utils import get_conn

def get_spotify_client():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, spotify_refresh_token FROM users")
    users = cur.fetchall()
    cur.close()
    conn.close()

    if len(users) == 0:
        raise Exception("❌ No users found in the database.")
    if len(users) > 1:
        raise Exception("❌ More than one user found in the database. This app only supports a single user.")

    user_id, refresh_token = users[0]
    if not refresh_token:
        raise Exception(f"❌ User {user_id} does not have a Spotify refresh token.")

    token_response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": os.environ["SPOTIFY_CLIENT_ID"],
            "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
        }
    )
    token_response.raise_for_status()
    return Spotify(auth=token_response.json()["access_token"])