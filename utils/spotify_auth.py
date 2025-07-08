from utils.logger import log_event
from spotipy.oauth2 import SpotifyOAuth
import os
import requests
from spotipy import Spotify

def get_spotify_client():
    refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")
    if not refresh_token:
        log_event("auth", "error", "❌ SPOTIFY_REFRESH_TOKEN not set in environment.")
        raise Exception("❌ SPOTIFY_REFRESH_TOKEN not set in environment.")

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
    access_token = token_response.json().get("access_token")
    if not access_token:
        raise Exception("❌ Failed to get access token from Spotify.")
    return Spotify(auth=access_token)


# Returns a SpotifyOAuth instance using environment variables (used during login flow)
def get_spotify_oauth():
    return SpotifyOAuth(
        client_id=os.environ['SPOTIFY_CLIENT_ID'],
        client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
        redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
        scope="user-read-recently-played user-library-read playlist-modify-private playlist-modify-public"
    )