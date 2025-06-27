# get_token.py
import os
from spotipy.oauth2 import SpotifyOAuth

sp_oauth = SpotifyOAuth(
    client_id=os.environ["SPOTIFY_CLIENT_ID"],
    client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    redirect_uri="http://localhost:8888/callback",
    scope="user-read-recently-played user-library-read playlist-modify-private playlist-modify-public"
)

auth_url = sp_oauth.get_authorize_url()
print(f"🔗 Go to this URL:\n{auth_url}")

response = input("\nPaste the full redirect URL after approval:\n")

code = sp_oauth.parse_response_code(response)
token_info = sp_oauth.get_access_token(code)

print("\n✅ Access token:", token_info["access_token"])
print("🔁 Refresh token:", token_info["refresh_token"])