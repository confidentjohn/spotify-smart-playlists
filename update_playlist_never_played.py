import os
import psycopg2
import requests
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

# ─────────────────────────────────────────────
# Auth with Spotipy
# ─────────────────────────────────────────────
def get_access_token():
    auth_response = requests.post(
        'https://accounts.spotify.com/api/token',
        data={
            'grant_type': 'refresh_token',
            'refresh_token': os.environ['SPOTIFY_REFRESH_TOKEN'],
            'client_id': os.environ['SPOTIFY_CLIENT_ID'],
            'client_secret': os.environ['SPOTIFY_CLIENT_SECRET']
        }
    )
    return auth_response.json()['access_token']

access_token = get_access_token()
sp = Spotify(auth=access_token)

# ─────────────────────────────────────────────
# Connect to PostgreSQL
# ─────────────────────────────────────────────
conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
)
cur = conn.cursor()

# ─────────────────────────────────────────────
# Fetch unplayed tracks (limit to 9000)
# ─────────────────────────────────────────────
cur.execute('''
    SELECT 'spotify:track:' || t.id
    FROM tracks t
    LEFT JOIN plays p ON t.id = p.track_id
    LEFT JOIN albums a ON t.album_id = a.id
    WHERE p.track_id IS NULL AND (a.is_saved IS NULL OR a.is_saved = TRUE)
    ORDER BY t.album_id, t.track_number NULLS LAST
    LIMIT 9000
''')
rows = cur.fetchall()
track_uris = [row[0] for row in rows]

# ─────────────────────────────────────────────
# Create or update playlist
# ─────────────────────────────────────────────
playlist_name = "🎧 Never Played Tracks"
user_id = sp.current_user()["id"]

# Find playlist if it exists
existing = sp.current_user_playlists()
playlist_id = None
for pl in existing["items"]:
    if pl["name"] == playlist_name:
        playlist_id = pl["id"]
        break

if playlist_id:
    print(f"📝 Updating existing playlist: {playlist_name}")
    sp.playlist_replace_items(playlist_id, [])  # Clear first
else:
    print(f"➕ Creating new playlist: {playlist_name}")
    playlist = sp.user_playlist_create(user=user_id, name=playlist_name, public=False)
    playlist_id = playlist["id"]

# Add tracks in batches of 100
print(f"🎶 Adding {len(track_uris)} tracks to playlist")
for i in range(0, len(track_uris), 100):
    sp.playlist_add_items(playlist_id, track_uris[i:i + 100])

cur.close()
conn.close()
print("✅ Playlist sync complete.")
