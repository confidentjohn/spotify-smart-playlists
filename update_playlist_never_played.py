import os
import psycopg2
import requests
from spotipy import Spotify

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
    auth_response.raise_for_status()
    return auth_response.json()['access_token']

access_token = get_access_token()
sp = Spotify(auth=access_token)
user_id = sp.current_user()["id"]
print("🔐 Authenticated")

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
# Fetch unplayed track URIs
# ─────────────────────────────────────────────
cur.execute("""
    SELECT 'spotify:track:' || t.id
    FROM tracks t
    LEFT JOIN plays p ON t.id = p.track_id
    LEFT JOIN albums a ON t.album_id = a.id
    WHERE p.track_id IS NULL AND (a.is_saved IS NULL OR a.is_saved = TRUE)
    ORDER BY t.album_id, t.track_number NULLS LAST
    LIMIT 9000
""")
rows = cur.fetchall()
track_uris = [row[0] for row in rows]

if not track_uris:
    print("⚠️ No tracks found.")
    exit()

print(f"🎯 Preparing to upload {len(track_uris)} tracks")

# ─────────────────────────────────────────────
# Lookup playlist mapping
# ─────────────────────────────────────────────
cur.execute("""
    SELECT playlist_id FROM playlist_mappings WHERE name = %s
""", ("Never Played",))
result = cur.fetchone()
if not result:
    print("❌ Playlist mapping for 'Never Played' not found.")
    exit()

playlist_url_or_id = result[0]
playlist_id = playlist_url_or_id.split("/")[-1].split("?")[0]

# ─────────────────────────────────────────────
# Clear and populate playlist
# ─────────────────────────────────────────────
sp.playlist_replace_items(playlist_id, [])
print("🧹 Playlist cleared")

for i in range(0, len(track_uris), 100):
    sp.playlist_add_items(playlist_id, track_uris[i:i + 100])

cur.close()
conn.close()
print("✅ Playlist updated")
