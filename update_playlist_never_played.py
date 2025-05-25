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
# Fetch playlist ID from playlist_mappings table
# ─────────────────────────────────────────────
playlist_name = "Never Played"
cur.execute("SELECT playlist_id FROM playlist_mappings WHERE name = %s", (playlist_name,))
row = cur.fetchone()

if not row:
    print(f"❌ Playlist name '{playlist_name}' not found in playlist_mappings table.")
    cur.close()
    conn.close()
    exit()

playlist_id = row[0]
print(f"📎 Found playlist mapping → ID: {playlist_id}")

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
print(f"🎯 Found {len(track_uris)} tracks to add to playlist")

if not track_uris:
    print("⚠️ No tracks to add. Aborting playlist update.")
    cur.close()
    conn.close()
    exit()

# ─────────────────────────────────────────────
# Clear and update the playlist
# ─────────────────────────────────────────────
print(f"📝 Replacing contents of playlist ID {playlist_id}")
sp.playlist_replace_items(playlist_id, [])  # Clear

print(f"🎶 Adding {len(track_uris)} tracks to playlist...")
for i in range(0, len(track_uris), 100):
    sp.playlist_add_items(playlist_id, track_uris[i:i + 100])

cur.close()
conn.close()
print("✅ Playlist sync complete.")
