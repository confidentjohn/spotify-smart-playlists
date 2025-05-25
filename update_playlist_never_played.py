import os
import psycopg2
import requests
import re
from spotipy import Spotify

# ─────────────────────────────────────────────
# Get Access Token
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
user_id = sp.current_user()["id"]
print(f"🔐 Authenticated as: {user_id}", flush=True)

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
# Get playlist mapping
# ─────────────────────────────────────────────
cur.execute("""
    SELECT playlist_id
    FROM playlist_mappings
    WHERE name = 'Never Played'
    LIMIT 1;
""")
result = cur.fetchone()
if not result:
    print("❌ No playlist mapping found for 'Never Played'", flush=True)
    exit(1)

playlist_url = result[0]
print(f"📎 Playlist mapping found → {playlist_url}", flush=True)

match = re.search(r"playlist/([a-zA-Z0-9]+)", playlist_url)
if not match:
    print("❌ Invalid playlist URL format.", flush=True)
    exit(1)

playlist_id = match.group(1)
print(f"🎯 Extracted playlist ID: {playlist_id}", flush=True)

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

print(f"🎯 Preparing to upload {len(track_uris)} tracks", flush=True)
print(f"🧪 Sample track URIs: {track_uris[:5]}", flush=True)

# ─────────────────────────────────────────────
# Replace contents and add tracks
# ─────────────────────────────────────────────
print("🧹 Clearing existing playlist contents...", flush=True)
sp.playlist_replace_items(playlist_id, [])

if track_uris:
    print("➕ Adding tracks in batches of 100...", flush=True)
    for i in range(0, len(track_uris), 100):
        batch = track_uris[i:i+100]
        sp.playlist_add_items(playlist_id, batch)

print("✅ Playlist sync complete.", flush=True)
cur.close()
conn.close()
