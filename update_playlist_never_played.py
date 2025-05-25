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
    auth_response.raise_for_status()
    return auth_response.json()['access_token']

access_token = get_access_token()
sp = Spotify(auth=access_token)
user_id = sp.current_user()["id"]
print(f"🔐 Authenticated as: {user_id}")

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
# Fetch track IDs (not URIs!)
# ─────────────────────────────────────────────
cur.execute("""
    SELECT t.id
    FROM tracks t
    LEFT JOIN plays p ON t.id = p.track_id
    LEFT JOIN albums a ON t.album_id = a.id
    WHERE p.track_id IS NULL AND (a.is_saved IS NULL OR a.is_saved = TRUE)
    ORDER BY t.album_id, t.track_number NULLS LAST
    LIMIT 9000
""")
rows = cur.fetchall()
track_ids = [row[0] for row in rows]

if not track_ids:
    print("⚠️ No tracks found to add.")
    exit()

print(f"🎯 Preparing to upload {len(track_ids)} tracks")

# ─────────────────────────────────────────────
# Lookup playlist from local mappings
# ─────────────────────────────────────────────
cur.execute("""
    SELECT playlist_id FROM playlist_mappings WHERE name = %s
""", ("Never Played",))
result = cur.fetchone()
if not result:
    print("❌ Playlist mapping for 'Never Played' not found.")
    exit()

playlist_url_or_id = result[0]
print(f"📎 Playlist mapping found → {playlist_url_or_id}")

# Extract actual playlist ID from URL if needed
playlist_id = playlist_url_or_id.split("/")[-1].split("?")[0]
print(f"🎯 Extracted playlist ID: {playlist_id}")

# ─────────────────────────────────────────────
# Clear playlist before refill
# ─────────────────────────────────────────────
print("🧹 Clearing existing playlist contents...")
sp.playlist_replace_items(playlist_id, [])

# ─────────────────────────────────────────────
# Add tracks in batches
# ─────────────────────────────────────────────
print(f"📦 Uploading in batches of 100...")
for i in range(0, len(track_ids), 100):
    batch = track_ids[i:i+100]
    print(f"🔁 Batch {i//100 + 1}: {batch[:3]}... ({len(batch)} tracks)")
    try:
        response = sp.playlist_add_items(playlist_id, batch)
        print(f"✅ Batch added → Snapshot: {response['snapshot_id']}")
    except Exception as e:
        print(f"❌ Failed to upload batch {i//100 + 1}: {e}")
        exit(1)

# ─────────────────────────────────────────────
cur.close()
conn.close()
print("✅ Playlist sync complete.")
