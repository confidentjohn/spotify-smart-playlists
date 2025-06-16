import os
import psycopg2
import requests
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

# ─────────────────────────────────────────────
# Get access token
# ─────────────────────────────────────────────
def get_access_token():
    auth_response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
            "client_id": os.environ["SPOTIFY_CLIENT_ID"],
            "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
        }
    )
    auth_response.raise_for_status()
    return auth_response.json()["access_token"]

access_token = get_access_token()
sp = Spotify(auth=access_token)
user = sp.current_user()
print(f"🔐 Authenticated as: {user['id']}")

# ─────────────────────────────────────────────
# Connect to DB
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
# Get playlist ID from mapping table
# ─────────────────────────────────────────────
cur.execute("SELECT playlist_id FROM playlist_mappings WHERE name = %s", ("Never Played",))
row = cur.fetchone()
if not row:
    print("❌ No playlist mapping found.")
    exit(1)

playlist_url = row[0]
playlist_id = playlist_url.split("/")[-1].split("?")[0]
print(f"🎯 Using playlist ID: {playlist_id}")

# ─────────────────────────────────────────────
# Fetch tracks (exclude unplayable ones)
# ─────────────────────────────────────────────
cur.execute("""
    SELECT 'spotify:track:' || ut.track_id
    FROM unified_tracks ut
    WHERE ut.play_count = 0
      AND ut.is_playable IS DISTINCT FROM FALSE
      AND added_at >= DATE '2025-06-16'
      AND ut.excluded IS DISTINCT FROM TRUE
    ORDER BY 
      ut.album_id,
      ut.disc_number NULLS LAST,
      ut.track_number NULLS LAST
    LIMIT 9000;
""")
rows = cur.fetchall()
track_uris = [row[0] for row in rows]

if not track_uris:
    print("⚠️ No tracks to update.")
    exit()

print(f"🎧 {len(track_uris)} tracks to push to Spotify playlist.")

# ─────────────────────────────────────────────
# Clear existing playlist and add new tracks
# ─────────────────────────────────────────────
try:
    print("🧹 Clearing playlist...")
    sp.user_playlist_replace_tracks(user["id"], playlist_id, [])

    print("➕ Adding tracks in batches of 100...")
    for i in range(0, len(track_uris), 100):
        sp.playlist_add_items(playlist_id, track_uris[i:i + 100])
except SpotifyException as e:
    print(f"❌ Spotify API error: {e.http_status} - {e.msg}")
    exit(1)

# ─────────────────────────────────────────────
# Update playlist_mappings with count and timestamp
# ─────────────────────────────────────────────
cur.execute("""
    UPDATE playlist_mappings
    SET track_count = %s,
        last_synced_at = CURRENT_TIMESTAMP
    WHERE name = %s
""", (len(track_uris), "Never Played"))
conn.commit()

cur.close()
conn.close()
print("✅ Playlist updated successfully.")
