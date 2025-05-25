import os
import psycopg2
import requests
from spotipy import Spotify

# ─────────────── Spotify Auth ────────────────
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

# ───────────── PostgreSQL Connection ─────────────
conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
)
cur = conn.cursor()

# ───────────── Find Playlist ID ─────────────
playlist_name = "Never Played"
cur.execute("SELECT playlist_id FROM playlist_mappings WHERE name = %s", (playlist_name,))
row = cur.fetchone()

if not row:
    print(f"❌ Playlist name '{playlist_name}' not found in playlist_mappings.")
    exit(1)

playlist_id = row[0]
print(f"📎 Playlist mapping found → {playlist_id}", flush=True)

# ───────────── Build Track List ─────────────
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
track_uris = [row[0] for row in rows if row[0]]

if not track_uris:
    print("⚠️ No unplayed tracks found.")
    cur.close()
    conn.close()
    exit(0)

print(f"🎯 Preparing to upload {len(track_uris)} tracks", flush=True)

# ───────────── Verify Playlist Ownership ─────────────
playlist_info = sp.playlist(playlist_id)
playlist_owner = playlist_info['owner']['id']
if playlist_owner != user_id:
    print(f"⚠️ Playlist owner is {playlist_owner}, but logged in as {user_id}. Cannot modify.")
    exit(1)

# ───────────── Clear Playlist ─────────────
print("🧹 Clearing existing playlist contents...", flush=True)
sp.playlist_replace_items(playlist_id, [])

# ───────────── Add New Tracks ─────────────
print("🎶 Uploading tracks in batches...", flush=True)
for i in range(0, len(track_uris), 100):
    batch = track_uris[i:i+100]
    print(f"➕ Adding batch {i//100 + 1}: {len(batch)} tracks", flush=True)
    sp.playlist_add_items(playlist_id, batch)

cur.close()
conn.close()
print("✅ Playlist successfully updated.")
