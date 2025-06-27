import os
import psycopg2
from spotipy.exceptions import SpotifyException
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client

# ─────────────────────────────────────────────
# Get Spotify client
# ─────────────────────────────────────────────
sp = get_spotify_client()

# ─────────────────────────────────────────────
# Connect to DB
# ─────────────────────────────────────────────
from utils.db_utils import get_db_connection
conn = get_db_connection()
cur = conn.cursor()

# ─────────────────────────────────────────────
# Get exclusion playlist ID
# ─────────────────────────────────────────────
cur.execute("SELECT playlist_id FROM playlist_mappings WHERE name = %s", ("exclusions",))
row = cur.fetchone()
if not row:
    log_event("sync_exclusions", "❌ No 'exclusions' playlist found in playlist_mappings.")
    exit(1)

playlist_url = row[0]
playlist_id = playlist_url.split("/")[-1].split("?")[0]
log_event("sync_exclusions", f"🎯 Using playlist ID: {playlist_id}")

# ─────────────────────────────────────────────
# Fetch track IDs from exclusion playlist
# ─────────────────────────────────────────────
track_ids = []
offset = 0
while True:
    results = sp.playlist_items(playlist_id, offset=offset, fields="items.track.id,total,next", additional_types=["track"])
    items = results.get("items", [])
    if not items:
        break
    for item in items:
        track = item.get("track")
        if track and track.get("id"):
            track_ids.append(track["id"])
    offset += len(items)

log_event("sync_exclusions", f"📦 Retrieved {len(track_ids)} track(s) to exclude.")

# ─────────────────────────────────────────────
# Update excluded_tracks table
# ─────────────────────────────────────────────
cur.execute("CREATE TABLE IF NOT EXISTS excluded_tracks (track_id TEXT PRIMARY KEY)")
cur.execute("TRUNCATE excluded_tracks")
for track_id in track_ids:
    cur.execute("INSERT INTO excluded_tracks (track_id) VALUES (%s)", (track_id,))
conn.commit()

log_event("sync_exclusions", "✅ excluded_tracks table updated.")
cur.close()
conn.close()