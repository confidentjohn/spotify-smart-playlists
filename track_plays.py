import os
import psycopg2
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

# Spotify authentication
sp = Spotify(auth_manager=SpotifyOAuth(
    client_id=os.environ['SPOTIFY_CLIENT_ID'],
    client_secret=os.environ['SPOTIFY_CLIENT_SECRET'],
    redirect_uri=os.environ['SPOTIFY_REDIRECT_URI'],
    scope='user-read-recently-played user-library-read',
    cache_path=None
))

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432)
)
cur = conn.cursor()

# Fetch recent plays (max 50)
results = sp.current_user_recently_played(limit=50)

for item in results['items']:
    track = item['track']
    track_id = track['id']
    name = track['name']
    artist = track['artists'][0]['name']
    album = track['album']['name']
    played_at = item['played_at']
    played_at = datetime.fromisoformat(played_at.replace('Z', '+00:00')).astimezone(ZoneInfo('UTC'))

    # Check if track is in user's saved library
    is_liked = sp.current_user_saved_tracks_contains([track_id])[0]

    # Insert or update track
    cur.execute("""
        INSERT INTO tracks (id, name, artist, album, is_liked)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET is_liked = EXCLUDED.is_liked;
    """, (track_id, name, artist, album, is_liked))

    # Insert play if not already logged
    cur.execute("""
        INSERT INTO plays (track_id, played_at)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """, (track_id, played_at))

conn.commit()
cur.close()
conn.close()

print("✅ Recent plays tracked and stored.")
