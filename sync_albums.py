import os
import psycopg2
import requests
from spotipy import Spotify

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

conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ.get('DB_PORT', 5432),
)
cur = conn.cursor()

limit = 20
offset = 0
current_album_ids = set()

while True:
    results = sp.current_user_saved_albums(limit=limit, offset=offset)
    items = results['items']

    if not items:
        break

    for item in items:
        album = item['album']
        album_id = album['id']
        name = album['name']
        artist = album['artists'][0]['name']
        release_date = album.get('release_date')
        total_tracks = album.get('total_tracks')
        added_at = item.get('added_at')
        current_album_ids.add(album_id)

        # Insert or update album
        cur.execute("""
            INSERT INTO albums (id, name, artist, release_date, total_tracks, is_saved, added_at)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (id) DO UPDATE SET is_saved = TRUE, added_at = EXCLUDED.added_at;
        """, (album_id, name, artist, release_date, total_tracks, added_at))

        # Insert tracks for this album
        album_tracks = sp.album(album_id)['tracks']['items']
        for track in album_tracks:
            track_id = track['id']
            track_name = track['name']
            track_artist = track['artists'][0]['name']
            track_album = name
            track_number = track.get("track_number") or 1

            cur.execute("""
                INSERT INTO tracks (id, name, artist, album, album_id, is_liked, from_album, track_number, added_at)
                VALUES (%s, %s, %s, %s, %s, FALSE, TRUE, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    from_album = TRUE,
                    track_number = EXCLUDED.track_number,
                    album_id = EXCLUDED.album_id;
                    added_at = EXCLUDED.added_at;
            """, (track_id, track_name, track_artist, track_album, album_id, track_number, added_at))

    offset += len(items)
    if len(items) < limit:
        break

# Mark removed albums
cur.execute("""
    UPDATE albums
    SET is_saved = FALSE
    WHERE id NOT IN %s
""", (tuple(current_album_ids),))

# Mark tracks from removed albums
cur.execute("""
    UPDATE tracks
    SET from_album = FALSE
    WHERE album_id IN (
        SELECT id FROM albums WHERE is_saved = FALSE
    )
""")

conn.commit()
cur.close()
conn.close()

print("✅ Synced saved albums and their tracks.")
