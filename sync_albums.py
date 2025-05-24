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

        cur.execute("""
            INSERT INTO albums (id, name, artist, release_date, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (album_id, name, artist, release_date, total_tracks))

    offset += len(items)
    if len(items) < limit:
        break

conn.commit()
cur.close()
conn.close()
print("✅ Synced saved albums from library.")
