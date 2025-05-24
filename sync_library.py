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

limit = 50
offset = 0
while True:
    results = sp.current_user_saved_tracks(limit=limit, offset=offset)
    tracks = results['items']

    if not tracks:
        break

    for item in tracks:
        track = item['track']
        track_id = track['id']
        name = track['name']
        artist = track['artists'][0]['name']
        album = track['album']['name']

        cur.execute("""
            INSERT INTO tracks (id, name, artist, album, is_liked)
            VALUES (%s, %s, %s, %s, TRUE)
            ON CONFLICT (id) DO UPDATE SET is_liked = TRUE;
        """, (track_id, name, artist, album))

    offset += len(tracks)
    if len(tracks) < limit:
        break

conn.commit()
cur.close()
conn.close()
print("✅ Synced saved tracks from library.")
