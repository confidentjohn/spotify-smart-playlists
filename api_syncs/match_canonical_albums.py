import os
import psycopg2
import requests
import time
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
from utils.logger import log_event


def get_access_token():
    response = requests.post(
        'https://accounts.spotify.com/api/token',
        data={
            'grant_type': 'refresh_token',
            'refresh_token': os.environ['SPOTIFY_REFRESH_TOKEN'],
            'client_id': os.environ['SPOTIFY_CLIENT_ID'],
            'client_secret': os.environ['SPOTIFY_CLIENT_SECRET']
        }
    )
    return response.json()['access_token']



def normalize_name(name):
    # Replace various apostrophe types with standard apostrophe
    return name.strip().lower().replace("’", "'").replace("‘", "'").replace("`", "'").replace("´", "'").replace("ʼ", "'")

def safe_artist_albums(sp: Spotify, artist_id, limit=50, offset=0):
    while True:
        try:
            return sp.artist_albums(artist_id, album_type='album', limit=limit, offset=offset)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', 5))
                log_event("match_canonical_albums", f"Rate limited for {retry_after}s on artist {artist_id}")
                time.sleep(retry_after)
            else:
                log_event("match_canonical_albums", f"Spotify error for {artist_id}: {e}", level="error")
                raise

def fetch_artist_albums(sp: Spotify, artist_id):
    albums = []
    seen_ids = set()
    offset = 0

    while True:
        results = safe_artist_albums(sp, artist_id, offset=offset)
        items = results.get('items', [])

        for album in items:
            if album['id'] not in seen_ids:
                seen_ids.add(album['id'])
                albums.append({
                    'id': album['id'],
                    'name': normalize_name(album['name'])
                })

        if len(items) < 50:
            break
        offset += 50

    return albums


def main():
    access_token = get_access_token()
    sp = Spotify(auth=access_token)

    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ.get('DB_PORT', 5432),
        sslmode='require'
    )
    cur = conn.cursor()

    # Fetch saved albums with artist_id
    cur.execute("SELECT id, name, artist_id FROM albums WHERE artist_id IS NOT NULL")
    albums = cur.fetchall()

    matched_count = 0
    for album_id, album_name, artist_id in albums:
        our_album_name = normalize_name(album_name)
        try:
            artist_albums = fetch_artist_albums(sp, artist_id)
            log_event("match_canonical_albums", f"Fetched {len(artist_albums)} albums for artist {artist_id}")
        except Exception as e:
            log_event("match_canonical_albums", f"Error fetching albums for {artist_id}: {e}", level="error")
            continue

        match = next((a for a in artist_albums if a['name'] == our_album_name), None)
        if match and match['id'] != album_id:
            # Insert into canonical_album_matches
            cur.execute("""
                INSERT INTO canonical_album_matches (album_id, artist_id, album_name, matched_canonical_id, matched_album_name, match_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (album_id) DO UPDATE
                SET matched_canonical_id = EXCLUDED.matched_canonical_id,
                    matched_album_name = EXCLUDED.matched_album_name,
                    match_status = EXCLUDED.match_status
            """, (album_id, artist_id, album_name, match['id'], match['name'], 'matched'))

            matched_count += 1
            log_event("match_canonical_albums", f"Matched: {album_name} → {match['id']}")

    conn.commit()
    cur.close()
    conn.close()

    log_event("match_canonical_albums", f"✅ Completed with {matched_count} matches")


if __name__ == "__main__":
    main()