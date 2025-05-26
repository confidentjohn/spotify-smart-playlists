import os
import psycopg2

conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
)

cur = conn.cursor()

# Create or ensure albums table
cur.execute("""
CREATE TABLE IF NOT EXISTS albums (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    release_date TEXT,
    total_tracks INTEGER,
    is_saved BOOLEAN DEFAULT TRUE
);
""")

# Create or ensure tracks table
cur.execute("""
CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    album TEXT,
    album_id TEXT,
    is_liked BOOLEAN DEFAULT FALSE,
    from_album BOOLEAN DEFAULT FALSE
);
""")

# Add the columns in case the table already existed before
cur.execute("""ALTER TABLE tracks ADD COLUMN IF NOT EXISTS album_id TEXT;""")
cur.execute("""ALTER TABLE albums ADD COLUMN IF NOT EXISTS is_saved BOOLEAN DEFAULT TRUE;""")
cur.execute("""ALTER TABLE tracks ADD COLUMN IF NOT EXISTS is_liked BOOLEAN DEFAULT FALSE;""")
cur.execute("""ALTER TABLE tracks ADD COLUMN IF NOT EXISTS track_number INTEGER;""")
cur.execute("""ALTER TABLE tracks ADD COLUMN IF NOT EXISTS added_at TIMESTAMP;""")
cur.execute("""ALTER TABLE albums ADD COLUMN IF NOT EXISTS added_at TIMESTAMP;""")
cur.execute("""ALTER TABLE albums ADD COLUMN IF NOT EXISTS tracks_synced BOOLEAN DEFAULT FALSE;""")




# Plays table
cur.execute("""
CREATE TABLE IF NOT EXISTS plays (
    id SERIAL PRIMARY KEY,
    track_id TEXT REFERENCES tracks(id),
    played_at TIMESTAMP,
    UNIQUE(track_id, played_at)
);
""")

# Playlist mapping table
cur.execute("""
CREATE TABLE IF NOT EXISTS playlist_mappings (
    slug TEXT PRIMARY KEY,
    name TEXT,
    playlist_id TEXT
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created and updated successfully.")