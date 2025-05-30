import os
import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
)

cur = conn.cursor()

# ─────────────────────────────────────────────
# Albums table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS albums (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    release_date TEXT,
    total_tracks INTEGER,
    is_saved BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP,
    tracks_synced BOOLEAN DEFAULT FALSE
);
""")

# ─────────────────────────────────────────────
# Tracks table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    album TEXT,
    album_id TEXT,
    is_liked BOOLEAN DEFAULT FALSE,
    from_album BOOLEAN DEFAULT FALSE,
    track_number INTEGER,
    added_at TIMESTAMP
);
""")

# ─────────────────────────────────────────────
# Plays table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS plays (
    id SERIAL PRIMARY KEY,
    track_id TEXT,
    played_at TIMESTAMP,
    UNIQUE(track_id, played_at)
);
""")

# Explicitly create a named unique index (optional but helpful for tuning/visibility)
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_plays_unique ON plays (track_id, played_at);
""")

# ─────────────────────────────────────────────
# Playlist mapping table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS playlist_mappings (
    slug TEXT PRIMARY KEY,
    name TEXT,
    playlist_id TEXT
);
""")

# ─────────────────────────────────────────────
# Track availability table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS track_availability (
    track_id TEXT PRIMARY KEY REFERENCES tracks(id),
    is_playable BOOLEAN,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created and updated successfully.")
