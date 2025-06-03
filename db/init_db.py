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
# Tracks table (UPDATED — is_liked removed)
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    album TEXT,
    album_id TEXT,
    from_album BOOLEAN DEFAULT FALSE,
    track_number INTEGER,
    disc_number INTEGER,
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

# Explicitly create a named unique index
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
    playlist_id TEXT,
    last_synced_at TIMESTAMP,
    status TEXT DEFAULT 'active',
    track_count INTEGER DEFAULT 0,
    rules JSONB
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

# ─────────────────────────────────────────────
# Liked tracks table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS liked_tracks (
    track_id TEXT PRIMARY KEY,
    liked_at TIMESTAMP,
    added_at TIMESTAMP,
    last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    track_name TEXT,
    track_artist TEXT
);
""")

# ─────────────────────────────────────────────
# Excluded tracks table
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS excluded_tracks (
    track_id TEXT PRIMARY KEY
);
""")

# ─────────────────────────────────────────────
# Logging table (MATCHES logger.py)
# ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    source TEXT NOT NULL,
    level TEXT DEFAULT 'info',
    message TEXT NOT NULL,
    extra JSONB
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created and updated successfully.")