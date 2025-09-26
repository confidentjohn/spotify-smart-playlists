import os
import psycopg2
from utils.db_utils import get_db_connection

def run_init_db():
    # Connect to PostgreSQL
    conn = get_db_connection()

    cur = conn.cursor()

    # ─────────────────────────────────────────────
    # Albums table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS albums (
        id TEXT PRIMARY KEY,
        name TEXT,
        artist TEXT,
        artist_id TEXT,
        release_date TEXT,
        total_tracks INTEGER,
        is_saved BOOLEAN DEFAULT TRUE,
        added_at TIMESTAMP,
        tracks_synced BOOLEAN DEFAULT FALSE,
        album_type TEXT,
        album_image_url TEXT,
        tracks_checked_at TIMESTAMP
    );
    """)

    # Ensure all expected columns exist in the albums table
    expected_album_columns = {
        "id": "TEXT",
        "name": "TEXT",
        "artist": "TEXT",
        "artist_id": "TEXT",
        "release_date": "TEXT",
        "total_tracks": "INTEGER",
        "is_saved": "BOOLEAN DEFAULT TRUE",
        "added_at": "TIMESTAMP",
        "tracks_synced": "BOOLEAN DEFAULT FALSE",
        "album_type": "TEXT",
        "album_image_url": "TEXT",
        "tracks_checked_at": "TIMESTAMP"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'albums';")
    existing_album_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_album_columns.items():
        if col_name not in existing_album_columns:
            print(f"🛠 Adding missing column to albums: {col_name}")
            cur.execute(f"ALTER TABLE albums ADD COLUMN {col_name} {col_type};")

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
        from_album BOOLEAN DEFAULT FALSE,
        track_number INTEGER,
        disc_number INTEGER,
        added_at TIMESTAMP,
        duration_ms INTEGER,
        popularity INTEGER
    );
    """)

    # Ensure all expected columns exist in the tracks table
    expected_track_columns = {
        "id": "TEXT",
        "name": "TEXT",
        "artist": "TEXT",
        "album": "TEXT",
        "album_id": "TEXT",
        "from_album": "BOOLEAN DEFAULT FALSE",
        "track_number": "INTEGER",
        "disc_number": "INTEGER",
        "added_at": "TIMESTAMP",
        "duration_ms": "INTEGER",
        "popularity": "INTEGER"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tracks';")
    existing_track_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_track_columns.items():
        if col_name not in existing_track_columns:
            print(f"🛠 Adding missing column to tracks: {col_name}")
            cur.execute(f"ALTER TABLE tracks ADD COLUMN {col_name} {col_type};")

    # ─────────────────────────────────────────────
    # Plays table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS plays (
        id SERIAL PRIMARY KEY,
        track_id TEXT,
        played_at TIMESTAMP,
        track_name TEXT,
        artist_id TEXT,
        duration_ms INTEGER,
        artist_name TEXT,
        album_id TEXT,
        album_name TEXT,
        album_type TEXT,
        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(track_id, played_at)
    );
    """)

    # Explicitly create a named unique index
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_plays_unique ON plays (track_id, played_at);
    """)

    # Ensure all expected columns exist in the plays table
    expected_play_columns = {
        "id": "SERIAL",
        "track_id": "TEXT",
        "played_at": "TIMESTAMP",
        "track_name": "TEXT",
        "artist_id": "TEXT",
        "duration_ms": "INTEGER",
        "artist_name": "TEXT",
        "album_id": "TEXT",
        "album_name": "TEXT",
        "album_type": "TEXT"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'plays';")
    existing_play_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_play_columns.items():
        if col_name not in existing_play_columns:
            print(f"🛠 Adding missing column to plays: {col_name}")
            cur.execute(f"ALTER TABLE plays ADD COLUMN {col_name} {col_type};")

    # ─────────────────────────────────────────────
    # Spotify play history table (same schema as plays; optional import target)
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS spotify_play_history (
        id SERIAL PRIMARY KEY,
        track_id TEXT,
        played_at TIMESTAMP,
        track_name TEXT,
        artist_id TEXT,
        duration_ms INTEGER,
        artist_name TEXT,
        album_id TEXT,
        album_name TEXT,
        album_type TEXT,
        checked_at TIMESTAMP,
        UNIQUE(track_id, played_at)
    );
    """)

    # Explicitly create a named unique index to mirror plays
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_spotify_play_history_unique ON spotify_play_history (track_id, played_at);
    """)

    # Ensure all expected columns exist in the spotify_play_history table
    expected_history_columns = {
        "id": "SERIAL",
        "track_id": "TEXT",
        "played_at": "TIMESTAMP",
        "track_name": "TEXT",
        "artist_id": "TEXT",
        "duration_ms": "INTEGER",
        "artist_name": "TEXT",
        "album_id": "TEXT",
        "album_name": "TEXT",
        "album_type": "TEXT",
        "checked_at": "TIMESTAMP"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'spotify_play_history';")
    existing_history_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_history_columns.items():
        if col_name not in existing_history_columns:
            print(f"🛠 Adding missing column to spotify_play_history: {col_name}")
            cur.execute(f"ALTER TABLE spotify_play_history ADD COLUMN {col_name} {col_type};")

    # ─────────────────────────────────────────────
    # Apple Music plays table (staging for Apple history import)
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS apple_music_plays (
        played_at TIMESTAMP NOT NULL,
        hour_assigned INTEGER,
        track_description TEXT,
        apple_track_id BIGINT,
        play_duration_ms INTEGER
    );
    """)

    # Helpful indexes for lookups and joins
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_apple_music_plays_track_id ON apple_music_plays(apple_track_id);
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_apple_music_plays_played_at ON apple_music_plays(played_at);
    """)

    # Ensure expected columns exist in apple_music_plays
    expected_apple_columns = {
        "played_at": "TIMESTAMP NOT NULL",
        "hour_assigned": "INTEGER",
        "track_description": "TEXT",
        "apple_track_id": "BIGINT",
        "play_duration_ms": "INTEGER",
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'apple_music_plays';")
    existing_apple_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_apple_columns.items():
        if col_name not in existing_apple_columns:
            print(f"🛠 Adding missing column to apple_music_plays: {col_name}")
            cur.execute(f"ALTER TABLE apple_music_plays ADD COLUMN {col_name} {col_type};")


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
        rules JSONB,
        is_dynamic BOOLEAN DEFAULT TRUE,
        snapshot_id TEXT,
        last_synced_hash TEXT
    );
    """)

    # Ensure all expected columns exist in the playlist_mappings table
    expected_pm_columns = {
        "slug": "TEXT",
        "name": "TEXT",
        "playlist_id": "TEXT",
        "last_synced_at": "TIMESTAMP",
        "status": "TEXT DEFAULT 'active'",
        "track_count": "INTEGER DEFAULT 0",
        "rules": "JSONB",
        "is_dynamic": "BOOLEAN DEFAULT TRUE",
        "snapshot_id": "TEXT",
        "last_synced_hash": "TEXT",
        "pending_delete": "BOOLEAN DEFAULT FALSE",
        "missing_count": "INTEGER DEFAULT 0",
        "last_seen_spotify_at": "TIMESTAMP",
        "last_missing_at": "TIMESTAMP",
        "last_missing_reason": "TEXT"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'playlist_mappings';")
    existing_pm_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_pm_columns.items():
        if col_name not in existing_pm_columns:
            print(f"🛠 Adding missing column to playlist_mappings: {col_name}")
            cur.execute(f"ALTER TABLE playlist_mappings ADD COLUMN {col_name} {col_type};")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_pm_pending_delete ON playlist_mappings (pending_delete)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pm_last_missing_at ON playlist_mappings (last_missing_at)")

    # ─────────────────────────────────────────────
    # Track availability table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS track_availability (
        track_id TEXT PRIMARY KEY,
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
        track_artist TEXT,
        album_id TEXT,
        album_in_library BOOLEAN DEFAULT FALSE,
        duration_ms INTEGER,
        popularity INTEGER
    );
    """)

    # Ensure all expected columns exist in the liked_tracks table
    expected_liked_columns = {
        "track_id": "TEXT",
        "liked_at": "TIMESTAMP",
        "added_at": "TIMESTAMP",
        "last_checked_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "track_name": "TEXT",
        "track_artist": "TEXT",
        "album_id": "TEXT",
        "artist_id": "TEXT",
        "album_in_library": "BOOLEAN DEFAULT FALSE",
        "duration_ms": "INTEGER",
        "popularity": "INTEGER"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'liked_tracks';")
    existing_liked_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_liked_columns.items():
        if col_name not in existing_liked_columns:
            print(f"🛠 Adding missing column to liked_tracks: {col_name}")
            cur.execute(f"ALTER TABLE liked_tracks ADD COLUMN {col_name} {col_type};")

    # Ensure all columns are populated with fallback values if NULL
    cur.execute("""
        UPDATE liked_tracks SET album_in_library = FALSE WHERE album_in_library IS NULL;
    """)
    cur.execute("""
        UPDATE liked_tracks SET last_checked_at = CURRENT_TIMESTAMP WHERE last_checked_at IS NULL;
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
    # Resolved fuzzy matches table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS resolved_fuzzy_matches (
        track_id TEXT PRIMARY KEY,
        resolved_at TIMESTAMP DEFAULT NOW()
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

    # ─────────────────────────────────────────────
    # Canonical album matches table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS canonical_album_matches (
        album_id TEXT PRIMARY KEY,
        artist_id TEXT NOT NULL,
        album_name TEXT NOT NULL,
        matched_canonical_id TEXT,
        matched_album_name TEXT,
        match_status TEXT NOT NULL
    );
    """)

    # ─────────────────────────────────────────────
    # Users table (multi-user support)
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        spotify_user_id TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        spotify_refresh_token TEXT
    );
    """)


    # ─────────────────────────────────────────────
    # Artists table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS artists (
        id TEXT PRIMARY KEY,
        name TEXT,
        genres TEXT[],
        image_url TEXT,
        last_checked_at TIMESTAMP,
        last_album_checked_at TIMESTAMP
    );
    """)

    # Ensure all expected columns exist in the artists table
    expected_artist_columns = {
        "id": "TEXT",
        "name": "TEXT",
        "genres": "TEXT[]",
        "image_url": "TEXT",
        "last_checked_at": "TIMESTAMP",
        "last_album_checked_at": "TIMESTAMP"
    }

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'artists';")
    existing_artist_columns = {row[0] for row in cur.fetchall()}

    for col_name, col_type in expected_artist_columns.items():
        if col_name not in existing_artist_columns:
            print(f"🛠 Adding missing column to artists: {col_name}")
            cur.execute(f"ALTER TABLE artists ADD COLUMN {col_name} {col_type};")

    # ─────────────────────────────────────────────
    # Outdated albums table
    # ─────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outdated_albums (
        artist_id TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        album_name TEXT NOT NULL,
        saved_album_id TEXT NOT NULL,
        newer_album_id TEXT NOT NULL,
        first_detected_at TIMESTAMP DEFAULT NOW(),
        last_checked_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (saved_album_id, newer_album_id)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

    # Build the unified_tracks materialized view
    import subprocess
    subprocess.run(["python", "-m", "api_syncs.materialized_views"], check=True)

    # Build the daily_metrics_cache table
    subprocess.run(["python", "-m", "api_syncs.materialized_metrics"], check=True)

    print("✅ Tables created and updated successfully.")

if __name__ == "__main__":
    run_init_db()