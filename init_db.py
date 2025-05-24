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

cur.execute("""
CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    name TEXT,
    artist TEXT,
    album TEXT,
    is_liked BOOLEAN
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS plays (
    id SERIAL PRIMARY KEY,
    track_id TEXT REFERENCES tracks(id),
    played_at TIMESTAMP,
    UNIQUE(track_id, played_at)
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created successfully.")
