import os
import psycopg2
import json
from datetime import datetime

def log_event(source, message, level="info", extra=None):
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs (timestamp, source, level, message, extra)
        VALUES (%s, %s, %s, %s, %s)
    """, (datetime.utcnow(), source, level, message, json.dumps(extra)))
    conn.commit()
    cur.close()
    conn.close()

