import os
from utils.db_auth import get_db_connection
import json
from datetime import datetime

def log_event(source, message, level="info", extra=None):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs (timestamp, source, level, message, extra)
        VALUES (%s, %s, %s, %s, %s)
    """, (datetime.utcnow(), source, level, message, json.dumps(extra)))
    conn.commit()
    cur.close()
    conn.close()

