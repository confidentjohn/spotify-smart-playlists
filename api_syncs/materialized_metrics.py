


import decimal
import json
from datetime import datetime
from utils.db_utils import get_db_connection
from utils.logger import log_event
from routes.metrics import collect_metrics_payload

# Function to handle Decimal serialization
def decimal_converter(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)

# SQL to create the table if it doesn't exist
METRICS_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS daily_metrics_cache (
    snapshot_date DATE PRIMARY KEY DEFAULT CURRENT_DATE,
    data JSONB NOT NULL
);
"""

if __name__ == "__main__":
    log_event("materialize_metrics", "🟡 Starting metrics materialization...")
    conn = get_db_connection()
    cur = conn.cursor()

    # Ensure the table exists
    cur.execute(METRICS_CACHE_TABLE)
    conn.commit()

    # Replace any existing row for today
    cur.execute("DELETE FROM daily_metrics_cache WHERE snapshot_date = CURRENT_DATE")

    # Get fresh metrics
    metrics = collect_metrics_payload()

    # Insert into cache
    cur.execute(
        "INSERT INTO daily_metrics_cache (snapshot_date, data) VALUES (%s, %s)",
        (datetime.utcnow().date(), json.dumps(metrics, default=decimal_converter))
    )

    conn.commit()
    cur.close()
    conn.close()
    log_event("materialize_metrics", "✅ Daily metrics cached successfully.")