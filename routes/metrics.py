from flask import Blueprint, jsonify, render_template
from utils.db_utils import get_db_connection
from datetime import datetime, timedelta

metrics_bp = Blueprint("metrics", __name__)

@metrics_bp.route("/metrics-data")
def metrics_data():
    conn = get_db_connection()
    cur = conn.cursor()

    # Top Artists
    cur.execute("""
        SELECT artist, SUM(play_count) as play_count
        FROM unified_tracks
        GROUP BY artist
        ORDER BY play_count DESC
        LIMIT 10
    """)
    top_artists = [{"artist": row[0], "count": row[1]} for row in cur.fetchall()]

    # Top Tracks
    cur.execute("""
        SELECT track_name, artist, play_count
        FROM unified_tracks
        ORDER BY play_count DESC
        LIMIT 10
    """)
    top_tracks = [{"track": row[0], "artist": row[1], "count": row[2]} for row in cur.fetchall()]

    # Plays Per Day (last 30 days)
    cur.execute("""
        SELECT DATE(last_played_at), SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at > NOW() - INTERVAL '30 days'
        GROUP BY DATE(last_played_at)
        ORDER BY DATE(last_played_at)
    """)
    daily_plays = [{"date": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({
        "top_artists": top_artists,
        "top_tracks": top_tracks,
        "daily_plays": daily_plays,
    })

@metrics_bp.route("/metrics")
def metrics_page():
    return render_template("metrics.html")