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
    top_tracks = [{"track": f"{row[0]} - {row[1]}", "count": row[2]} for row in cur.fetchall()]

    # Plays Per Day (last 30 days)
    cur.execute("""
        SELECT DATE(last_played_at), SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at > NOW() - INTERVAL '30 days'
        GROUP BY DATE(last_played_at)
        ORDER BY DATE(last_played_at)
    """)
    daily_plays = [{"date": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    # Top Albums
    cur.execute("""
        SELECT album_name, artist, SUM(play_count) as total_plays
        FROM unified_tracks
        GROUP BY album_name, artist
        ORDER BY total_plays DESC
        LIMIT 10
    """)
    top_albums = [{"album": f"{row[0]} - {row[1]}", "count": row[2]} for row in cur.fetchall()]

    # First Played
    cur.execute("""
        SELECT track_name, artist, first_played_at
        FROM unified_tracks
        WHERE first_played_at IS NOT NULL
        ORDER BY first_played_at ASC
        LIMIT 10
    """)
    first_played = [{"track": f"{row[0]} - {row[1]}", "date": row[2].isoformat()} for row in cur.fetchall()]

    # Recently Played
    cur.execute("""
        SELECT track_name, artist, last_played_at
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        ORDER BY last_played_at DESC
        LIMIT 10
    """)
    recent_plays = [{"track": f"{row[0]} - {row[1]}", "date": row[2].isoformat()} for row in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({
        "top_artists": top_artists,
        "top_tracks": top_tracks,
        "daily_plays": daily_plays,
        "top_albums": top_albums,
        "first_played": first_played,
        "recent_plays": recent_plays,
    })

@metrics_bp.route("/metrics")
def metrics_page():
    return render_template("metrics.html")