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


    # Plays by Day of Week
    cur.execute("""
        SELECT TRIM(TO_CHAR(last_played_at, 'Day')) AS day, SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        GROUP BY TRIM(TO_CHAR(last_played_at, 'Day'))
        ORDER BY 
            CASE TRIM(TO_CHAR(last_played_at, 'Day'))
                WHEN 'Sunday' THEN 1
                WHEN 'Monday' THEN 2
                WHEN 'Tuesday' THEN 3
                WHEN 'Wednesday' THEN 4
                WHEN 'Thursday' THEN 5
                WHEN 'Friday' THEN 6
                WHEN 'Saturday' THEN 7
        END
    """)
    plays_by_day = [{"day": row[0].strip(), "count": row[1]} for row in cur.fetchall()]

    # Plays by Hour of Day
    cur.execute("""
        SELECT EXTRACT(HOUR FROM last_played_at) AS hour, SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """)
    plays_by_hour = [{"hour": int(row[0]), "count": row[1]} for row in cur.fetchall()]

    # Plays by Month
    cur.execute("""
        SELECT TO_CHAR(last_played_at, 'YYYY-MM') AS month, SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        GROUP BY TO_CHAR(last_played_at, 'YYYY-MM')
        ORDER BY month
    """)
    plays_by_month = [{"month": row[0], "count": row[1]} for row in cur.fetchall()]

    # Tracks Added Over Time
    cur.execute("""
        SELECT DATE(added_at), COUNT(*)
        FROM unified_tracks
        WHERE added_at IS NOT NULL
        GROUP BY DATE(added_at)
        ORDER BY DATE(added_at)
    """)
    tracks_added = [{"date": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    # Top Liked Artists
    cur.execute("""
        SELECT artist, COUNT(*) as liked_count
        FROM unified_tracks
        WHERE is_liked = TRUE
        GROUP BY artist
        ORDER BY liked_count DESC
        LIMIT 10
    """)
    top_liked_artists = [{"artist": row[0], "count": row[1]} for row in cur.fetchall()]

    # Unavailable Tracks Over Time
    cur.execute("""
        SELECT DATE(last_checked_at) AS check_date, COUNT(*)
        FROM unified_tracks
        WHERE is_playable = FALSE
        AND last_checked_at IS NOT NULL
        GROUP BY check_date
        ORDER BY check_date
    """)
    unplayable_tracks = [{"date": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({
        "top_artists": top_artists,
        "top_tracks": top_tracks,
        "daily_plays": daily_plays,
        "top_albums": top_albums,
        "plays_by_day": plays_by_day,
        "plays_by_hour": plays_by_hour,
        "plays_by_month": plays_by_month,
        "tracks_added": tracks_added,
        "top_liked_artists": top_liked_artists,
        "unplayable_tracks": unplayable_tracks,
    })

@metrics_bp.route("/metrics")
def metrics_page():
    return render_template("metrics.html")