import json
from psycopg2.extras import RealDictCursor
from flask import Blueprint, jsonify, render_template
from utils.db_utils import get_db_connection
from datetime import datetime, timedelta

metrics_bp = Blueprint("metrics", __name__)

@metrics_bp.route("/cached-metrics-data")
def cached_metrics_data():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT data
        FROM daily_metrics_cache
        WHERE snapshot_date = CURRENT_DATE
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return jsonify(row["data"])
    else:
        return jsonify({"error": "No cached metrics found."}), 404

def collect_metrics_payload():
    conn = get_db_connection()
    cur = conn.cursor()

    # Top Artists
    cur.execute("""
        SELECT artist, artist_image, SUM(play_count) as play_count
        FROM unified_tracks
        WHERE artist_image IS NOT NULL
        GROUP BY artist, artist_image
        ORDER BY play_count DESC
        LIMIT 10
    """)
    top_artists = [
        {"artist": row[0], "image_url": row[1], "count": row[2]}
        for row in cur.fetchall()
    ]

    # Top Tracks
    cur.execute("""
        SELECT track_name, artist, album_image_url, play_count
        FROM unified_tracks
        WHERE album_image_url IS NOT NULL
        ORDER BY play_count DESC
        LIMIT 10
    """)
    top_tracks = [
        {"track_name": row[0], "artist": row[1], "image_url": row[2], "count": row[3]}
        for row in cur.fetchall()
    ]

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
        SELECT album_name, artist, album_image_url, SUM(play_count) as total_plays
        FROM unified_tracks
        WHERE album_image_url IS NOT NULL
        GROUP BY album_name, artist, album_image_url
        ORDER BY total_plays DESC
        LIMIT 10
    """)
    top_albums = [
        {"album_name": row[0], "artist": row[1], "image_url": row[2], "count": row[3]}
        for row in cur.fetchall()
    ]


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
        SELECT EXTRACT(HOUR FROM last_played_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS hour,
        SUM(play_count)
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

    # Time from Release to First Play
    cur.execute("""
        SELECT bucket, COUNT(*) AS count
        FROM (
          SELECT
            CASE
              WHEN EXTRACT(DAY FROM MIN(last_played_at) - parsed_release_date) <= 1 THEN '0-1 days'
              WHEN EXTRACT(DAY FROM MIN(last_played_at) - parsed_release_date) <= 7 THEN '2-7 days'
              WHEN EXTRACT(DAY FROM MIN(last_played_at) - parsed_release_date) <= 30 THEN '8-30 days'
              WHEN EXTRACT(DAY FROM MIN(last_played_at) - parsed_release_date) <= 90 THEN '31-90 days'
              ELSE '90+ days'
            END AS bucket
          FROM (
            SELECT
              track_name,
              artist,
              last_played_at,
              CASE
                WHEN LENGTH(release_date) = 4 THEN TO_DATE(release_date || '-01-01', 'YYYY-MM-DD')
                WHEN LENGTH(release_date) = 7 THEN TO_DATE(release_date || '-01', 'YYYY-MM-DD')
                WHEN LENGTH(release_date) = 10 THEN TO_DATE(release_date, 'YYYY-MM-DD')
                ELSE NULL
              END AS parsed_release_date
            FROM unified_tracks
            WHERE last_played_at IS NOT NULL
              AND release_date IS NOT NULL
              AND play_count > 0
          ) normalized
          WHERE parsed_release_date IS NOT NULL
            AND last_played_at >= parsed_release_date
          GROUP BY parsed_release_date, track_name, artist
        ) sub
        GROUP BY bucket
        ORDER BY
          CASE bucket
            WHEN '0-1 days' THEN 1
            WHEN '2-7 days' THEN 2
            WHEN '8-30 days' THEN 3
            WHEN '31-90 days' THEN 4
            ELSE 5
          END
    """)
    release_to_play = [{"bucket": row[0], "count": row[1]} for row in cur.fetchall()]

    # Monthly Increase in Library Size
    cur.execute("""
        SELECT DATE_TRUNC('month', added_at) AS month, COUNT(*) AS added
        FROM unified_tracks
        WHERE added_at IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)
    monthly_library_growth = [{"month": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    # Top Artist by Month
    cur.execute("""
        SELECT artist, month, plays FROM (
            SELECT
                artist,
                TO_CHAR(last_played_at, 'YYYY-MM') AS month,
                SUM(play_count) AS plays,
                ROW_NUMBER() OVER (PARTITION BY TO_CHAR(last_played_at, 'YYYY-MM') ORDER BY SUM(play_count) DESC) AS rank
            FROM unified_tracks
            WHERE last_played_at IS NOT NULL
            GROUP BY artist, TO_CHAR(last_played_at, 'YYYY-MM')
        ) ranked
        WHERE rank = 1
        ORDER BY month
    """)
    top_artist_by_month = [{"month": row[1], "artist": row[0], "count": row[2]} for row in cur.fetchall()]

    # Summary Stats
    cur.execute("""
        SELECT 
            COUNT(DISTINCT artist),
            COUNT(*),
            COUNT(*) FILTER (WHERE is_liked = TRUE),
            SUM(play_count)
        FROM unified_tracks
    """)
    row = cur.fetchone()
    summary_stats = {
        "total_artists": row[0],
        "total_tracks": row[1],
        "total_liked": row[2],
        "total_plays": row[3]
    }

    cur.close()
    conn.close()

    return {
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
        "release_to_play": release_to_play,
        "monthly_library_growth": monthly_library_growth,
        "top_artist_by_month": top_artist_by_month,
        "summary_stats": summary_stats,
    }

@metrics_bp.route("/metrics-data")
def metrics_data():
    return jsonify(collect_metrics_payload())

@metrics_bp.route("/metrics")
def metrics_page():
    return render_template("metrics.html")