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
        ORDER BY snapshot_date DESC
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
        SELECT artist, COALESCE(artist_image, '/app/static/img/no_image.png') AS image_url, SUM(play_count) as play_count
        FROM unified_tracks
        WHERE play_count > 0
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
        SELECT track_name, artist, COALESCE(album_image_url, artist_image, '/app/static/img/no_image.png') AS image_url, play_count
        FROM unified_tracks
        WHERE play_count > 0
        ORDER BY play_count DESC
        LIMIT 10
    """)
    top_tracks = [
        {"track_name": row[0], "artist": row[1], "image_url": row[2], "count": row[3]}
        for row in cur.fetchall()
    ]

    # Plays Per Day (last 30 days)
    cur.execute("""
        SELECT DATE(played_at) AS play_date, COUNT(*) AS daily_play_count
        FROM plays
        WHERE played_at >= NOW() - INTERVAL '30 days'
        GROUP BY play_date
        ORDER BY play_date;
    """)
    daily_plays = [{"date": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    # Top Albums
    cur.execute("""
        SELECT album_name, artist, COALESCE(MIN(album_image_url),'/app/static/img/no_image.png') AS image_url, SUM(play_count) AS total_plays
        FROM unified_tracks
        WHERE play_count > 0 AND track_source = 'library' AND album_type IN ('album', 'compilation')
        GROUP BY album_name, artist
        ORDER BY total_plays DESC
        LIMIT 10;
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
    rows = cur.fetchall()
    total_daily_plays = sum(row[1] for row in rows)
    plays_by_day = [
        {"day": row[0].strip(), "percentage": round((row[1] / total_daily_plays) * 100, 1)}
        for row in rows
    ]

    # Plays by Hour of Day
    cur.execute("""
        SELECT EXTRACT(HOUR FROM last_played_at_est) AS hour,
        SUM(play_count)
        FROM unified_tracks
        WHERE last_played_at_est IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """)
    rows = cur.fetchall()
    total_hourly_plays = sum(row[1] for row in rows)
    plays_by_hour = [
        {"hour": int(row[0]), "percentage": round((row[1] / total_hourly_plays) * 100, 1)}
        for row in rows
    ]

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
        SELECT artist, artist_image, COUNT(*) as liked_count
        FROM unified_tracks
        WHERE is_liked = TRUE AND artist_image IS NOT NULL
        GROUP BY artist, artist_image
        ORDER BY liked_count DESC
        LIMIT 10
    """)
    top_liked_artists = [{"artist": row[0], "image_url": row[1], "count": row[2]} for row in cur.fetchall()]

    # Top Genres by First Genre
    cur.execute("""
        SELECT TRIM(genres[1]) AS primary_genre, SUM(play_count) AS total_plays
        FROM unified_tracks
        WHERE play_count > 0 AND genres IS NOT NULL AND array_length(genres, 1) > 0
        GROUP BY primary_genre
        ORDER BY total_plays DESC
        LIMIT 10
    """)
    top_genres = [{"genre": row[0], "count": row[1]} for row in cur.fetchall()]

    # Popularity Distribution of Liked Tracks
    cur.execute("""
        SELECT
          CASE
            WHEN popularity >= 90 THEN '90–100'
            WHEN popularity >= 80 THEN '80–89'
            WHEN popularity >= 70 THEN '70–79'
            WHEN popularity >= 60 THEN '60–69'
            WHEN popularity >= 50 THEN '50–59'
            WHEN popularity >= 40 THEN '40–49'
            WHEN popularity >= 30 THEN '30–39'
            WHEN popularity >= 20 THEN '20–29'
            WHEN popularity >= 10 THEN '10–19'
            ELSE '0–9'
          END AS popularity_range,
          COUNT(*) AS count
        FROM unified_tracks
        WHERE is_liked = TRUE AND popularity IS NOT NULL
        GROUP BY popularity_range
        ORDER BY popularity_range
    """)
    popularity_distribution = [{"range": row[0], "count": row[1]} for row in cur.fetchall()]

    # Average Popularity Score
    cur.execute("""
        SELECT ROUND(AVG(popularity), 1)
        FROM unified_tracks
        WHERE is_liked = TRUE AND popularity IS NOT NULL
    """)
    avg_popularity_score = cur.fetchone()[0] or 0

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
            SUM(play_count),
            COUNT(*) FILTER (WHERE play_count > 0),
            SUM(duration_ms * play_count)
        FROM unified_tracks
    """)
    row = cur.fetchone()
    total_ms = row[5] or 0
    total_seconds = total_ms // 1000
    years = total_seconds // (365 * 86400)
    remaining = total_seconds % (365 * 86400)
    days = remaining // 86400
    remaining %= 86400
    hours = remaining // 3600
    minutes = (remaining % 3600) // 60
    seconds = remaining % 60
    # Conditional formatting for total_time_spent to omit zero units at the beginning
    time_parts = []
    if years > 0:
        time_parts.append(f"{years}y")
    if years > 0 or days > 0:
        time_parts.append(f"{days}d")
    time_parts.append(f"{hours}h {minutes}m {seconds}s")
    formatted_time_spent = ' '.join(time_parts)

    cur.execute("""
        SELECT
        COUNT(DISTINCT album_id) FILTER (WHERE COALESCE(album_type, 'single') = 'album'),
        COUNT(DISTINCT album_id) FILTER (WHERE COALESCE(album_type, 'single') = 'single'),
        COUNT(DISTINCT album_id) FILTER (WHERE COALESCE(album_type, 'single') = 'compilation')
        FROM unified_tracks
        WHERE album_id IS NOT NULL AND track_source = 'library'
    """)
    album_counts = cur.fetchone()

    cur.execute("""
        SELECT COUNT(slug)
        FROM playlist_mappings
        WHERE is_dynamic IS TRUE
    """)
    dynamic_playlists_count = cur.fetchone()[0] or 0

    # Longest consecutive listening streak
    cur.execute("""
        SELECT DATE(last_played_at) AS day
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        GROUP BY DATE(last_played_at)
        ORDER BY day
    """)
    dates = [row[0] for row in cur.fetchall()]
    streak = max_streak = 1 if dates else 0
    for i in range(1, len(dates)):
        if (dates[i] - dates[i-1]).days == 1:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 1

    # Active days per week (weekly average of distinct days)
    cur.execute("""
        SELECT DATE_TRUNC('week', last_played_at) AS week, COUNT(DISTINCT DATE(last_played_at)) AS active_days
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
        GROUP BY week
    """)
    weekly_active_days = [row[1] for row in cur.fetchall()]
    avg_active_days_per_week = round(sum(weekly_active_days) / len(weekly_active_days), 1) if weekly_active_days else 0

    # Average Listens per Day
    cur.execute("""
        SELECT COUNT(*)
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
    """)
    total_listens = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT MIN(DATE(last_played_at)), MAX(DATE(last_played_at))
        FROM unified_tracks
        WHERE last_played_at IS NOT NULL
    """)
    min_date, max_date = cur.fetchone()
    day_span = (max_date - min_date).days + 1 if min_date and max_date else 1
    avg_listens_per_day = round(total_listens / day_span, 1)

    # Average Listens per Month
    month_span = ((max_date.year - min_date.year) * 12 + max_date.month - min_date.month + 1) if min_date and max_date else 1
    avg_listens_per_month = round(total_listens / month_span, 1)

    summary_stats = {
        "total_artists": row[0],
        "total_tracks": row[1],
        "total_liked": row[2],
        "total_plays": row[3],
        "total_unique_plays": row[4],
        "total_time_spent": formatted_time_spent,
        "total_albums": album_counts[0],
        "total_singles": album_counts[1],
        "total_compilations": album_counts[2],
        "total_dynamic_playlists": dynamic_playlists_count,
        "longest_listening_streak": max_streak,
        "avg_listens_per_day": avg_listens_per_day,
        "avg_listens_per_month": avg_listens_per_month,
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
        "top_genres": top_genres,
        "popularity_distribution": popularity_distribution,
        "avg_popularity_score": avg_popularity_score,
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