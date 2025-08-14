import os
import json
import psycopg2
import hashlib
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from routes.rule_parser import build_track_query
from utils.logger import log_event
from utils.spotify_auth import get_spotify_client
from datetime import datetime

def compute_tracklist_hash(track_uris):
    joined = ",".join(track_uris)  # order matters
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()

def sync_playlist(slug):
    log_event("generate_playlist", f"🔁 Starting sync for playlist slug: '{slug}'")
    try:
        from utils.db_utils import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT name, playlist_id, rules, is_dynamic FROM playlist_mappings WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Playlist with slug '{slug}' not found")

        name, playlist_url, rules_json, is_dynamic = row
        playlist_id = playlist_url.split("/")[-1]

        # Spotify check first
        sp = get_spotify_client()
        try:
            user_id = sp.current_user()["id"]
            playlists = []
            results = sp.current_user_playlists()
            while results:
                playlists.extend(results['items'])
                results = sp.next(results) if results.get('next') else None

            user_playlist_ids = {pl["id"] for pl in playlists}
            if playlist_id not in user_playlist_ids:
                reason = "playlist not found in user's library"
                log_event("generate_playlist", f"⚠️ Playlist '{playlist_id}' not found in user's library. Soft-flagging in DB instead of deleting.")
                cur.execute(
                    """
                    UPDATE playlist_mappings
                    SET pending_delete = TRUE,
                        missing_count = COALESCE(missing_count, 0) + 1,
                        last_missing_at = NOW(),
                        last_missing_reason = %s,
                        last_seen_spotify_at = NOW()
                    WHERE slug = %s
                    """,
                    (reason, slug)
                )
                conn.commit()
                return
            # Reset soft-delete flags if previously marked missing
            cur.execute(
                """
                UPDATE playlist_mappings
                SET pending_delete = FALSE,
                    missing_count = 0,
                    last_missing_at = NULL,
                    last_missing_reason = NULL
                WHERE slug = %s
                """,
                (slug,)
            )
            conn.commit()
        except Exception as e:
            reason = str(e)
            log_event("generate_playlist", f"⚠️ Playlist '{playlist_id}' not accessible. Soft-flagging in DB instead of deleting. Error: {e}")
            cur.execute(
                """
                UPDATE playlist_mappings
                SET pending_delete = TRUE,
                    missing_count = COALESCE(missing_count, 0) + 1,
                    last_missing_at = NOW(),
                    last_missing_reason = %s,
                    last_seen_spotify_at = NOW()
                WHERE slug = %s
                """,
                (reason[:500], slug)
            )
            conn.commit()
            return

        if not is_dynamic:
            log_event("generate_playlist", f"⏭ Skipped legacy playlist '{name}' (not dynamic)")
            return

        if slug == "exclusions":
            log_event("generate_playlist", "⏭ Skipped 'exclusions' playlist (manually managed)")
            return

        try:
            log_event("generate_playlist", f"📥 Raw rules_json for '{slug}': {rules_json} (type: {type(rules_json)})")
            if isinstance(rules_json, dict):
                rules = rules_json
            else:
                rules = json.loads(rules_json or "{}")
            log_event("generate_playlist", f"📋 Successfully loaded rules for '{slug}': {rules} (type: {type(rules)})")
        except Exception as e:
            log_event("generate_playlist", f"❌ Failed to parse rules for '{slug}': {e} — rules_json was: {rules_json}", level="error")
            return

        try:
            query = build_track_query(rules)
            log_event("generate_playlist", f"🔍 Running query: {query}")
            log_event("generate_playlist", f"🛠 SQL Query: {query} | Params: []")
            cur.execute(query)
            rows = cur.fetchall()
            log_event("generate_playlist", f"📊 Fetched rows: {len(rows)} | Sample: {rows[:5]}")
            track_uris = [row[0] for row in rows if row and row[0]]
            log_event("generate_playlist", f"📦 Track URIs fetched: {track_uris}")

            new_hash = compute_tracklist_hash(track_uris)
            cur.execute("SELECT last_synced_hash FROM playlist_mappings WHERE slug = %s", (slug,))
            last_hash_row = cur.fetchone()
            last_synced_hash = last_hash_row[0] if last_hash_row else None
            log_event("generate_playlist", f"🧠 Computed hash: {new_hash} | Stored hash: {last_synced_hash}")

            if last_synced_hash == new_hash:
                log_event("generate_playlist", f"✅ Playlist '{slug}' unchanged — skipping update.")
                cur.execute("UPDATE playlist_mappings SET last_synced_at = %s WHERE slug = %s", (datetime.utcnow(), slug))
                conn.commit()
                return
        except Exception as query_error:
            log_event("generate_playlist", f"❌ Error building/executing track query for '{slug}': {query_error} — rules: {rules}", level="error")
            return

        # Always clear the playlist before re-adding
        log_event("generate_playlist", f"🧹 Clearing playlist '{slug}' before re-adding tracks")
        sp = get_spotify_client()
        user = sp.current_user()
        sp.user_playlist_replace_tracks(user["id"], playlist_id, [])

        if not track_uris:
            log_event("generate_playlist", f"⚠️ No tracks found for '{slug}' — playlist was cleared.")
            cur.execute("UPDATE playlist_mappings SET track_count = 0, last_synced_at = %s WHERE slug = %s", (datetime.utcnow(), slug))
            conn.commit()
            return

        log_event("generate_playlist", f"🎧 Retrieved {len(track_uris)} tracks for '{slug}'")

        log_event("generate_playlist", f"➕ Adding {len(track_uris)} tracks to playlist '{slug}' in batches of 100")
        for i in range(0, len(track_uris), 100):
            sp.playlist_add_items(playlist_id, track_uris[i:i + 100])

        cur.execute("UPDATE playlist_mappings SET track_count = %s, last_synced_at = %s, last_synced_hash = %s WHERE slug = %s", (len(track_uris), datetime.utcnow(), new_hash, slug))
        conn.commit()
        log_event("generate_playlist", f"📝 Updated playlist_mappings for '{slug}' with {len(track_uris)} track URIs")

        log_event("generate_playlist", f"✅ Synced {len(track_uris)} tracks to playlist '{name}'")

    except Exception as e:
        log_event("generate_playlist", f"❌ Failed to sync playlist '{slug}': {e}", level="error")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
def delete_playlist(slug):
    log_event("delete_playlist", f"🗑 Attempting to delete playlist with slug: '{slug}'")
    try:
        from utils.db_utils import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT playlist_id FROM playlist_mappings WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            log_event("delete_playlist", f"⚠️ Playlist with slug '{slug}' not found in DB")
            return

        playlist_url = row[0]
        playlist_id = playlist_url.split("/")[-1]

        sp = get_spotify_client()
        user_id = sp.current_user()["id"]

        try:
            sp.current_user_unfollow_playlist(playlist_id)
            log_event("delete_playlist", f"✅ Successfully unfollowed playlist '{playlist_id}' on Spotify")
        except Exception as e:
            log_event("delete_playlist", f"⚠️ Failed to unfollow playlist '{playlist_id}' on Spotify: {e}")

        cur.execute("DELETE FROM playlist_mappings WHERE slug = %s", (slug,))
        conn.commit()
        log_event("delete_playlist", f"🧹 Deleted playlist '{slug}' from DB")

    except Exception as e:
        log_event("delete_playlist", f"❌ Failed to delete playlist '{slug}': {e}", level="error")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()