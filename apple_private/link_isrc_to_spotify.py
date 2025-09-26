#!/usr/bin/env python3
import os, sys, time, math
from typing import Optional, Tuple
import requests
import psycopg2
from spotipy import Spotify
from spotipy.exceptions import SpotifyException

# --- make project utils importable (same pattern as your other jobs)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import get_db_connection
from utils.spotify_auth import get_spotify_client
from utils.logger import log_event

BATCH_LIMIT   = int(os.getenv("ISRC_LINK_LIMIT", "500"))     # rows per run
SLEEP_BETWEEN = float(os.getenv("ISRC_LINK_SLEEP", "0.10"))  # light throttle
MARKET        = os.getenv("ISRC_LINK_MARKET", "US")

def safe_spotify_call(sp: Spotify, func, *args, **kwargs):
    """Retry on 429 and transient network errors."""
    attempts = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            # rate limit
            if e.http_status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 5))
                attempts += 1
                log_event("isrc_link", f"429 rate limit. retry {attempts} in {retry_after}s")
                time.sleep(retry_after)
                continue
            # auth problems should fail fast
            log_event("isrc_link", f"spotify exception: {e}", level="error")
            raise
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            attempts += 1
            backoff = min(30, 2 ** min(attempts, 4))
            log_event("isrc_link", f"net error: {e}. retry {attempts} in {backoff}s")
            time.sleep(backoff)

def fetch_pending_rows(cur) -> list[Tuple[int, Optional[str], Optional[int], Optional[str]]]:
    """
    Returns rows: (apple_track_id, isrc, duration_ms, artist_name)
    Only rows with ISRC present and spotify_checked_at NULL.
    """
    cur.execute(
        """
        SELECT apple_track_id, isrc, duration_ms, artist_name
        FROM apple_unique_track_ids
        WHERE isrc IS NOT NULL
          AND spotify_checked_at IS NULL
        ORDER BY apple_track_id
        LIMIT %s;
        """,
        (BATCH_LIMIT,)
    )
    return cur.fetchall()

def pick_best(candidates: list, target_duration: Optional[int], target_artist: Optional[str]) -> Optional[str]:
    """
    candidates = list of Spotify track objects.
    Strategy:
      1) exact ISRC match (belt & braces)
      2) else: closest duration (abs delta), tie-break by looser artist name match
    """
    if not candidates:
        return None

    # exact ISRC guard (sometimes Spotify returns additional items)
    for c in candidates:
        isrc = c.get("external_ids", {}).get("isrc")
        if isrc and isrc.lower():
            # already searched by isrc, but double-check equality
            return c.get("id")

    # duration-based pick
    best_id = None
    best_delta = math.inf
    norm_artist = (target_artist or "").strip().lower()

    for c in candidates:
        dur = c.get("duration_ms")
        delta = abs((target_duration or 0) - (dur or 0)) if (target_duration and dur) else math.inf
        score_bump = 0
        # tiny artist nudge if the first artist name matches roughly
        sp_artist = ""
        arts = c.get("artists") or []
        if arts:
            sp_artist = (arts[0].get("name") or "").lower()

        if norm_artist and sp_artist and (norm_artist in sp_artist or sp_artist in norm_artist):
            score_bump = -500   # prefer these a bit more (reduce effective delta)

        eff_delta = max(0, delta + score_bump)
        if eff_delta < best_delta:
            best_delta = eff_delta
            best_id = c.get("id")

    return best_id

def update_row(cur, apple_id: int, spotify_id: Optional[str]):
    if spotify_id:
        cur.execute(
            """
            UPDATE apple_unique_track_ids
               SET spotify_track_id   = %s,
                   spotify_checked_at = NOW()
             WHERE apple_track_id = %s;
            """,
            (spotify_id, apple_id)
        )
    else:
        cur.execute(
            """
            UPDATE apple_unique_track_ids
               SET spotify_checked_at = NOW()
             WHERE apple_track_id = %s;
            """,
            (apple_id,)
        )

def main():
    sp = get_spotify_client()  # uses your env secrets via utils.spotify_auth

    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM apple_unique_track_ids WHERE isrc IS NOT NULL AND spotify_checked_at IS NULL;")
        total_pending = cur.fetchone()[0]
        log_event("isrc_link", f"pending rows: {total_pending}")

        cur2 = conn.cursor()

        processed = 0
        while True:
            rows = fetch_pending_rows(cur)
            if not rows:
                break

            for apple_id, isrc, dur_ms, artist_name in rows:
                try:
                    q = f"isrc:{isrc}"
                    res = safe_spotify_call(sp, sp.search, q=q, type="track", limit=5, market=MARKET)
                    items = (res or {}).get("tracks", {}).get("items", []) or []

                    best_spotify_id = pick_best(items, dur_ms, artist_name)

                    if best_spotify_id:
                        update_row(cur2, apple_id, best_spotify_id)
                        log_event("isrc_link", f"link ok apple={apple_id} isrc={isrc} -> sp={best_spotify_id}")
                    else:
                        update_row(cur2, apple_id, None)
                        log_event("isrc_link", f"link miss apple={apple_id} isrc={isrc} -> no_spotify")

                except Exception as e:
                    update_row(cur2, apple_id, None)
                    log_event("isrc_link", f"error apple={apple_id} isrc={isrc} {e}", level="error")

                processed += 1
                if processed % 100 == 0:
                    conn.commit()
                    log_event("isrc_link", f"progress {processed}")

                time.sleep(SLEEP_BETWEEN)

            conn.commit()

        log_event("isrc_link", f"done processed={processed}")

if __name__ == "__main__":
    main()