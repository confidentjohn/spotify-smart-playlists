#!/usr/bin/env python3
"""
Backfill Spotify track IDs and metadata for Apple tracks
using a text search by (title, artist, album).

Runs in batches and is safe to re-run:
- Only processes rows where spotify_track_id IS NULL AND checked_at_spotify_back_fill IS NULL
- On a confident match, fills all spotify_* columns + stamps spotify_checked_at and checked_at_spotify_back_fill
- On no confident match, stamps checked_at_spotify_back_fill so we don't keep retrying endlessly

Relies on:
  utils.db_utils.get_db_connection()
  utils.spotify_auth.get_spotify_client()
  utils.logger.log_event()

This script purposely does NOT depend on Apple/ISRC; it’s a fuzzy search fallback
after ISRC and exact joins have been exhausted.
"""
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from difflib import SequenceMatcher

# Ensure local utils are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
from psycopg2.extras import execute_values
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from utils.db_utils import get_db_connection
from utils.spotify_auth import get_spotify_client
from utils.logger import log_event

# -----------------------
# Tunables (via env vars)
# -----------------------
BATCH_SIZE = int(os.getenv("SPOTIFY_FIELDS_BATCH", "200"))
PAUSE_BETWEEN_BATCHES = float(os.getenv("SPOTIFY_FIELDS_PAUSE_S", "1.0"))

# Matching thresholds
TITLE_SIM_MIN = float(os.getenv("SPOTIFY_FIELDS_TITLE_SIM_MIN", "0.70"))
ALBUM_SIM_MIN = float(os.getenv("SPOTIFY_FIELDS_ALBUM_SIM_MIN", "0.35"))
DUR_MAX_MS     = int(os.getenv("SPOTIFY_FIELDS_DUR_MAX_MS", "20000"))   # 20s
DUR_TIGHT_MS   = int(os.getenv("SPOTIFY_FIELDS_DUR_TIGHT_MS", "5000"))  # 5s

# Weighting for single score (still keep hard gates above)
W_TITLE = float(os.getenv("SPOTIFY_FIELDS_W_TITLE", "0.75"))
W_ALBUM = float(os.getenv("SPOTIFY_FIELDS_W_ALBUM", "0.20"))
W_DUR   = float(os.getenv("SPOTIFY_FIELDS_W_DUR",   "0.05"))


# -----------------------
# Helpers
# -----------------------
_title_strip_pat = re.compile(
    r"(?i)\s*(?:-\s*)?(?:"
    r"\(\s*\d{4}\s*remaster(?:ed)?\s*\)|"
    r"\(\s*remaster(?:ed)?\s*\)|"
    r"\bremaster(?:ed)?\b|"
    r"\bmono\b|\bstereo\b|"
    r"\blive\b|"
    r"\bspecial edition\b|\bdeluxe(?: edition)?\b|"
    r"\bbonus track\b|\bsingle version\b|\bradio edit\b|\balbum version\b"
    r")"
)
_paren_cleanup = re.compile(r"\(\s*\)")

def _unaccent_basic(s: str) -> str:
    # lightweight unaccent without extra deps
    # just strip most common accents via mapping; fall back to NFKD filter
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def normalize_title(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _unaccent_basic(s).lower().strip()
    s = _title_strip_pat.sub("", s)
    s = _paren_cleanup.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_album(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _unaccent_basic(s).lower().strip()
    # drop parenthetical edition/remaster notes
    s = re.sub(r"(?i)\s*\((?:deluxe|special|expanded|remaster(?:ed)?|anniversary).*?\)\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def text_sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(a=a, b=b).ratio()

def duration_score_ms(diff_ms: int) -> float:
    # Map diff to [0..1], tight diff gets full credit, taper afterwards
    d = abs(diff_ms)
    if d <= DUR_TIGHT_MS:
        return 1.0
    if d >= DUR_MAX_MS:
        return 0.0
    # linear drop-off between tight and max
    span = DUR_MAX_MS - DUR_TIGHT_MS
    return max(0.0, 1.0 - (d - DUR_TIGHT_MS) / float(span))


def build_query(title: str, artist: str, album: Optional[str]) -> str:
    # Be explicit to help Spotify search quality
    parts = [f'track:"{title}"', f'artist:"{artist}"']
    if album:
        parts.append(f'album:"{album}"')
    return " ".join(parts)


def safe_spotify_call(sp: Spotify, func, *args, **kwargs):
    """Retry wrapper for Spotipy calls."""
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            # rate limited
            if e.http_status == 429:
                wait_s = int(e.headers.get("Retry-After", "5"))
                retries += 1
                log_event("apple_spotify_match", f"429 rate limit. sleeping {wait_s}s (retry {retries})")
                time.sleep(wait_s)
                continue
            # auth gone bad - surface and stop (get_spotify_client should refresh)
            if e.http_status in (401, 403):
                log_event("apple_spotify_match", f"auth error {e.http_status}: {e}", level="error")
                raise
            log_event("apple_spotify_match", f"spotify error: {e}", level="error")
            raise
        except Exception as e:
            retries += 1
            sleep = min(5 * retries, 20)
            log_event("apple_spotify_match", f"net/other error: {e}. retry in {sleep}s", level="warning")
            time.sleep(sleep)


# -----------------------
# Core match logic
# -----------------------
def choose_best_match(a_title: str, a_album: str, a_dur: Optional[int], items: list) -> Optional[Tuple[Dict, float, float, int]]:
    """Return (best_item, title_sim, album_sim, dur_diff_ms) or None."""
    a_title_n = normalize_title(a_title)
    a_album_n = normalize_album(a_album)
    best = None
    best_score = -1.0
    best_title_sim = 0.0
    best_album_sim = 0.0
    best_dur_diff = 10**9

    for it in items:
        sp_title = it["name"]
        sp_album = it.get("album", {}).get("name") or ""
        sp_dur = it.get("duration_ms")
        sp_title_n = normalize_title(sp_title)
        sp_album_n = normalize_album(sp_album)

        t_sim = text_sim(a_title_n, sp_title_n)
        a_sim = text_sim(a_album_n, sp_album_n) if a_album_n and sp_album_n else 0.0
        d_diff = (sp_dur - a_dur) if (sp_dur is not None and a_dur is not None) else 10**9

        # hard gates
        if t_sim < TITLE_SIM_MIN:
            continue
        if a_dur is not None and sp_dur is not None and abs(d_diff) > DUR_MAX_MS:
            continue
        if a_album_n and sp_album_n and a_sim < ALBUM_SIM_MIN:
            # if duration is extremely tight we can still allow it
            if not (a_dur is not None and sp_dur is not None and abs(d_diff) <= DUR_TIGHT_MS):
                continue

        # blended score
        d_score = duration_score_ms(d_diff if isinstance(d_diff, int) else DUR_MAX_MS)
        score = W_TITLE * t_sim + W_ALBUM * a_sim + W_DUR * d_score

        if score > best_score:
            best_score = score
            best = it
            best_title_sim = t_sim
            best_album_sim = a_sim
            best_dur_diff = d_diff if isinstance(d_diff, int) else 10**9

    if best is None:
        return None
    return best, best_title_sim, best_album_sim, best_dur_diff


def fetch_batch(conn) -> list[Tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT apple_track_id, name, artist_name, album_name, duration_ms
            FROM apple_unique_track_ids
            WHERE spotify_track_id IS NULL
              AND (checked_at_spotify_back_fill IS NULL)
              AND name IS NOT NULL
              AND artist_name IS NOT NULL
            ORDER BY apple_track_id
            LIMIT %s
            """,
            (BATCH_SIZE,),
        )
        return cur.fetchall()


def update_rows(conn, rows_to_upsert: list[Tuple], rows_no_match: list[int]):
    # rows_to_upsert: tuples of columns to set on success
    if rows_to_upsert:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO apple_unique_track_ids (
                    apple_track_id,
                    spotify_track_id,
                    spotify_track_name,
                    spotify_artist_id,
                    spotify_duration_ms,
                    spotify_artist_name,
                    spotify_album_id,
                    spotify_album_name,
                    spotify_album_type,
                    spotify_checked_at,
                    checked_at_spotify_back_fill
                ) VALUES %s
                ON CONFLICT (apple_track_id) DO UPDATE SET
                    spotify_track_id      = EXCLUDED.spotify_track_id,
                    spotify_track_name    = EXCLUDED.spotify_track_name,
                    spotify_artist_id     = EXCLUDED.spotify_artist_id,
                    spotify_duration_ms   = EXCLUDED.spotify_duration_ms,
                    spotify_artist_name   = EXCLUDED.spotify_artist_name,
                    spotify_album_id      = EXCLUDED.spotify_album_id,
                    spotify_album_name    = EXCLUDED.spotify_album_name,
                    spotify_album_type    = EXCLUDED.spotify_album_type,
                    spotify_checked_at    = EXCLUDED.spotify_checked_at,
                    checked_at_spotify_back_fill = EXCLUDED.checked_at_spotify_back_fill
                """,
                rows_to_upsert,
                page_size=100,
            )
    if rows_no_match:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE apple_unique_track_ids AS a
                SET checked_at_spotify_back_fill = NOW(),
                    spotify_checked_at = NOW()
                FROM (VALUES %s) AS v(apple_track_id)
                WHERE a.apple_track_id = v.apple_track_id
                """,
                [(aid,) for aid in rows_no_match],
                page_size=200,
            )
    conn.commit()


def main():
    sp: Spotify = get_spotify_client()
    conn = get_db_connection()
    processed_total = 0
    matched_total = 0

    log_event("apple_spotify_match", "Starting backfill by fields (title/artist/album)")

    try:
        while True:
            batch = fetch_batch(conn)
            if not batch:
                break

            rows_to_upsert = []
            rows_no_match = []

            for apple_track_id, title, artist, album, dur_ms in batch:
                query = build_query(title, artist, album)
                result = safe_spotify_call(sp, sp.search, q=query, type="track", limit=10, market="US")
                items = result.get("tracks", {}).get("items", []) if result else []
                choice = choose_best_match(title, album or "", dur_ms, items)

                if choice:
                    item, t_sim, a_sim, d_diff = choice
                    track_id   = item["id"]
                    track_name = item["name"]
                    album_obj  = item.get("album") or {}
                    album_id   = album_obj.get("id")
                    album_name = album_obj.get("name")
                    album_type = album_obj.get("album_type")
                    duration   = item.get("duration_ms")
                    artists    = item.get("artists") or []
                    artist_id  = artists[0]["id"] if artists else None
                    artist_name= artists[0]["name"] if artists else None

                    rows_to_upsert.append(
                        (
                            apple_track_id,
                            track_id,
                            track_name,
                            artist_id,
                            duration,
                            artist_name,
                            album_id,
                            album_name,
                            album_type,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )
                    )
                    matched_total += 1
                else:
                    rows_no_match.append(apple_track_id)

                processed_total += 1

            update_rows(conn, rows_to_upsert, rows_no_match)
            log_event(
                "apple_spotify_match",
                f"Batch processed={len(batch)} matched={len(rows_to_upsert)} "
                f"no_match={len(rows_no_match)} totals processed={processed_total} matched={matched_total}"
            )

            if len(batch) < BATCH_SIZE:
                break

            time.sleep(PAUSE_BETWEEN_BATCHES)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    log_event("apple_spotify_match", f"Done. processed={processed_total} matched={matched_total}")


if __name__ == "__main__":
    main()
