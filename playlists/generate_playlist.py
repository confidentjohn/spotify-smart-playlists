import os
import json
from datetime import datetime
from spotipy import Spotify
import requests
from utils.logger import log_event
from routes.rule_parser import build_track_query
from playlists.playlist_sync import sync_playlist
from utils.spotify_auth import get_spotify_client
from utils.db_utils import get_db_connection

conn = get_db_connection()