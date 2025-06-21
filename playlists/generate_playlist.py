import os
import psycopg2
import json
from datetime import datetime
from spotipy import Spotify
import requests
from utils.logger import log_event
from routes.rule_parser import build_track_query
from playlists.playlist_sync import sync_playlist
from utils.spotify_auth import get_spotify_client