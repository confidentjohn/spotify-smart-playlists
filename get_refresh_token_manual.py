# get_refresh_token_manual.py
import requests
import urllib.parse

CLIENT_ID = "78f7fd3278b74f75a524263c3ec995b6"
CLIENT_SECRET = "86dd108302ae48dc99180bc68c3fc9e8"
REDIRECT_URI = "https://spotify-oauth-tracker.onrender.com/callback"

SCOPE = "user-read-recently-played user-library-read playlist-modify-private playlist-modify-public"

# STEP 1: Direct user to Spotify auth page
params = {
    "client_id": CLIENT_ID,
    "response_type": "code",
    "redirect_uri": REDIRECT_URI,
    "scope": SCOPE,
}
auth_url = f"https://accounts.spotify.com/authorize?{urllib.parse.urlencode(params)}"
print("🔗 Go to this URL:\n", auth_url)

# STEP 2: Paste full redirect URL after approving
redirect_response = input("\nPaste the full redirect URL after approval:\n").strip()

# STEP 3: Extract the 'code' from the URL
parsed = urllib.parse.urlparse(redirect_response)
code = urllib.parse.parse_qs(parsed.query).get("code")
if not code:
    print("❌ Failed to extract authorization code.")
    exit(1)

code = code[0]

# STEP 4: Exchange code for tokens
response = requests.post("https://accounts.spotify.com/api/token", data={
    "grant_type": "authorization_code",
    "code": code,
    "redirect_uri": REDIRECT_URI,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
})

if response.status_code != 200:
    print("❌ Token request failed:")
    print(response.text)
else:
    token_data = response.json()
    print("✅ Success! Copy this refresh token:\n")
    print(token_data["refresh_token"])