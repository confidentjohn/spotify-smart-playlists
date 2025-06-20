# utils/rule_parser.py
import json

# Define a basic mapping of field names to database column names with lambdas for flexible parsing
FIELD_MAP = {
    "min_plays": lambda v: f"play_count >= {int(v)}",
    "max_plays": lambda v: f"play_count <= {int(v)}",
    "added_after": lambda v: f"added_at >= '{v}'",
    "added_before": lambda v: f"added_at <= '{v}'",
    "not_played": lambda v: "play_count = 0",
    "played_times": lambda v: f"play_count = {int(v)}",
    "is_liked": lambda v: "id IN (SELECT track_id FROM liked_tracks)",
    "artist": lambda v: f"artist_name = '{v}'",
    "not_in": lambda v: "id NOT IN (SELECT track_id FROM exclusions)" if v == "exclusions" else ""
}

def build_track_query(rules_json):
    try:
        rules = json.loads(rules_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON for rules")

    conditions = []

    for key, value in rules.items():
        parser = FIELD_MAP.get(key)
        if parser:
            try:
                conditions.append(parser(value))
            except Exception:
                raise ValueError(f"Error parsing rule: {key}")

    conditions.append("is_available = TRUE")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT id FROM tracks WHERE {where_clause} ORDER BY play_count DESC LIMIT 100"
    return query