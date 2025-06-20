# utils/rule_parser.py
import json
from utils.logger import log_event

# Define a mapping of supported condition fields to SQL templates
CONDITION_MAP = {
    "min_plays": lambda v: f"play_count >= {int(v)}",
    "max_plays": lambda v: f"play_count <= {int(v)}",
    "added_after": lambda v: f"added_at >= '{v}'",
    "added_before": lambda v: f"added_at <= '{v}'",
    "plays": lambda v: f"play_count = {int(v)}",
    "is_liked": lambda v: f"is_liked = {str(v).upper()}",
    "artist": lambda v: f"LOWER(artist) LIKE LOWER('%{v}%')",
    "is_playable": lambda v: f"is_playable = {str(v).upper()}",
    "added_in_last_days": lambda v: f"added_at >= NOW() - INTERVAL '{int(v)} days'",
    "date_added": {
        "gt": lambda v: f"added_at > '{v}'",
        "lt": lambda v: f"added_at < '{v}'",
        "gte": lambda v: f"added_at >= '{v}'",
        "lte": lambda v: f"added_at <= '{v}'",
        "eq": lambda v: f"added_at = '{v}'"
    },
    "album": lambda v: f"LOWER(album_name) LIKE LOWER('%{v}%')",
    "track": lambda v: f"LOWER(track_name) LIKE LOWER('%{v}%')",
    "last_played": lambda v: f"last_played_at >= '{v}'",
    "first_played": lambda v: f"first_played_at >= '{v}'",
}

def build_track_query(rules_json):
    try:
        if isinstance(rules_json, dict):
            rules = rules_json
        elif isinstance(rules_json, str):
            rules = json.loads(rules_json)
        else:
            rules = json.loads(json.dumps(rules_json))
        log_event("rule_parser", f"🔍 Raw input to parse: {rules_json}")
        log_event("rule_parser", f"📥 Loaded rules: {rules}")
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON for rules")

    base_conditions = []

    # Build conditions from rules["conditions"]
    for condition in rules.get("conditions", []):
        field = condition.get("field")
        value = condition.get("value")
        operator = condition.get("operator")

        if not field or field not in CONDITION_MAP:
            log_event("rule_parser", f"⚠️ Unsupported or missing field in rule: {condition}", level="error")
            continue

        try:
            map_entry = CONDITION_MAP[field]
            if isinstance(map_entry, dict):
                if operator not in map_entry:
                    raise ValueError(f"Unsupported operator '{operator}' for field '{field}'")
                condition_sql = map_entry[operator](value)
            else:
                condition_sql = map_entry(value)
            base_conditions.append(condition_sql)
            log_event("rule_parser", f"✅ Parsed rule '{field}' -> {condition_sql}")
        except Exception as e:
            log_event("rule_parser", f"❌ Error parsing rule '{field}': {e}", level="error")

    # Always include playable tracks unless user specifies otherwise
    if not any(c.get("field") == "is_playable" for c in rules.get("conditions", [])):
        base_conditions.append("is_playable = TRUE")
    base_conditions.append("excluded = FALSE")

    match_type = rules.get("match", "all")
    connector = " AND " if match_type == "all" else " OR "
    where_clause = connector.join(base_conditions) if base_conditions else "1=1"

    # Sort clause
    sort_clause = "ORDER BY play_count DESC"
    if isinstance(rules.get("sort"), list):
        sort_fields = []
        sort_field_map = {
            "album": "album_name",
            "artist": "artist",
            "added": "added_at",
            "plays": "play_count",
            "last_played": "last_played_at",
            "album_id": "album_id",
            "disc_number": "disc_number",
            "track_number": "track_number"
        }
        for sort_rule in rules["sort"]:
            sort_by = sort_rule.get("by", "play_count")
            direction = sort_rule.get("direction", "desc").upper()
            mapped_sort_by = sort_field_map.get(sort_by, sort_by)
            if direction in ("ASC", "DESC"):
                sort_fields.append(f"{mapped_sort_by} {direction}")
        if sort_fields:
            sort_clause = "ORDER BY " + ", ".join(sort_fields)

    limit = rules.get("limit", 100)
    query = f"SELECT 'spotify:track:' || track_id FROM unified_tracks WHERE {where_clause} {sort_clause} LIMIT {int(limit)}"

    log_event("rule_parser", f"🛠 Built SQL: {query}")
    return query