# utils/rule_parser.py
import json
from utils.logger import log_event

# Define a mapping of supported condition fields to SQL templates
CONDITION_MAP = {
    "min_plays": lambda v: f"play_count >= {int(v)}",
    "max_plays": lambda v: f"play_count <= {int(v)}",
    "added_after": lambda v: f"added_at >= '{v}'",
    "added_before": lambda v: f"added_at <= '{v}'",
    "not_played": lambda v: "play_count = 0",
    "played_times": lambda v: f"play_count = {int(v)}",
    "is_liked": lambda v: f"is_liked = {str(v).upper()}",
    "artist": lambda v: f"LOWER(artist_name) LIKE LOWER('%{v}%')",
    "is_playable": lambda v: f"is_playable = {str(v).upper()}",
    "library_source": lambda v: (
        f"library_source IN ({', '.join([repr(item) for item in v])})"
        if isinstance(v, list) else f"library_source = '{v}'"
    ),
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
            condition_sql = CONDITION_MAP[field](value)
            base_conditions.append(condition_sql)
            log_event("rule_parser", f"✅ Parsed rule '{field}' -> {condition_sql}")
        except Exception as e:
            log_event("rule_parser", f"❌ Error parsing rule '{field}': {e}", level="error")

    # Always include available tracks
    base_conditions.append("is_available = TRUE")

    match_type = rules.get("match", "all")
    connector = " AND " if match_type == "all" else " OR "
    where_clause = connector.join(base_conditions) if base_conditions else "1=1"

    # Sort clause
    sort_clause = "ORDER BY play_count DESC"
    if "sort" in rules:
        sort_by = rules["sort"].get("by", "play_count")
        direction = rules["sort"].get("direction", "desc").upper()
        if sort_by and direction in ("ASC", "DESC"):
            sort_clause = f"ORDER BY {sort_by} {direction}"

    limit = rules.get("limit", 100)
    query = f"SELECT id FROM tracks WHERE {where_clause} {sort_clause} LIMIT {int(limit)}"

    log_event("rule_parser", f"🛠 Built SQL: {query}")
    return query