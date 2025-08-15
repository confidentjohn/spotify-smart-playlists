# utils/rule_parser.py
import json
from utils.logger import log_event

def _normalize_track_source(v: object):
    val = str(v or "").strip().lower()
    if val in ("library", "non_library"):
        return val
    if val in ("", "both", "all", "any"):
        return None  # treat as no filter
    raise ValueError("Source must be 'library', 'non_library', or 'both'.")

def _source_clause(v: object, op: str) -> str:
    val = _normalize_track_source(v)
    return "" if val is None else f"track_source {op} '{val}'"

# Define a mapping of supported condition fields to SQL templates
CONDITION_MAP = {
    "min_plays": lambda v: f"play_count >= {int(v)}",
    "max_plays": lambda v: f"play_count <= {int(v)}",
    "added_after": lambda v: f"added_at >= '{v}'",
    "added_before": lambda v: f"added_at <= '{v}'",
    "plays": {
        "is": lambda v: f"play_count = {int(v)}",
        "eq": lambda v: f"play_count = {int(v)}",
        "gt": lambda v: f"play_count > {int(v)}",
        "lt": lambda v: f"play_count < {int(v)}",
        "gte": lambda v: f"play_count >= {int(v)}",
        "lte": lambda v: f"play_count <= {int(v)}",
        "is_not": lambda v: f"play_count != {int(v)}"
    },
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
    "source": {
        "eq": lambda v: _source_clause(v, "="),
        "is_not": lambda v: _source_clause(v, "<>")
    },
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

    def parse_condition_group(group):
        connector = " AND " if group.get("match", "all") == "all" else " OR "
        condition_strings = []

        for condition in group.get("conditions", []):
            if "conditions" in condition:
                # Nested group
                sub_clause = parse_condition_group(condition)
                condition_strings.append(f"({sub_clause})")
                continue

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
                if condition_sql and str(condition_sql).strip():
                    condition_strings.append(condition_sql)
                    log_event("rule_parser", f"✅ Parsed rule '{field}' -> {condition_sql}")
                else:
                    log_event("rule_parser", f"ℹ️ Skipping rule '{field}' because it produced no condition (likely 'both')")
            except Exception as e:
                log_event("rule_parser", f"❌ Error parsing rule '{field}': {e}", level="error")

        return connector.join(condition_strings)

    # Start parsing from top-level group
    where_clause = parse_condition_group(rules)

    if not where_clause or not where_clause.strip():
        where_clause = "1=1"

    # Always include these
    if "is_playable" not in [c.get("field") for c in rules.get("conditions", [])]:
        where_clause += " AND is_playable = TRUE"
    where_clause += " AND excluded = FALSE"

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