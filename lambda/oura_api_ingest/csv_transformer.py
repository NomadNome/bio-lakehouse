"""
Transform Oura API v2 JSON responses into comma-delimited CSV strings.

Output format matches what split_oura_data.py produces, which is what the
downstream ingestion trigger Lambda and Glue ETL jobs expect.
"""

import csv
import io


READINESS_COLUMNS = [
    "id", "day", "score", "temperature_deviation",
    "temperature_trend_deviation", "timestamp",
    "contributors_activity_balance", "contributors_body_temperature",
    "contributors_hrv_balance", "contributors_previous_day_activity",
    "contributors_previous_night", "contributors_recovery_index",
    "contributors_resting_heart_rate", "contributors_sleep_balance",
    "contributors_sleep_regularity",
]

SLEEP_COLUMNS = [
    "id", "day", "score", "timestamp",
    "contributors_deep_sleep", "contributors_efficiency",
    "contributors_latency", "contributors_rem_sleep",
    "contributors_restfulness", "contributors_timing",
    "contributors_total_sleep",
]

ACTIVITY_COLUMNS = [
    "id", "day", "score", "timestamp",
    "active_calories", "steps",
    "high_activity_time", "medium_activity_time",
    "low_activity_time", "sedentary_time", "total_calories",
    "met_interval", "met_avg", "met_max", "met_count",
]

COLUMNS = {
    "readiness": READINESS_COLUMNS,
    "sleep": SLEEP_COLUMNS,
    "activity": ACTIVITY_COLUMNS,
}


def _flatten_contributors(record):
    """Flatten nested contributors dict into prefixed keys."""
    contributors = record.get("contributors") or {}
    flat = {}
    for key, value in contributors.items():
        flat[f"contributors_{key}"] = value if value is not None else ""
    return flat


def _compute_met_stats(record):
    """Compute MET summary stats from the met.items array."""
    met = record.get("met") or {}
    items = met.get("items") if isinstance(met, dict) else None

    if items and len(items) > 0:
        return {
            "met_interval": met.get("interval", ""),
            "met_avg": round(sum(items) / len(items), 2),
            "met_max": max(items),
            "met_count": len(items),
        }
    return {
        "met_interval": "",
        "met_avg": "",
        "met_max": "",
        "met_count": "",
    }


def transform_record(record, data_type):
    """Transform a single API record into a flat dict matching CSV columns."""
    columns = COLUMNS[data_type]
    flat = {}

    # Copy top-level scalar fields
    for col in columns:
        if col in record:
            val = record[col]
            flat[col] = val if val is not None else ""

    # Flatten contributors
    if "contributors" in (record or {}):
        flat.update(_flatten_contributors(record))

    # Compute MET stats for activity
    if data_type == "activity":
        flat.update(_compute_met_stats(record))

    # Build final row in column order, filling missing with empty string
    return {col: flat.get(col, "") for col in columns}


def records_to_csv(records, data_type):
    """Convert a list of API records to a CSV string.

    Args:
        records: List of dicts from Oura API
        data_type: One of 'readiness', 'sleep', 'activity'

    Returns:
        CSV string with headers and data rows (comma-delimited)
    """
    columns = COLUMNS[data_type]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()

    for record in records:
        writer.writerow(transform_record(record, data_type))

    return output.getvalue()
