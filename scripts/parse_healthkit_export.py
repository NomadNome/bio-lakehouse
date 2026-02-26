"""
Bio Lakehouse - Apple HealthKit Export Parser

Streaming XML parser for Apple Health export.xml files.
Produces 4 CSV files (daily_vitals, workouts, body, mindfulness)
in Hive-partitioned layout for Bronze S3 ingestion.

Uses lxml for 5-10x faster parsing of large (2GB+) XML files.
Supports --since flag to skip old records and only parse recent data.

IMPORTANT - First Run vs Incremental:
    The FIRST time you parse a new HealthKit export, run WITHOUT --since
    to establish the full baseline dataset:

        python3 scripts/parse_healthkit_export.py --input ~/Downloads/export.xml

    This full parse takes ~15 seconds for a 2GB+ file (with lxml installed).

    For SUBSEQUENT runs (e.g. weekly re-exports), use --since to only parse
    records newer than your last successful run:

        python3 scripts/parse_healthkit_export.py --input ~/Downloads/export.xml --since 2026-02-24

    The --since flag filters on the record's startDate attribute, skipping
    any record older than the given date. This dramatically reduces parse
    time for incremental updates.

Usage:
    python3 scripts/parse_healthkit_export.py [--input PATH] [--output-dir PATH] [--since DATE]

Defaults:
    --input:      data/apple_health_export/export.xml
    --output-dir: bronze_staged/healthkit
"""

import argparse
import csv
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    from lxml import etree as ET
    USING_LXML = True
except ImportError:
    import xml.etree.ElementTree as ET
    USING_LXML = False

# -------------------------------------------------------
# Constants
# -------------------------------------------------------

VITAL_TYPES = {
    "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate_bpm",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrv_ms",
    "HKQuantityTypeIdentifierVO2Max": "vo2_max",
    "HKQuantityTypeIdentifierOxygenSaturation": "blood_oxygen_pct",
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratory_rate",
}

BODY_TYPES = {
    "HKQuantityTypeIdentifierBodyMass": "weight",
    "HKQuantityTypeIdentifierBodyFatPercentage": "body_fat_pct",
    "HKQuantityTypeIdentifierBodyMassIndex": "bmi",
    "HKQuantityTypeIdentifierLeanBodyMass": "lean_body_mass",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "bmr",  # Basal Metabolic Rate
    # Additional body composition metrics (if Hume pod writes these)
    "HKQuantityTypeIdentifierAppleStandingHeight": "height",
}

# Aggregation: "last" picks the last value of the day, "mean" averages all values
VITAL_AGGREGATION = {
    "resting_heart_rate_bpm": "last",
    "hrv_ms": "last",
    "vo2_max": "last",
    "blood_oxygen_pct": "mean",
    "respiratory_rate": "mean",
}

DAILY_VITALS_HEADERS = [
    "date", "resting_heart_rate_bpm", "hrv_ms", "vo2_max",
    "blood_oxygen_pct", "respiratory_rate",
]

WORKOUTS_HEADERS = [
    "date", "start_time", "end_time", "workout_type", "duration_minutes",
    "calories_burned", "avg_heart_rate", "distance_mi", "source_app",
]

BODY_HEADERS = ["date", "weight_lbs", "body_fat_pct", "bmi", "lean_body_mass_lbs", "bmr", "height_in", "device_name"]

MINDFULNESS_HEADERS = ["date", "duration_minutes", "session_count"]

KG_TO_LBS = 2.20462
KM_TO_MI = 0.621371


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def parse_date(date_str):
    """Extract YYYY-MM-DD from Apple Health date string like '2025-11-25 08:30:00 -0500'."""
    if not date_str:
        return None
    return date_str[:10]


def parse_datetime_iso(date_str):
    """Convert Apple Health date to ISO 8601."""
    if not date_str:
        return None
    # Input: "2025-11-25 08:30:00 -0500"
    # Output: "2025-11-25T08:30:00-05:00"
    parts = date_str.strip().split(" ")
    if len(parts) >= 2:
        date_part = parts[0]
        time_part = parts[1]
        offset = parts[2] if len(parts) > 2 else "+0000"
        # Format offset as -05:00
        offset_formatted = offset[:3] + ":" + offset[3:] if len(offset) >= 5 else offset
        return f"{date_part}T{time_part}{offset_formatted}"
    return date_str


def normalize_workout_type(hk_type):
    """Convert HKWorkoutActivityTypeHiking → hiking, HKWorkoutActivityTypeFunctionalStrengthTraining → functional_strength_training."""
    if not hk_type:
        return "unknown"
    # Strip prefix
    name = hk_type.replace("HKWorkoutActivityType", "")
    # CamelCase → snake_case
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()
    return snake


def safe_float(val):
    """Convert to float or return None."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """Convert to int or return None."""
    f = safe_float(val)
    if f is None:
        return None
    return int(round(f))


# -------------------------------------------------------
# Accumulators
# -------------------------------------------------------

class HealthKitAccumulator:
    """Collects records during XML streaming and aggregates to daily CSVs."""

    def __init__(self):
        # vitals: {date: {metric: [values]}}
        self.vitals = defaultdict(lambda: defaultdict(list))
        # workouts: list of dicts
        self.workouts = []
        # body: {date: {metric: value}} — last-of-day wins
        self.body = defaultdict(dict)
        # mindfulness: {date: {"duration": float, "count": int}}
        self.mindfulness = defaultdict(lambda: {"duration": 0.0, "count": 0})

    def add_vital(self, record_type, date, value):
        metric_name = VITAL_TYPES.get(record_type)
        if metric_name and date and value is not None:
            self.vitals[date][metric_name].append(value)

    def add_workout(self, workout_dict):
        self.workouts.append(workout_dict)

    def add_body(self, record_type, date, value, unit, source_name=""):
        if not date or value is None:
            return
        field = BODY_TYPES.get(record_type)
        if not field:
            return
        self.body[date][field] = (value, unit)
        # Only store device_name for body composition measurements, not BMR
        # BMR comes from Apple Watch continuously and shouldn't overwrite body comp source
        if source_name and field != "bmr":
            self.body[date]["_device_name"] = source_name

    def add_mindfulness(self, date, duration_minutes):
        if date and duration_minutes is not None:
            self.mindfulness[date]["duration"] += duration_minutes
            self.mindfulness[date]["count"] += 1

    def aggregate_vitals(self):
        """Return list of daily vital rows."""
        rows = []
        for date in sorted(self.vitals.keys()):
            row = {"date": date}
            for metric in DAILY_VITALS_HEADERS[1:]:
                values = self.vitals[date].get(metric, [])
                if not values:
                    row[metric] = ""
                    continue
                agg = VITAL_AGGREGATION.get(metric, "last")
                if agg == "last":
                    val = values[-1]
                else:
                    val = sum(values) / len(values)
                # SpO2: convert 0.975 → 97.5
                if metric == "blood_oxygen_pct" and val <= 1.0:
                    val = round(val * 100, 1)
                else:
                    val = round(val, 2)
                row[metric] = val
            rows.append(row)
        return rows

    def aggregate_body(self):
        """Return list of daily body rows."""
        rows = []
        for date in sorted(self.body.keys()):
            data = self.body[date]
            row = {"date": date}

            # Weight
            if "weight" in data:
                val, unit = data["weight"]
                row["weight_lbs"] = round(val * KG_TO_LBS, 1) if unit == "kg" else round(val, 1)
            else:
                row["weight_lbs"] = ""

            # Body fat
            if "body_fat_pct" in data:
                val, _ = data["body_fat_pct"]
                row["body_fat_pct"] = round(val * 100, 1) if val <= 1.0 else round(val, 1)
            else:
                row["body_fat_pct"] = ""

            # BMI
            if "bmi" in data:
                val, _ = data["bmi"]
                row["bmi"] = round(val, 1)
            else:
                row["bmi"] = ""

            # Lean body mass
            if "lean_body_mass" in data:
                val, unit = data["lean_body_mass"]
                row["lean_body_mass_lbs"] = round(val * KG_TO_LBS, 1) if unit == "kg" else round(val, 1)
            else:
                row["lean_body_mass_lbs"] = ""

            # BMR (Basal Metabolic Rate)
            if "bmr" in data:
                val, unit = data["bmr"]
                # BMR is typically in kcal/day
                row["bmr"] = round(val, 0)
            else:
                row["bmr"] = ""

            # Height
            if "height" in data:
                val, unit = data["height"]
                # Convert cm to inches (divide by 2.54) or m to inches (multiply by 39.3701)
                if unit == "cm":
                    row["height_in"] = round(val / 2.54, 1)
                elif unit == "m":
                    row["height_in"] = round(val * 39.3701, 1)
                elif unit == "in":
                    row["height_in"] = round(val, 1)
                else:
                    row["height_in"] = round(val / 2.54, 1)  # assume cm
            else:
                row["height_in"] = ""

            # Device name (last-of-day wins, same as other body fields)
            row["device_name"] = data.get("_device_name", "")

            rows.append(row)
        return rows

    def aggregate_mindfulness(self):
        """Return list of daily mindfulness rows."""
        rows = []
        for date in sorted(self.mindfulness.keys()):
            data = self.mindfulness[date]
            rows.append({
                "date": date,
                "duration_minutes": round(data["duration"], 1),
                "session_count": data["count"],
            })
        return rows


# -------------------------------------------------------
# XML Streaming Parser
# -------------------------------------------------------


def parse_export_with_mindfulness(input_path, since_date=None):
    """Stream-parse with mindfulness support. Uses lxml if available for 5-10x speed.

    Args:
        input_path: Path to export.xml
        since_date: Optional YYYY-MM-DD string. Skip records older than this date.
    """
    acc = HealthKitAccumulator()
    parser_name = "lxml" if USING_LXML else "stdlib ElementTree"
    print(f"Parsing {input_path} with {parser_name}...")
    if since_date:
        print(f"  Filtering: only records on or after {since_date}")

    record_count = 0
    skipped_count = 0
    t0 = time.time()

    # Track types we care about for fast filtering
    relevant_record_types = set(VITAL_TYPES) | set(BODY_TYPES) | {"HKCategoryTypeIdentifierMindfulSession"}

    context = ET.iterparse(input_path, events=("end",))

    for event, elem in context:
        tag = elem.tag if isinstance(elem.tag, str) else elem.tag

        if tag == "Record":
            record_count += 1
            start_date = elem.get("startDate", "")
            date = start_date[:10] if start_date else None

            # Skip old records
            if since_date and date and date < since_date:
                skipped_count += 1
                elem.clear()
                continue

            record_type = elem.get("type", "")

            # Skip irrelevant record types early (biggest speed win)
            if record_type not in relevant_record_types:
                elem.clear()
                continue

            value = safe_float(elem.get("value"))
            unit = elem.get("unit", "")

            # Vitals
            if record_type in VITAL_TYPES and value is not None:
                acc.add_vital(record_type, date, value)

            # Body measurements
            elif record_type in BODY_TYPES and value is not None:
                acc.add_body(record_type, date, value, unit, elem.get("sourceName", ""))

            # Mindfulness sessions
            elif record_type == "HKCategoryTypeIdentifierMindfulSession":
                end_date = elem.get("endDate", "")
                if start_date and end_date:
                    try:
                        start_dt = datetime.strptime(start_date[:19], "%Y-%m-%d %H:%M:%S")
                        end_dt = datetime.strptime(end_date[:19], "%Y-%m-%d %H:%M:%S")
                        duration_min = (end_dt - start_dt).total_seconds() / 60.0
                        acc.add_mindfulness(date, duration_min)
                    except ValueError:
                        pass

            elem.clear()

            # Progress indicator every 500k records
            if record_count % 500_000 == 0:
                elapsed = time.time() - t0
                print(f"  {record_count:,} records processed ({elapsed:.0f}s)...")

        elif tag in ("WorkoutStatistics", "MetadataEntry", "WorkoutEvent", "WorkoutRoute"):
            # Don't clear — children of Workout, needed for findall()
            pass

        elif tag == "Workout":
            record_count += 1
            start_date = elem.get("startDate", "")
            date = parse_date(start_date)

            if since_date and date and date < since_date:
                skipped_count += 1
                elem.clear()
                continue

            source_name = elem.get("sourceName", "")

            # Filter out Peloton workouts
            if source_name and "peloton" in source_name.lower():
                elem.clear()
                continue

            workout_type_raw = elem.get("workoutActivityType", "")
            end_date = elem.get("endDate", "")
            duration = safe_float(elem.get("duration"))
            calories = safe_float(elem.get("totalEnergyBurned"))
            distance_val = safe_float(elem.get("totalDistance"))
            distance_unit = elem.get("totalDistanceUnit", "")

            workout_type = normalize_workout_type(workout_type_raw)

            # Convert distance to miles
            distance_mi = None
            if distance_val is not None:
                if distance_unit == "km":
                    distance_mi = round(distance_val * KM_TO_MI, 2)
                elif distance_unit == "mi":
                    distance_mi = round(distance_val, 2)
                else:
                    distance_mi = round(distance_val * KM_TO_MI, 2)

            # Extract avg HR and calories from WorkoutStatistics sub-elements
            avg_hr = None
            stats_calories = None
            for stat in elem.findall(".//WorkoutStatistics"):
                stat_type = stat.get("type")
                if stat_type == "HKQuantityTypeIdentifierHeartRate":
                    avg_hr = safe_int(stat.get("average"))
                elif stat_type == "HKQuantityTypeIdentifierActiveEnergyBurned":
                    stats_calories = safe_float(stat.get("sum"))

            # Prefer top-level totalEnergyBurned, fall back to WorkoutStatistics
            final_calories = calories if calories else stats_calories

            acc.add_workout({
                "date": date or "",
                "start_time": parse_datetime_iso(start_date) or "",
                "end_time": parse_datetime_iso(end_date) or "",
                "workout_type": workout_type,
                "duration_minutes": round(duration, 1) if duration else "",
                "calories_burned": safe_int(final_calories) if final_calories else "",
                "avg_heart_rate": avg_hr if avg_hr else "",
                "distance_mi": distance_mi if distance_mi else "",
                "source_app": source_name,
            })

            elem.clear()
        else:
            elem.clear()

    elapsed = time.time() - t0
    print(f"Parsed {record_count:,} records in {elapsed:.1f}s ({skipped_count:,} skipped by date filter)")
    return acc


# -------------------------------------------------------
# CSV Writers
# -------------------------------------------------------

def write_partitioned_csv(rows, headers, output_dir, data_type):
    """Write rows to Hive-partitioned CSV files: {output_dir}/{data_type}/year=YYYY/month=MM/day=DD/{data_type}.csv"""
    if not rows:
        print(f"No {data_type} data to write")
        return 0

    # Group by date for partitioning
    by_date = defaultdict(list)
    for row in rows:
        date = row.get("date", "")
        if date and len(date) >= 10:
            by_date[date].append(row)
        else:
            by_date["unknown"].append(row)

    total = 0
    for date, date_rows in by_date.items():
        if date == "unknown":
            continue
        year = date[:4]
        month = date[5:7]
        day = date[8:10]
        part_dir = Path(output_dir) / data_type / f"year={year}" / f"month={month}" / f"day={day}"
        part_dir.mkdir(parents=True, exist_ok=True)

        csv_path = part_dir / f"{data_type}.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(date_rows)
        total += len(date_rows)

    print(f"Wrote {total} {data_type} records across {len(by_date)} days")
    return total


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Parse Apple Health export.xml to Bronze CSVs")
    parser.add_argument("--input", default="data/apple_health_export/export.xml",
                        help="Path to export.xml")
    parser.add_argument("--output-dir", default="bronze_staged/healthkit",
                        help="Output directory for partitioned CSVs")
    parser.add_argument("--since", default=None,
                        help="Only parse records on or after this date (YYYY-MM-DD). "
                             "Dramatically faster for incremental updates.")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found")
        print("Place your Apple Health export.xml at the expected path or use --input")
        return 1

    acc = parse_export_with_mindfulness(args.input, since_date=args.since)

    # Write CSVs
    vitals_rows = acc.aggregate_vitals()
    write_partitioned_csv(vitals_rows, DAILY_VITALS_HEADERS, args.output_dir, "daily_vitals")

    write_partitioned_csv(acc.workouts, WORKOUTS_HEADERS, args.output_dir, "workouts")

    body_rows = acc.aggregate_body()
    write_partitioned_csv(body_rows, BODY_HEADERS, args.output_dir, "body")

    mindfulness_rows = acc.aggregate_mindfulness()
    write_partitioned_csv(mindfulness_rows, MINDFULNESS_HEADERS, args.output_dir, "mindfulness")

    print("HealthKit export parsing complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
