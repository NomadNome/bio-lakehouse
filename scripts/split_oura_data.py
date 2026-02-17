#!/usr/bin/env python3
"""
Split Oura Ring CSV exports into date-partitioned files for Bronze layer ingestion.

Oura CSVs use ';' delimiter and contain JSON-embedded columns (contributors, MET data).
This script:
  1. Reads each Oura CSV (dailyreadiness, dailysleep, dailyactivity, workout)
  2. Parses JSON-embedded columns into flat columns
  3. Splits rows by date into partitioned directory structure
  4. Writes normalized CSVs (comma-delimited) for each date partition
"""

import csv
import json
import os
import sys
from pathlib import Path

# Source and output directories
OURA_DATA_DIR = Path(__file__).parent.parent / "data" / "App Data"
OUTPUT_DIR = Path(__file__).parent.parent / "bronze_staged"

# Oura files to process and their JSON columns
OURA_FILES = {
    "dailyreadiness": {
        "file": "dailyreadiness.csv",
        "json_cols": ["contributors"],
        "date_col": "day",
        "s3_prefix": "oura/readiness",
    },
    "dailysleep": {
        "file": "dailysleep.csv",
        "json_cols": ["contributors"],
        "date_col": "day",
        "s3_prefix": "oura/sleep",
    },
    "dailyactivity": {
        "file": "dailyactivity.csv",
        "json_cols": ["contributors", "met"],
        "date_col": "day",
        "s3_prefix": "oura/activity",
    },
    "workout": {
        "file": "workout.csv",
        "json_cols": [],
        "date_col": "day",
        "s3_prefix": "oura/workout",
    },
}


def parse_json_column(value: str) -> dict:
    """Parse a JSON string from an Oura CSV column. Returns empty dict on failure."""
    if not value or value.strip() == "":
        return {}
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}


def flatten_row(row: dict, json_cols: list[str]) -> dict:
    """Flatten JSON-embedded columns into individual prefixed columns."""
    flat = {}
    for key, value in row.items():
        if key in json_cols:
            parsed = parse_json_column(value)
            if key == "met":
                # MET data is large (interval + items array); store summary stats
                if isinstance(parsed, dict) and "items" in parsed:
                    items = parsed["items"]
                    flat[f"{key}_interval"] = parsed.get("interval", "")
                    flat[f"{key}_avg"] = round(sum(items) / len(items), 2) if items else ""
                    flat[f"{key}_max"] = max(items) if items else ""
                    flat[f"{key}_count"] = len(items)
                else:
                    flat[f"{key}_interval"] = ""
                    flat[f"{key}_avg"] = ""
                    flat[f"{key}_max"] = ""
                    flat[f"{key}_count"] = ""
            else:
                # Flatten dict keys as prefix_key
                for sub_key, sub_value in parsed.items():
                    flat[f"{key}_{sub_key}"] = sub_value if sub_value is not None else ""
        else:
            flat[key] = value
    return flat


def process_oura_file(dataset_name: str, config: dict) -> int:
    """Process a single Oura CSV file and split into date partitions."""
    source_file = OURA_DATA_DIR / config["file"]
    if not source_file.exists():
        print(f"  SKIP: {source_file} not found")
        return 0

    # Read all rows with semicolon delimiter
    with open(source_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    if not rows:
        print(f"  SKIP: {dataset_name} has no data rows")
        return 0

    # Flatten JSON columns
    flat_rows = [flatten_row(row, config["json_cols"]) for row in rows]

    # Get all unique column names (order matters for consistency)
    all_cols = list(dict.fromkeys(col for row in flat_rows for col in row.keys()))

    # Group by date
    date_col = config["date_col"]
    by_date: dict[str, list[dict]] = {}
    for row in flat_rows:
        date_str = row.get(date_col, "unknown")
        by_date.setdefault(date_str, []).append(row)

    # Write partitioned files
    total_written = 0
    for date_str, date_rows in sorted(by_date.items()):
        if date_str == "unknown" or len(date_str) < 10:
            print(f"  WARN: Skipping rows with invalid date: {date_str}")
            continue

        year, month, day = date_str[:4], date_str[5:7], date_str[8:10]
        out_dir = OUTPUT_DIR / config["s3_prefix"] / f"year={year}" / f"month={month}" / f"day={day}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"{dataset_name}.csv"
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(date_rows)
            total_written += len(date_rows)

    return total_written


def main():
    print(f"Oura Data Splitter")
    print(f"Source: {OURA_DATA_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print()

    if not OURA_DATA_DIR.exists():
        print(f"ERROR: Source directory not found: {OURA_DATA_DIR}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    for name, config in OURA_FILES.items():
        print(f"Processing {name}...")
        count = process_oura_file(name, config)
        print(f"  Wrote {count} records across date partitions")
        total += count

    print(f"\nDone. Total records processed: {total}")


if __name__ == "__main__":
    main()
