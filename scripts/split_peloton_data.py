#!/usr/bin/env python3
"""
Split Peloton workout CSV into date-partitioned files for Bronze layer ingestion.

Peloton CSV uses ',' delimiter with timezone-aware timestamps like "2021-05-29 09:27 (-04)".
This script:
  1. Reads the Peloton CSV
  2. Parses timezone-aware timestamps
  3. Splits rows by workout date into partitioned directory structure
  4. Writes normalized CSVs for each date partition
"""

import csv
import re
import sys
from pathlib import Path

# Source and output
PELOTON_CSV = Path(__file__).parent.parent / "KnownasNoma_workouts.csv"
OUTPUT_DIR = Path(__file__).parent.parent / "bronze_staged"

# Timestamp patterns:
#   Numeric offset: "2021-05-29 09:27 (-04)"
#   TZ abbreviation: "2022-01-21 17:22 (EST)"
TS_NUMERIC = re.compile(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})\s+\(([+-]\d{2})\)")
TS_ABBREV = re.compile(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})\s+\(([A-Z]{2,5})\)")

# Common US timezone abbreviations to UTC offset
TZ_OFFSETS = {
    "EST": "-05", "EDT": "-04",
    "CST": "-06", "CDT": "-05",
    "MST": "-07", "MDT": "-06",
    "PST": "-08", "PDT": "-07",
    "UTC": "+00", "GMT": "+00",
}


def parse_peloton_timestamp(ts_str: str) -> tuple[str, str, str]:
    """Parse Peloton timestamp into (date, time, utc_offset).

    Handles both numeric offsets (-04) and abbreviations (EST).

    Returns:
        (date_str, time_str, offset_str) e.g. ("2021-05-29", "09:27", "-04")
    """
    if not ts_str:
        return ("unknown", "", "")
    ts_str = ts_str.strip()

    match = TS_NUMERIC.match(ts_str)
    if match:
        return match.group(1), match.group(2), match.group(3)

    match = TS_ABBREV.match(ts_str)
    if match:
        tz_abbr = match.group(3)
        offset = TZ_OFFSETS.get(tz_abbr, "")
        return match.group(1), match.group(2), offset

    return ("unknown", "", "")


def normalize_column_name(name: str) -> str:
    """Normalize column names to snake_case for consistency."""
    name = name.strip().lower()
    name = re.sub(r"[.\s/()]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name


def main():
    print(f"Peloton Data Splitter")
    print(f"Source: {PELOTON_CSV}")
    print(f"Output: {OUTPUT_DIR}")
    print()

    if not PELOTON_CSV.exists():
        print(f"ERROR: Source file not found: {PELOTON_CSV}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Read all rows
    with open(PELOTON_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    if not raw_rows:
        print("ERROR: No data rows found")
        sys.exit(1)

    print(f"Read {len(raw_rows)} workout records")

    # Normalize column names and parse timestamps
    rows = []
    for raw_row in raw_rows:
        row = {normalize_column_name(k): v for k, v in raw_row.items()}

        # Parse workout timestamp
        workout_ts = raw_row.get("Workout Timestamp", "")
        date_str, time_str, offset = parse_peloton_timestamp(workout_ts)
        row["workout_date"] = date_str
        row["workout_time"] = time_str
        row["utc_offset"] = offset

        # Parse class timestamp
        class_ts = raw_row.get("Class Timestamp", "")
        c_date, c_time, c_offset = parse_peloton_timestamp(class_ts)
        row["class_date"] = c_date
        row["class_time"] = c_time

        rows.append(row)

    # Get all column names
    all_cols = list(dict.fromkeys(col for row in rows for col in row.keys()))

    # Group by workout date
    by_date: dict[str, list[dict]] = {}
    for row in rows:
        by_date.setdefault(row["workout_date"], []).append(row)

    # Write partitioned files
    total_written = 0
    for date_str, date_rows in sorted(by_date.items()):
        if date_str == "unknown":
            print(f"  WARN: Skipping {len(date_rows)} rows with unparseable timestamps")
            continue

        year, month, day = date_str[:4], date_str[5:7], date_str[8:10]
        out_dir = OUTPUT_DIR / "peloton" / "workouts" / f"year={year}" / f"month={month}" / f"day={day}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "workouts.csv"
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(date_rows)
            total_written += len(date_rows)

    unique_dates = len([d for d in by_date if d != "unknown"])
    print(f"Wrote {total_written} records across {unique_dates} date partitions")
    print(f"Output at: {OUTPUT_DIR / 'peloton' / 'workouts'}")


if __name__ == "__main__":
    main()
