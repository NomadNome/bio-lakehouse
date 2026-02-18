#!/usr/bin/env python3
"""
Bio Insights Engine - Weekly Report Runner

Entry point for generating and delivering the weekly report.
Run manually or via cron (Mondays at 7 AM EST):
    0 7 * * 1 /usr/local/bin/python3 /path/to/run_weekly_report.py

Usage:
    python scripts/run_weekly_report.py
    python scripts/run_weekly_report.py --week-ending 2026-02-16
    python scripts/run_weekly_report.py --local-only
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.core.athena_client import AthenaClient
from insights_engine.reports.weekly_report import WeeklyReportGenerator
from insights_engine.reports.delivery import upload_to_s3, save_local


def main():
    parser = argparse.ArgumentParser(description="Generate weekly bio-insights report")
    parser.add_argument(
        "--week-ending",
        type=str,
        default=None,
        help="End date for the report week (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Save report locally only, don't upload to S3.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to save local report (default: reports_output/)",
    )
    args = parser.parse_args()

    # Parse week ending date
    if args.week_ending:
        week_ending = date.fromisoformat(args.week_ending)
    else:
        week_ending = date.today()

    print(f"Generating weekly report for week ending {week_ending}...")

    athena = AthenaClient()
    generator = WeeklyReportGenerator(athena)
    result = generator.generate(week_ending=week_ending)

    # Always save locally
    local_path = save_local(result.html, args.output_dir)
    print(f"Local: {local_path}")

    # Upload to S3 unless --local-only
    if not args.local_only:
        try:
            s3_uri = upload_to_s3(result.html, week_ending)
            print(f"S3: {s3_uri}")
        except Exception as e:
            print(f"WARNING: S3 upload failed: {e}")
            print("Report saved locally only.")

    print(f"\nDone! Generated in {result.metadata['generation_time_sec']}s")
    print(f"Insights: {result.metadata['insight_count']}")


if __name__ == "__main__":
    main()
