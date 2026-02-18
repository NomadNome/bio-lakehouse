"""
Bio Insights Engine - Report Delivery

Upload generated reports to S3 gold bucket.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import boto3

from insights_engine.config import AWS_CONFIG


def upload_to_s3(
    html: str,
    week_ending: date,
    bucket: str = None,
    prefix: str = "reports/weekly",
) -> str:
    """Upload HTML report to S3 gold bucket. Returns the S3 URI."""
    bucket = bucket or AWS_CONFIG["gold_bucket"]
    key = f"{prefix}/{week_ending.isoformat()}/weekly-report.html"

    s3 = boto3.client("s3", region_name=AWS_CONFIG["aws_region"])
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )

    s3_uri = f"s3://{bucket}/{key}"
    print(f"Report uploaded to {s3_uri}")
    return s3_uri


def save_local(html: str, output_dir: str | Path = None) -> Path:
    """Save HTML report to a local file. Returns the file path."""
    if output_dir is None:
        output_dir = Path.cwd() / "reports_output"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    path = output_dir / "weekly-report.html"
    path.write_text(html, encoding="utf-8")
    print(f"Report saved to {path}")
    return path
