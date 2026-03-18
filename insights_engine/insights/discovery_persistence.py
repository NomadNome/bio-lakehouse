"""
Bio Insights Engine - Discovery Persistence

Save and load DiscoveryResult JSON archives to/from S3 gold bucket.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from insights_engine.config import AWS_CONFIG, DISCOVERY_CONFIG
from insights_engine.insights.correlation_discovery import DiscoveryResult


class DiscoveryStore:
    """Persist and retrieve DiscoveryResult objects from S3."""

    def __init__(self, bucket: str = None):
        self.bucket = bucket or AWS_CONFIG["gold_bucket"]
        self.prefix = DISCOVERY_CONFIG["s3_prefix"]
        self.s3 = boto3.client("s3", region_name=AWS_CONFIG["aws_region"])

    def save(self, result: DiscoveryResult) -> str:
        """Save a DiscoveryResult to S3. Returns the S3 URI."""
        key = f"{self.prefix}/{result.run_date}/discoveries.json"
        body = json.dumps(result.to_dict(), indent=2)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        s3_uri = f"s3://{self.bucket}/{key}"
        print(f"Discovery results saved to {s3_uri}")
        return s3_uri

    def load_latest(self) -> DiscoveryResult | None:
        """Load the most recent DiscoveryResult from S3."""
        runs = self.list_runs()
        if not runs:
            return None
        latest_date = sorted(runs, reverse=True)[0]
        return self.load_by_date(latest_date)

    def load_by_date(self, run_date: str) -> DiscoveryResult | None:
        """Load a DiscoveryResult for a specific run date (YYYY-MM-DD)."""
        key = f"{self.prefix}/{run_date}/discoveries.json"
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return DiscoveryResult.from_dict(data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def list_runs(self) -> list[str]:
        """List all available discovery run dates."""
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{self.prefix}/",
                Delimiter="/",
            )
        except ClientError:
            return []

        dates = []
        for prefix_obj in response.get("CommonPrefixes", []):
            # prefix looks like "discoveries/weekly/2026-03-16/"
            parts = prefix_obj["Prefix"].rstrip("/").split("/")
            if parts:
                dates.append(parts[-1])

        return sorted(dates)

    def save_local(self, result: DiscoveryResult, output_dir: str | Path = None) -> Path:
        """Save DiscoveryResult to a local JSON file."""
        if output_dir is None:
            output_dir = Path.cwd() / "reports_output"
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        path = output_dir / "latest_discoveries.json"
        path.write_text(
            json.dumps(result.to_dict(), indent=2),
            encoding="utf-8",
        )
        print(f"Discovery results saved to {path}")
        return path
