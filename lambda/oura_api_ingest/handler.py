"""
Bio Lakehouse - Oura API Ingestion Lambda Handler

Fetches daily Oura data (readiness, sleep, activity) from the Oura API v2,
transforms JSON responses into comma-delimited CSVs matching the expected
downstream schema, and writes them to the Bronze S3 bucket.

Triggered by EventBridge daily cron or manual invocation.

Event format:
    Daily cron (default):   {} — fetches yesterday's data
    Single date:            {"date": "2026-02-17"}
    Backfill range:         {"start_date": "2025-01-01", "end_date": "2025-12-31"}
"""

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

import boto3

from oura_client import fetch_daily_data
from csv_transformer import records_to_csv

# AWS clients
ssm = boto3.client("ssm")
s3 = boto3.client("s3")

# Configuration
SSM_TOKEN_PARAM = os.environ.get("SSM_TOKEN_PARAM", "/bio-lakehouse/oura-api-token")
# When set, OAuth2 mode is used instead of a legacy PAT (Oura deprecated PATs
# in 2026; new users authorize an OAuth app via scripts/oura_oauth_authorize.py)
SSM_OAUTH_PARAM = os.environ.get("SSM_OAUTH_PARAM", "")
OURA_TOKEN_URL = "https://api.ouraring.com/oauth/token"
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "")
DATA_TYPES = ["readiness", "sleep", "activity"]

S3_PREFIX_MAP = {
    "readiness": "oura/readiness",
    "sleep": "oura/sleep",
    "activity": "oura/activity",
}

FILE_NAME_MAP = {
    "readiness": "dailyreadiness.csv",
    "sleep": "dailysleep.csv",
    "activity": "dailyactivity.csv",
}


def get_oura_token():
    """Return a usable Oura bearer token.

    PAT mode (legacy, grandfathered accounts): read the token straight from
    SSM. OAuth mode: read the token bundle, refresh if within 5 minutes of
    expiry, and persist the rotated refresh token (Oura refresh tokens are
    single-use — losing the rotation means re-authorizing in the browser).
    """
    if SSM_OAUTH_PARAM:
        return _get_oauth_access_token()
    resp = ssm.get_parameter(Name=SSM_TOKEN_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _get_oauth_access_token():
    bundle = json.loads(
        ssm.get_parameter(Name=SSM_OAUTH_PARAM, WithDecryption=True)[
            "Parameter"
        ]["Value"]
    )
    if bundle.get("expires_at", 0) - time.time() > 300:
        return bundle["access_token"]

    print("OAuth access token near expiry — refreshing")
    data = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": bundle["refresh_token"],
            "client_id": bundle["client_id"],
            "client_secret": bundle["client_secret"],
        }
    ).encode()
    with urllib.request.urlopen(
        urllib.request.Request(OURA_TOKEN_URL, data=data), timeout=15
    ) as resp:
        tokens = json.loads(resp.read().decode("utf-8"))

    bundle["access_token"] = tokens["access_token"]
    bundle["refresh_token"] = tokens.get("refresh_token", bundle["refresh_token"])
    bundle["expires_at"] = int(time.time()) + int(tokens.get("expires_in", 86400))

    # Persist BEFORE returning: the old refresh token is now dead.
    ssm.put_parameter(
        Name=SSM_OAUTH_PARAM,
        Type="SecureString",
        Value=json.dumps(bundle),
        Overwrite=True,
    )
    return bundle["access_token"]


def build_s3_key(data_type, day_str):
    """Build the S3 key matching existing partition structure.

    Example: oura/readiness/year=2026/month=02/day=17/dailyreadiness.csv
    """
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    prefix = S3_PREFIX_MAP[data_type]
    filename = FILE_NAME_MAP[data_type]
    return (
        f"{prefix}/year={dt.year:04d}/month={dt.month:02d}"
        f"/day={dt.day:02d}/{filename}"
    )


def upload_csv(csv_content, s3_key):
    """Upload CSV string to S3 with required AES256 encryption."""
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )


def process_date_range(token, start_date, end_date):
    """Fetch and upload data for a date range, one day at a time.

    Each data type is processed independently so partial failures
    don't block other types.
    """
    results = []

    for data_type in DATA_TYPES:
        try:
            print(f"Fetching {data_type} for {start_date} to {end_date}")
            records = fetch_daily_data(token, data_type, start_date, end_date)

            if not records:
                print(f"No {data_type} records returned")
                results.append({
                    "type": data_type,
                    "status": "no_data",
                    "records": 0,
                })
                continue

            # Group records by day for per-day CSV files
            by_day = {}
            for record in records:
                day = record.get("day", start_date)
                by_day.setdefault(day, []).append(record)

            uploaded = 0
            for day_str, day_records in by_day.items():
                csv_content = records_to_csv(day_records, data_type)
                s3_key = build_s3_key(data_type, day_str)
                upload_csv(csv_content, s3_key)
                uploaded += len(day_records)
                print(f"Uploaded s3://{BRONZE_BUCKET}/{s3_key} ({len(day_records)} records)")

            results.append({
                "type": data_type,
                "status": "success",
                "records": uploaded,
            })

        except ValueError as e:
            # Auth errors — log and continue with other types
            print(f"Auth error for {data_type}: {e}")
            results.append({
                "type": data_type,
                "status": "auth_error",
                "error": str(e),
            })
        except Exception as e:
            print(f"Error processing {data_type}: {e}")
            results.append({
                "type": data_type,
                "status": "error",
                "error": str(e),
            })

    return results


def lambda_handler(event, context):
    """Main Lambda entry point.

    Supports three invocation modes:
    1. No event / empty event: fetch yesterday's data
    2. {"date": "YYYY-MM-DD"}: fetch a specific date
    3. {"start_date": "...", "end_date": "..."}: backfill a range
    """
    event = event or {}

    # Determine date range
    if "start_date" in event and "end_date" in event:
        start_date = event["start_date"]
        end_date = event["end_date"]
    elif "date" in event:
        start_date = event["date"]
        end_date = event["date"]
    else:
        # Fetch today + yesterday: today's readiness is available after wake-up,
        # and yesterday's activity may finalize late.
        start_date = (date.today() - timedelta(days=1)).isoformat()
        end_date = date.today().isoformat()

    print(f"Processing date range: {start_date} to {end_date}")

    token = get_oura_token()
    results = process_date_range(token, start_date, end_date)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "start_date": start_date,
            "end_date": end_date,
            "results": results,
        }),
    }
