"""
Bio Lakehouse - Ingestion Trigger Lambda Handler

Triggered by S3 PUT events on the Bronze bucket. Validates uploaded CSV files,
logs ingestion metadata to DynamoDB, and triggers downstream Glue ETL jobs.
"""

import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

# Clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
glue = boto3.client("glue")

# Environment
INGESTION_LOG_TABLE = os.environ.get("INGESTION_LOG_TABLE", "bio_ingestion_log")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
OURA_GLUE_JOB = os.environ.get("OURA_GLUE_JOB", "bio-lakehouse-oura-normalizer")
PELOTON_GLUE_JOB = os.environ.get("PELOTON_GLUE_JOB", "bio-lakehouse-peloton-normalizer")
HEALTHKIT_GLUE_JOB = os.environ.get("HEALTHKIT_GLUE_JOB", "bio-lakehouse-healthkit-normalizer")

# Expected headers for validation
EXPECTED_HEADERS = {
    "oura/readiness": ["id", "day", "score", "timestamp"],
    "oura/sleep": ["id", "day", "score", "timestamp"],
    "oura/activity": ["id", "day", "score", "timestamp", "active_calories", "steps"],
    "oura/workout": ["id", "activity", "calories", "day"],
    "peloton/workouts": [
        "workout_timestamp",
        "fitness_discipline",
        "total_output",
        "calories_burned",
    ],
    "healthkit/daily_vitals": ["date", "resting_heart_rate_bpm"],
    "healthkit/workouts": ["date", "workout_type", "duration_minutes"],
    "healthkit/body": ["date", "weight_lbs", "device_name"],
    "healthkit/mindfulness": ["date", "duration_minutes"],
}


def detect_source(key: str) -> str:
    """Determine data source from S3 key path."""
    for prefix in EXPECTED_HEADERS:
        if prefix in key:
            return prefix
    return "unknown"


def validate_csv_headers(bucket: str, key: str, source: str) -> dict:
    """Download first 1KB of CSV and validate headers match expected schema."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-1024")
        first_chunk = resp["Body"].read().decode("utf-8")
        first_line = first_chunk.split("\n")[0].strip()
        headers = [h.strip().lower() for h in first_line.split(",")]

        expected = EXPECTED_HEADERS.get(source, [])
        missing = [h for h in expected if h not in headers]

        return {
            "valid": len(missing) == 0,
            "headers_found": headers,
            "missing_headers": missing,
            "header_count": len(headers),
        }
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "headers_found": [],
            "missing_headers": [],
            "header_count": 0,
        }


def log_ingestion(file_path: str, metadata: dict) -> None:
    """Write ingestion record to DynamoDB."""
    table = dynamodb.Table(INGESTION_LOG_TABLE)
    table.put_item(
        Item={
            "file_path": file_path,
            "upload_timestamp": int(datetime.now(timezone.utc).timestamp()),
            "source": metadata.get("source", "unknown"),
            "validation": metadata.get("validation", {}),
            "file_size": metadata.get("file_size", 0),
            "environment": ENVIRONMENT,
            "status": "ingested" if metadata.get("valid", False) else "validation_failed",
        }
    )


def get_job_name(source):
    """Return the Glue job name for a given source type."""
    if source.startswith("oura/"):
        return OURA_GLUE_JOB
    elif source.startswith("peloton/"):
        return PELOTON_GLUE_JOB
    elif source.startswith("healthkit/"):
        return HEALTHKIT_GLUE_JOB
    return None


def is_job_running(job_name):
    """Check if a Glue job currently has an active run."""
    try:
        response = glue.get_job_runs(JobName=job_name, MaxResults=1)
        runs = response.get("JobRuns", [])
        if runs and runs[0].get("JobRunState") in ("STARTING", "RUNNING", "STOPPING"):
            return True
    except Exception as e:
        print(f"Failed to check job status for {job_name}: {e}")
    return False


def trigger_glue_job(source, bucket, key):
    """Start the appropriate Glue job based on data source."""
    job_name = get_job_name(source)
    if not job_name:
        return None

    if is_job_running(job_name):
        print(f"Glue job {job_name} already running, skipping trigger")
        return None

    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--source_bucket": bucket,
                "--source_key": key,
                "--source_type": source,
            },
        )
        return response.get("JobRunId")
    except glue.exceptions.ConcurrentRunsExceededException:
        print(f"Glue job {job_name} already running, skipping trigger")
        return None
    except Exception as e:
        print(f"Failed to trigger Glue job {job_name}: {e}")
        return None


def handle_batch_manifest(bucket, key):
    """Process a batch manifest file and trigger Glue jobs for each source type."""
    print(f"Batch manifest detected: s3://{bucket}/{key}")

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        manifest = json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Failed to read manifest: {e}")
        return {"error": str(e)}

    batch_id = manifest.get("batch_id", "unknown")
    source_types = manifest.get("source_types", [])
    file_count = manifest.get("file_count", 0)

    print(f"Batch '{batch_id}': {file_count} files, sources={source_types}")

    triggered = []
    skipped = []

    for source in source_types:
        job_name = get_job_name(source)
        if not job_name:
            print(f"No Glue job mapped for source: {source}")
            skipped.append(source)
            continue

        if is_job_running(job_name):
            print(f"Glue job {job_name} already running, skipping")
            skipped.append(source)
            continue

        try:
            response = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    "--source_bucket": bucket,
                    "--source_type": source,
                    "--batch_id": batch_id,
                },
            )
            run_id = response.get("JobRunId")
            print(f"Triggered {job_name} for {source}, run ID: {run_id}")
            triggered.append({"source": source, "job_name": job_name, "run_id": run_id})
        except Exception as e:
            print(f"Failed to trigger {job_name}: {e}")
            skipped.append(source)

    # Log batch ingestion to DynamoDB
    log_ingestion(
        file_path=f"s3://{bucket}/{key}",
        metadata={
            "source": "batch",
            "validation": {"valid": True},
            "file_size": file_count,
            "valid": True,
            "batch_id": batch_id,
            "source_types": source_types,
        },
    )

    return {
        "batch_id": batch_id,
        "triggered": triggered,
        "skipped": skipped,
    }


def lambda_handler(event, context):
    """Process S3 PUT event for Bronze bucket ingestion."""
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size = record["s3"]["object"].get("size", 0)

        print(f"Processing: s3://{bucket}/{key} ({size} bytes)")

        # Batch manifest handling — trigger normalizers once for bulk uploads
        if key.endswith("_manifest.json"):
            batch_result = handle_batch_manifest(bucket, key)
            results.append({"key": key, "source": "batch", "batch": batch_result})
            continue

        # Detect source type from path
        source = detect_source(key)
        print(f"Detected source: {source}")

        # Validate CSV headers
        validation = validate_csv_headers(bucket, key, source)
        print(f"Validation result: valid={validation['valid']}")

        if validation.get("missing_headers"):
            print(f"Missing headers: {validation['missing_headers']}")

        # Log to DynamoDB
        log_ingestion(
            file_path=f"s3://{bucket}/{key}",
            metadata={
                "source": source,
                "validation": validation,
                "file_size": size,
                "valid": validation["valid"],
            },
        )

        # Trigger Glue job if validation passed
        glue_run_id = None
        if validation["valid"]:
            glue_run_id = trigger_glue_job(source, bucket, key)
            if glue_run_id:
                print(f"Triggered Glue job, run ID: {glue_run_id}")

        results.append(
            {
                "key": key,
                "source": source,
                "valid": validation["valid"],
                "glue_run_id": glue_run_id,
            }
        )

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": len(results), "results": results}),
    }
