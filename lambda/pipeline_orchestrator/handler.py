"""
Bio Lakehouse - Pipeline Orchestrator Lambda

Triggered by EventBridge when the Oura normalizer Glue job succeeds.
Chains the remaining pipeline steps: Silver crawler -> Gold refresh ->
Gold crawler -> Morning Briefing Lambda.

Skips orchestration if the manual pipeline (run_daily_ingestion.sh) is
running (detected by checking if HK/Peloton normalizers are active).
"""

import json
import os
import time

import boto3

glue = boto3.client("glue")
lam = boto3.client("lambda")

REGION = os.environ.get("AWS_REGION", "us-east-1")
SILVER_CRAWLER = os.environ.get("SILVER_CRAWLER", "bio-lakehouse-silver-crawler")
GOLD_REFRESH_JOB = os.environ.get("GOLD_REFRESH_JOB", "bio-lakehouse-dbt-gold-refresh")
GOLD_CRAWLER = os.environ.get("GOLD_CRAWLER", "bio-lakehouse-gold-crawler")
MORNING_BRIEFING_FN = os.environ.get("MORNING_BRIEFING_FN", "bio-lakehouse-morning-briefing")
HK_NORMALIZER = os.environ.get("HK_NORMALIZER", "bio-lakehouse-healthkit-normalizer")
PELOTON_NORMALIZER = os.environ.get("PELOTON_NORMALIZER", "bio-lakehouse-peloton-normalizer")


def is_job_active(job_name):
    """Check if a Glue job has a RUNNING or STARTING run."""
    try:
        resp = glue.get_job_runs(JobName=job_name, MaxResults=1)
        runs = resp.get("JobRuns", [])
        if runs and runs[0].get("JobRunState") in ("STARTING", "RUNNING"):
            return True
    except Exception as e:
        print(f"Error checking {job_name}: {e}")
    return False


def wait_for_crawler(crawler_name, timeout=300):
    """Start a crawler and wait for it to finish. Returns True on success."""
    print(f"Starting crawler: {crawler_name}")
    try:
        glue.start_crawler(Name=crawler_name)
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler {crawler_name} already running, waiting...")

    for _ in range(timeout // 10):
        time.sleep(10)
        resp = glue.get_crawler(Name=crawler_name)
        state = resp["Crawler"]["State"]
        print(f"  Crawler {crawler_name}: {state}")
        if state == "READY":
            return True
    print(f"Crawler {crawler_name} timed out after {timeout}s")
    return False


def wait_for_job(job_name, timeout=600):
    """Start a Glue job (or attach to existing run) and wait for completion."""
    print(f"Starting job: {job_name}")
    try:
        resp = glue.start_job_run(JobName=job_name)
        run_id = resp["JobRunId"]
        print(f"  Run ID: {run_id}")
    except glue.exceptions.ConcurrentRunsExceededException:
        # Attach to the already-running job
        runs = glue.get_job_runs(JobName=job_name, MaxResults=1)
        if not runs.get("JobRuns"):
            print(f"  ConcurrentRunsExceeded but no runs found — failing")
            return False
        run_id = runs["JobRuns"][0]["Id"]
        print(f"  Job already running, attaching to run: {run_id}")

    for _ in range(timeout // 15):
        time.sleep(15)
        resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = resp["JobRun"]["JobRunState"]
        print(f"  Job {job_name}: {state}")
        if state == "SUCCEEDED":
            return True
        if state in ("FAILED", "STOPPED", "ERROR", "TIMEOUT"):
            reason = resp["JobRun"].get("ErrorMessage", "unknown")
            print(f"  Job FAILED: {reason}")
            return False
    print(f"Job {job_name} timed out after {timeout}s")
    return False


def lambda_handler(event, context):
    """Orchestrate Silver -> Gold -> Briefing pipeline after Oura normalizer."""
    print(f"Pipeline orchestrator triggered: {json.dumps(event, default=str)}")

    # Skip if manual pipeline is running (HK or Peloton normalizers active)
    if is_job_active(HK_NORMALIZER) or is_job_active(PELOTON_NORMALIZER):
        msg = "Manual pipeline in progress (HK/Peloton normalizer running) — skipping."
        print(msg)
        return {"statusCode": 200, "body": msg}

    # Step 1: Silver crawler
    if not wait_for_crawler(SILVER_CRAWLER, timeout=300):
        return {"statusCode": 500, "body": "Silver crawler failed or timed out"}

    # Step 2: Gold refresh (dbt)
    if not wait_for_job(GOLD_REFRESH_JOB, timeout=600):
        return {"statusCode": 500, "body": "Gold refresh failed or timed out"}

    # Step 3: Gold crawler
    if not wait_for_crawler(GOLD_CRAWLER, timeout=300):
        return {"statusCode": 500, "body": "Gold crawler failed or timed out"}

    # Step 4: Morning briefing
    print(f"Invoking morning briefing: {MORNING_BRIEFING_FN}")
    try:
        resp = lam.invoke(
            FunctionName=MORNING_BRIEFING_FN,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        payload = json.loads(resp["Payload"].read())
        print(f"Morning briefing result: {payload}")
    except Exception as e:
        print(f"Morning briefing invoke failed: {e}")
        return {"statusCode": 500, "body": f"Briefing failed: {e}"}

    return {
        "statusCode": 200,
        "body": "Pipeline complete: Silver crawler -> Gold refresh -> Gold crawler -> Briefing",
    }
