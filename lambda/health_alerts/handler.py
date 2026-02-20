"""
Bio Lakehouse - Health Alerts Lambda Handler

Triggered daily by EventBridge at 3:00 AM UTC (after Gold jobs complete ~2:30 AM).
Queries Athena for latest metrics and 30-day baselines, evaluates alert conditions,
and publishes notifications to SNS.

Alert conditions:
  1. Resting HR > mean + 1.5 * std_dev
  2. HRV < mean - 1.5 * std_dev
  3. Overtraining risk = 'high_risk'
  4. Readiness declining 3 consecutive days
"""

import json
import os
import time

import boto3

# Clients
athena = boto3.client("athena")
sns = boto3.client("sns")

# Environment
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "bio_gold")
ATHENA_RESULTS_BUCKET = os.environ.get(
    "ATHENA_RESULTS_BUCKET", "bio-lakehouse-athena-results-000000000000"
)


def run_athena_query(sql):
    """Execute an Athena query and return rows as list of dicts."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/health-alerts/"
        },
    )
    execution_id = response["QueryExecutionId"]

    # Poll for completion
    for _ in range(60):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)
    else:
        raise RuntimeError("Athena query timed out after 120s")

    # Fetch results
    result = athena.get_query_results(QueryExecutionId=execution_id)
    rows = result["ResultSet"]["Rows"]
    if len(rows) < 2:
        return []

    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    return [
        {headers[i]: col.get("VarCharValue") for i, col in enumerate(row["Data"])}
        for row in rows[1:]
    ]


def safe_float(value, default=None):
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def check_alerts():
    """Evaluate all alert conditions and return triggered alerts."""
    alerts = []

    # Query 1: Latest metrics + 30-day baselines (mean, stddev)
    baseline_sql = """
    SELECT
        AVG(resting_heart_rate_bpm) AS rhr_mean,
        STDDEV(resting_heart_rate_bpm) AS rhr_std,
        AVG(hrv_ms) AS hrv_mean,
        STDDEV(hrv_ms) AS hrv_std
    FROM bio_gold.daily_readiness_performance
    WHERE resting_heart_rate_bpm IS NOT NULL
      AND COALESCE(
            TRY(CAST(date AS date)),
            TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
          ) >= CURRENT_DATE - INTERVAL '30' DAY
    """

    latest_sql = """
    SELECT
        date,
        resting_heart_rate_bpm,
        hrv_ms,
        readiness_score
    FROM bio_gold.daily_readiness_performance
    WHERE resting_heart_rate_bpm IS NOT NULL
    ORDER BY date DESC
    LIMIT 1
    """

    # Query 2: Overtraining risk
    overtraining_sql = """
    SELECT date, overtraining_risk
    FROM bio_gold.overtraining_risk
    ORDER BY date DESC
    LIMIT 1
    """

    # Query 3: Readiness trend (last 3 days)
    readiness_trend_sql = """
    SELECT date, readiness_score
    FROM bio_gold.daily_readiness_performance
    WHERE readiness_score IS NOT NULL
    ORDER BY date DESC
    LIMIT 3
    """

    # Run queries
    baseline_rows = run_athena_query(baseline_sql)
    latest_rows = run_athena_query(latest_sql)
    overtraining_rows = run_athena_query(overtraining_sql)
    readiness_rows = run_athena_query(readiness_trend_sql)

    # Alert 1: Resting HR elevated
    if baseline_rows and latest_rows:
        b = baseline_rows[0]
        l = latest_rows[0]
        rhr_mean = safe_float(b.get("rhr_mean"))
        rhr_std = safe_float(b.get("rhr_std"))
        rhr_latest = safe_float(l.get("resting_heart_rate_bpm"))

        if rhr_mean and rhr_std and rhr_latest:
            threshold = rhr_mean + 1.5 * rhr_std
            if rhr_latest > threshold:
                alerts.append({
                    "condition": "Elevated Resting Heart Rate",
                    "message": (
                        f"Your resting heart rate ({rhr_latest:.0f} bpm) is above "
                        f"your 30-day norm ({rhr_mean:.0f} +/- {rhr_std:.1f} bpm). "
                        f"This may indicate stress, illness, or insufficient recovery."
                    ),
                    "severity": "warning",
                })

    # Alert 2: HRV depressed
    if baseline_rows and latest_rows:
        b = baseline_rows[0]
        l = latest_rows[0]
        hrv_mean = safe_float(b.get("hrv_mean"))
        hrv_std = safe_float(b.get("hrv_std"))
        hrv_latest = safe_float(l.get("hrv_ms"))

        if hrv_mean and hrv_std and hrv_latest:
            threshold = hrv_mean - 1.5 * hrv_std
            if hrv_latest < threshold:
                alerts.append({
                    "condition": "Depressed HRV",
                    "message": (
                        f"Your HRV ({hrv_latest:.0f} ms) is below "
                        f"your 30-day norm ({hrv_mean:.0f} +/- {hrv_std:.1f} ms). "
                        f"Consider a rest day or reduced training intensity."
                    ),
                    "severity": "warning",
                })

    # Alert 3: Overtraining risk = high_risk
    if overtraining_rows:
        risk = overtraining_rows[0].get("overtraining_risk", "")
        if risk == "high_risk":
            alerts.append({
                "condition": "High Overtraining Risk",
                "message": (
                    f"Overtraining risk is HIGH as of {overtraining_rows[0].get('date', '?')}. "
                    f"Readiness has been declining and/or workout load is unsustainable. "
                    f"Take a rest day."
                ),
                "severity": "critical",
            })

    # Alert 4: Readiness declining 3 consecutive days
    if len(readiness_rows) >= 3:
        scores = [safe_float(r.get("readiness_score")) for r in readiness_rows]
        if all(s is not None for s in scores):
            # Rows are newest-first, so declining = each older day was higher
            if scores[0] < scores[1] < scores[2]:
                alerts.append({
                    "condition": "Readiness Declining",
                    "message": (
                        f"Readiness has declined 3 days in a row: "
                        f"{scores[2]:.0f} -> {scores[1]:.0f} -> {scores[0]:.0f}. "
                        f"Monitor recovery and consider adjusting training load."
                    ),
                    "severity": "info",
                })

    return alerts


def publish_alerts(alerts):
    """Publish alerts to SNS topic."""
    if not SNS_TOPIC_ARN:
        print("No SNS_TOPIC_ARN configured, skipping publish")
        return

    subject = f"Bio Lakehouse Health Alert ({len(alerts)} condition{'s' if len(alerts) != 1 else ''})"

    body_lines = ["Bio Lakehouse - Daily Health Alert Summary", "=" * 50, ""]
    for alert in alerts:
        severity_icon = {"critical": "!!!", "warning": "!", "info": "i"}.get(
            alert["severity"], "?"
        )
        body_lines.append(f"[{severity_icon}] {alert['condition']}")
        body_lines.append(f"    {alert['message']}")
        body_lines.append("")

    body_lines.append("---")
    body_lines.append("This alert was generated by Bio Lakehouse Health Alerts.")
    body_lines.append("Data sources: Oura Ring + Peloton + Apple Health")

    message = "\n".join(body_lines)

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject limit
        Message=message,
    )
    print(f"Published {len(alerts)} alerts to SNS")


def lambda_handler(event, context):
    """Daily health alert evaluation handler."""
    print("Starting health alert evaluation...")

    try:
        alerts = check_alerts()
        print(f"Alert evaluation complete: {len(alerts)} alerts triggered")

        if alerts:
            publish_alerts(alerts)
            for alert in alerts:
                print(f"  [{alert['severity']}] {alert['condition']}: {alert['message']}")
        else:
            print("No alert conditions triggered. All metrics within normal range.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "alerts_triggered": len(alerts),
                "alerts": alerts,
            }),
        }

    except Exception as e:
        print(f"Health alert evaluation failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
