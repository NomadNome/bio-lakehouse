"""
Bio Lakehouse - Morning Briefing Lambda Handler

Triggered daily at 7 AM EST (12:00 UTC) by EventBridge.
Queries today's readiness, sleep, energy state, and workout recommendation,
then sends a 3-4 bullet actionable summary via SNS email.

No charts — just the actionable takeaway for the morning.
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3

# Clients
athena = boto3.client("athena")
sns = boto3.client("sns")

# Environment
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "bio_gold")
ATHENA_RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", "")


def run_athena_query(sql):
    """Execute an Athena query and return rows as list of dicts."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/morning-briefing/"
        },
    )
    execution_id = response["QueryExecutionId"]

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


def build_briefing():
    """Query latest metrics and build the morning briefing bullets."""
    bullets = []

    # Query 1: Latest day's core metrics
    latest_sql = """
    SELECT
        date,
        readiness_score,
        sleep_score,
        resting_heart_rate_bpm,
        hrv_ms,
        had_workout,
        combined_wellness_score
    FROM bio_gold.daily_readiness_performance
    WHERE readiness_score IS NOT NULL
    ORDER BY date DESC
    LIMIT 1
    """

    # Query 2: Energy state for today
    energy_sql = """
    SELECT date, energy_state, guidance
    FROM bio_gold.energy_state
    ORDER BY date DESC
    LIMIT 1
    """

    # Query 3: Workout recommendation
    workout_sql = """
    SELECT date, recommended_intensity, recommendation_text
    FROM bio_gold.workout_recommendations
    ORDER BY date DESC
    LIMIT 1
    """

    # Query 4: Training load (TSB / form)
    training_sql = """
    SELECT date, tss
    FROM bio_gold.training_load_daily
    ORDER BY date DESC
    LIMIT 7
    """

    latest_rows = run_athena_query(latest_sql)
    energy_rows = run_athena_query(energy_sql)
    workout_rows = run_athena_query(workout_sql)
    training_rows = run_athena_query(training_sql)

    latest_date = "?"

    # Bullet 1: Readiness + Sleep summary
    if latest_rows:
        r = latest_rows[0]
        latest_date = r.get("date", "?")
        readiness = safe_float(r.get("readiness_score"))
        sleep = safe_float(r.get("sleep_score"))
        rhr = safe_float(r.get("resting_heart_rate_bpm"))
        hrv = safe_float(r.get("hrv_ms"))

        parts = []
        if readiness is not None:
            parts.append(f"Readiness {readiness:.0f}")
        if sleep is not None:
            parts.append(f"Sleep {sleep:.0f}")
        if parts:
            vitals = []
            if rhr is not None:
                vitals.append(f"RHR {rhr:.0f}")
            if hrv is not None:
                vitals.append(f"HRV {hrv:.0f}")
            vitals_str = f" ({', '.join(vitals)})" if vitals else ""
            bullets.append(f"{' | '.join(parts)}{vitals_str}")

    # Bullet 2: Energy state
    if energy_rows:
        e = energy_rows[0]
        state = e.get("energy_state", "unknown")
        guidance = e.get("guidance", "")
        state_display = state.replace("_", " ").title()
        bullets.append(f"Energy: {state_display} -- {guidance}")

    # Bullet 3: Workout recommendation
    if workout_rows:
        w = workout_rows[0]
        rec = w.get("recommendation_text", "No recommendation available.")
        bullets.append(f"Workout: {rec}")

    # Bullet 4: Training load context (7-day avg TSS)
    if training_rows and len(training_rows) >= 3:
        tss_values = [safe_float(r.get("tss", 0), 0) for r in training_rows]
        avg_tss_7d = sum(tss_values) / len(tss_values) if tss_values else 0
        yesterday_tss = tss_values[0] if tss_values else 0

        if yesterday_tss > 0:
            bullets.append(
                f"Yesterday's TSS: {yesterday_tss:.0f} | "
                f"7-day avg: {avg_tss_7d:.0f}"
            )

    return latest_date, bullets


def publish_briefing(latest_date, bullets):
    """Publish the morning briefing to SNS."""
    if not SNS_TOPIC_ARN:
        print("No SNS_TOPIC_ARN configured, skipping publish")
        return

    today = datetime.now(timezone.utc).strftime("%A, %b %d")
    subject = f"Morning Briefing - {today}"

    body_lines = [
        f"Good morning! Here's your bio-optimization briefing.",
        f"(Latest data: {latest_date})",
        "",
    ]
    for i, bullet in enumerate(bullets, 1):
        body_lines.append(f"{i}. {bullet}")

    body_lines.extend([
        "",
        "---",
        "Bio Lakehouse Morning Briefing",
        "Data: Oura Ring + Peloton + Apple Health",
    ])

    message = "\n".join(body_lines)

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],
        Message=message,
    )
    print(f"Published morning briefing to SNS")


def lambda_handler(event, context):
    """Daily morning briefing handler."""
    print("Building morning briefing...")

    try:
        latest_date, bullets = build_briefing()
        print(f"Briefing built: {len(bullets)} bullets for {latest_date}")

        for b in bullets:
            print(f"  - {b}")

        if bullets:
            publish_briefing(latest_date, bullets)
        else:
            print("No data available for briefing.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "latest_date": latest_date,
                "bullets": bullets,
            }),
        }

    except Exception as e:
        print(f"Morning briefing failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
