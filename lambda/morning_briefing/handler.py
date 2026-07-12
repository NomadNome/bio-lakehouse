"""
Bio Lakehouse - Morning Briefing Lambda Handler

Invoked by run_daily_ingestion.sh after the Gold layer refreshes.
Queries today's readiness, sleep, energy state, and workout recommendation,
then sends a 3-4 bullet actionable summary via SNS email.

Includes a freshness guard: if Gold data is >1 day stale, sends a
"STALE DATA — Action Needed" alert instead of the normal briefing.

No charts — just the actionable takeaway for the morning.
"""

import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3

# Clients
athena = boto3.client("athena")
sns = boto3.client("sns")

# Environment
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "bio_gold")
ATHENA_RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", "")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "")
ANTHROPIC_KEY_PARAM = os.environ.get("ANTHROPIC_KEY_PARAM", "")
NARRATOR_MODEL = os.environ.get("NARRATOR_MODEL", "claude-sonnet-5")
OURA_TOKEN_PARAM = os.environ.get("OURA_TOKEN_PARAM", "")
OURA_INGEST_FUNCTION = os.environ.get("OURA_INGEST_FUNCTION", "")

s3 = boto3.client("s3")

_anthropic_client = None


def _get_anthropic_client():
    """Lazily build the Anthropic client from the SSM SecureString key."""
    global _anthropic_client
    if _anthropic_client is not None:
        return _anthropic_client
    if not ANTHROPIC_KEY_PARAM:
        return None
    try:
        ssm = boto3.client("ssm")
        key = ssm.get_parameter(Name=ANTHROPIC_KEY_PARAM, WithDecryption=True)[
            "Parameter"
        ]["Value"]
        import anthropic

        _anthropic_client = anthropic.Anthropic(api_key=key)
    except Exception as e:
        print(f"Narration disabled (no client): {e}")
        _anthropic_client = None
    return _anthropic_client


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


def build_energy_guidance(e):
    """Build dynamic, context-specific energy guidance from today's metrics."""
    state = e.get("energy_state", "unknown")
    readiness = safe_float(e.get("readiness_score"))
    sleep = safe_float(e.get("sleep_score"))
    hrv_bal = safe_float(e.get("hrv_balance"))
    r_delta = safe_float(e.get("readiness_delta"), 0)
    s_delta = safe_float(e.get("sleep_delta"), 0)
    r_3day = safe_float(e.get("readiness_3day_avg"))
    had_workout = e.get("had_workout", "false") == "true"

    # Trend description
    trend_parts = []
    if r_delta >= 5:
        trend_parts.append(f"readiness up {r_delta:+.0f}")
    elif r_delta <= -5:
        trend_parts.append(f"readiness down {r_delta:+.0f}")
    if s_delta >= 5:
        trend_parts.append(f"sleep up {s_delta:+.0f}")
    elif s_delta <= -5:
        trend_parts.append(f"sleep down {s_delta:+.0f}")
    trend = f" ({', '.join(trend_parts)} vs yesterday)" if trend_parts else ""

    # 3-day context
    streak = ""
    if r_3day is not None and readiness is not None:
        if r_3day >= 85 and readiness >= 85:
            streak = " You've been consistently high — ride the wave."
        elif r_3day < 65:
            streak = " Multiple low days — prioritize recovery."

    # State-specific guidance with actual numbers
    if state == "peak":
        base = f"Readiness {readiness:.0f}, sleep {sleep:.0f}"
        if hrv_bal is not None:
            base += f", HRV balance {hrv_bal:.0f}"
        base += f".{trend} All systems go — push hard today (HIIT, heavy lifts, sprints)."
        return base + streak
    elif state == "high":
        base = f"Readiness {readiness:.0f}, sleep {sleep:.0f}.{trend}"
        base += " Strong day — great for hard cycling, bootcamp, or deep focus work."
        return base + streak
    elif state == "moderate":
        base = f"Readiness {readiness:.0f}, sleep {sleep:.0f}.{trend}"
        base += " Solid but not peak — good for endurance rides, strength training, or steady work."
        return base + streak
    elif state == "low":
        base = f"Readiness {readiness:.0f}, sleep {sleep:.0f}.{trend}"
        base += " Keep it light — yoga, stretching, or an easy walk."
        return base + streak
    else:  # recovery_needed
        base = f"Readiness {readiness:.0f}" if readiness else "Low recovery"
        if sleep is not None:
            base += f", sleep {sleep:.0f}"
        base += f".{trend} Rest day recommended — gentle meditation or total rest."
        return base + streak


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

    # Query 2: Energy state with context for dynamic guidance
    energy_sql = """
    SELECT date, energy_state, readiness_score, sleep_score, hrv_balance,
           readiness_delta, sleep_delta, readiness_3day_avg, sleep_3day_avg,
           had_workout, output_zone
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

    # Freshness check — if Gold data is >1 day stale, return alert instead
    if latest_rows:
        latest_date = latest_rows[0].get("date", "")
        yesterday_str = (date.today() - timedelta(days=1)).isoformat()
        if latest_date < yesterday_str:
            days_behind = (date.today() - date.fromisoformat(latest_date)).days
            print(f"STALE DATA: latest={latest_date}, {days_behind} day(s) behind")
            return latest_date, [
                f"DATA STALE — Gold data is {days_behind} day(s) behind (latest: {latest_date}).",
                "Action needed: export HealthKit + Peloton and run the daily ingestion pipeline.",
            ]

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
            bullets.append(f"As of {latest_date}: {' | '.join(parts)}{vitals_str}")

    # Bullet 2: Energy state with dynamic context
    if energy_rows:
        e = energy_rows[0]
        state = e.get("energy_state", "unknown")
        state_display = state.replace("_", " ").title()
        bullets.append(f"Energy: {state_display} -- {build_energy_guidance(e)}")

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

    # Bullet 5: Today's session from the saved Coach plan (if current)
    coach_bullet = _get_coach_session()
    if coach_bullet:
        bullets.append(f"Today's plan: {coach_bullet}")

    # Bullet 6: Latest correlation discovery (if < 8 days old)
    discovery_bullet = _get_latest_discovery()
    if discovery_bullet:
        bullets.append(f"Discovery: {discovery_bullet}")

    return latest_date, bullets


def _get_live_oura():
    """Fetch this morning's readiness/sleep directly from the Oura API.

    Gold data is only as fresh as the last pipeline run; the ring's morning
    scores are usually in Oura's cloud before that. This overlay makes the
    briefing current-as-of-this-morning regardless of pipeline timing.
    Returns {"readiness": {...}, "sleep": {...}} for today, or None.
    """
    if not OURA_TOKEN_PARAM:
        return None
    try:
        import urllib.request

        token = boto3.client("ssm").get_parameter(
            Name=OURA_TOKEN_PARAM, WithDecryption=True
        )["Parameter"]["Value"]

        today = date.today().isoformat()
        out = {}
        for dtype, endpoint in (("readiness", "daily_readiness"), ("sleep", "daily_sleep")):
            url = (
                f"https://api.ouraring.com/v2/usercollection/{endpoint}"
                f"?start_date={today}&end_date={today}"
            )
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            recs = [r for r in data.get("data", []) if r.get("day") == today]
            if recs:
                out[dtype] = recs[0]
        return out or None
    except Exception as e:
        print(f"Live Oura fetch failed (falling back to Gold data): {e}")
        return None


def _trigger_oura_ingest(day_str):
    """Fire-and-forget: land today's Oura data in Bronze so Gold catches up."""
    if not OURA_INGEST_FUNCTION:
        return
    try:
        boto3.client("lambda").invoke(
            FunctionName=OURA_INGEST_FUNCTION,
            InvocationType="Event",
            Payload=json.dumps({"date": day_str}).encode("utf-8"),
        )
        print(f"Triggered async Oura ingest for {day_str}")
    except Exception as e:
        print(f"Could not trigger Oura ingest: {e}")


def apply_live_overlay(latest_date, bullets):
    """Prepend this morning's live Oura readings; label Gold data by its date.

    Returns (data_date, bullets): data_date is what the narrative should treat
    as 'current' — today when the live fetch succeeds, else the Gold date.
    """
    today_str = date.today().isoformat()
    live = _get_live_oura()

    if not live:
        if latest_date != today_str:
            bullets.insert(
                0,
                f"Ring not synced yet this morning — the numbers below describe "
                f"{latest_date}, not today.",
            )
        return latest_date, bullets

    r = live.get("readiness", {})
    s = live.get("sleep", {})
    parts = []
    if r.get("score") is not None:
        parts.append(f"Readiness {r['score']}")
    if s.get("score") is not None:
        parts.append(f"Sleep {s['score']}")
    temp = r.get("temperature_deviation")
    if isinstance(temp, (int, float)):
        parts.append(f"temp deviation {temp:+.2f}C")
    if not parts:
        return latest_date, bullets

    bullets.insert(0, f"THIS MORNING (live, {today_str}): {' | '.join(parts)}")
    if latest_date != today_str:
        _trigger_oura_ingest(today_str)
    return today_str, bullets


def _get_coach_session():
    """Read today's prescribed session from the saved Coach plan on S3."""
    if not GOLD_BUCKET:
        return None
    try:
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key="coach/current_plan.json")
        plan = json.loads(obj["Body"].read().decode("utf-8"))

        generated = date.fromisoformat(plan.get("generated_at", "1970-01-01"))
        if (date.today() - generated).days > 8:
            return None  # plan has lapsed

        today_str = date.today().isoformat()
        for day in plan.get("days", []):
            if day.get("date") == today_str:
                if day.get("intensity") == "none":
                    return f"Rest day. {day.get('why', '')}".strip()
                discipline = day.get("discipline", "").replace("_", " + ").title()
                return (
                    f"{discipline} — {day.get('intensity')} "
                    f"(~{day.get('target_tss', 0):.0f} TSS). {day.get('why', '')}"
                ).strip()
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"Could not load coach plan: {e}")
    return None


def narrate_briefing(data_date, bullets):
    """Turn the data bullets into a short coach-style narrative via Claude.

    Returns None on any failure — the briefing always falls back to the
    plain template so a model/API problem can never break the email.
    """
    client = _get_anthropic_client()
    if client is None or not bullets:
        return None

    today_str = date.today().isoformat()
    facts = "\n".join(f"- {b}" for b in bullets)
    freshness = (
        "The live morning readings and the pipeline data are both current."
        if data_date == today_str
        else (
            f"IMPORTANT: the pipeline data describes {data_date}, not today. "
            "Frame those numbers as what happened then — never present them "
            "as this morning's state."
        )
    )
    prompt = (
        f"Today is {today_str}. Here are the health metrics and context:\n\n"
        f"{facts}\n\n"
        f"Data freshness: {freshness} Bullets marked 'THIS MORNING (live...)' "
        "are real-time readings from the ring and always describe today.\n\n"
        "Write a morning briefing of 3-5 sentences in plain text (no markdown, "
        "no headers, no emoji). Lead with the single most important thing about "
        "today. Use only the numbers provided — do not invent any, and be "
        "explicit about which day any number belongs to. If a prescribed "
        "training session is listed, weave it in as today's plan. End with one "
        "concrete, actionable sentence. Sound like a calm, knowledgeable "
        "coach, not a cheerleader."
    )

    try:
        response = client.messages.create(
            model=NARRATOR_MODEL,
            max_tokens=400,
            system=(
                "You are the narrator for a personal health data platform that "
                "fuses Oura, Peloton, Apple Health, and nutrition data. You write "
                "brief, grounded morning briefings. Accuracy over enthusiasm."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in response.content if b.type == "text").strip()
        return text or None
    except Exception as e:
        print(f"Narration failed, falling back to template: {e}")
        return None


def _get_latest_discovery():
    """Read the latest correlation discovery narrative from S3 (if recent)."""
    if not GOLD_BUCKET:
        return None
    try:
        # List discovery prefixes to find the most recent
        response = s3.list_objects_v2(
            Bucket=GOLD_BUCKET,
            Prefix="discoveries/weekly/",
            Delimiter="/",
        )
        prefixes = response.get("CommonPrefixes", [])
        if not prefixes:
            return None

        # Get most recent date
        dates = []
        for p in prefixes:
            parts = p["Prefix"].rstrip("/").split("/")
            if parts:
                dates.append(parts[-1])
        if not dates:
            return None

        latest_date_str = sorted(dates, reverse=True)[0]

        # Check freshness (< 8 days old)
        from datetime import date as _date
        try:
            run_date = _date.fromisoformat(latest_date_str)
            if (_date.today() - run_date).days > 7:
                return None
        except ValueError:
            return None

        # Load the JSON
        key = f"discoveries/weekly/{latest_date_str}/discoveries.json"
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        narrative = data.get("top_finding_narrative", "")
        if narrative and narrative != "No significant discoveries found in this scan.":
            return narrative
    except Exception as e:
        print(f"Could not load discovery: {e}")
    return None


def publish_briefing(latest_date, bullets, narrative=None):
    """Publish the morning briefing to SNS."""
    if not SNS_TOPIC_ARN:
        print("No SNS_TOPIC_ARN configured, skipping publish")
        return

    is_stale = any("DATA STALE" in b for b in bullets)
    today = datetime.now(timezone.utc).strftime("%A, %b %d")
    subject = (
        f"STALE DATA — Action Needed ({today})"
        if is_stale
        else f"Morning Briefing - {today}"
    )

    body_lines = [
        f"Good morning! Here's your bio-optimization briefing.",
        f"(Latest data: {latest_date})",
        "",
    ]
    if narrative:
        body_lines.extend([narrative, "", "— The numbers —", ""])
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
            is_stale = any("DATA STALE" in b for b in bullets)
            if is_stale:
                narrative = None
            else:
                data_date, bullets = apply_live_overlay(latest_date, bullets)
                narrative = narrate_briefing(data_date, bullets)
            if narrative:
                print(f"Narrative generated ({len(narrative)} chars)")
            publish_briefing(latest_date, bullets, narrative)
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
