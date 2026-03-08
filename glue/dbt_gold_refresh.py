"""
Bio Lakehouse - Gold Layer Refresh (Glue Python Shell Job)

Replaces the manual `dbt run` step. Drops and recreates the three
dbt-managed Gold tables via Athena CTAS queries:
  1. gold_daily_rollup
  2. feature_readiness_daily
  3. workout_recovery_windows

Triggered automatically after the Oura/Peloton/HealthKit normalizers complete.

Glue job arguments:
  --gold_bucket: Gold S3 bucket name
  --athena_results_bucket: Athena query results bucket
"""

import sys
import time

import boto3

# ── Parse arguments ─────────────────────────────────────────────────────
# Python Shell jobs don't have awsglue.utils, so parse args manually
def get_args(argv, keys):
    args = {}
    for i, arg in enumerate(argv):
        for key in keys:
            if arg == f"--{key}" and i + 1 < len(argv):
                args[key] = argv[i + 1]
    return args

args = get_args(sys.argv, ["gold_bucket", "athena_results_bucket"])
_acct = boto3.client("sts").get_caller_identity()["Account"]
GOLD_BUCKET = args.get("gold_bucket", f"bio-lakehouse-gold-{_acct}")
ATHENA_RESULTS = f"s3://{args.get('athena_results_bucket', f'bio-lakehouse-athena-results-{_acct}')}/"
DATABASE = "bio_gold"

athena = boto3.client("athena")
s3 = boto3.client("s3")


def run_query(sql, description=""):
    """Execute an Athena query and wait for completion."""
    print(f"Running: {description}")
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            print(f"  {description}: SUCCEEDED")
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"{description} failed: {reason}")
        time.sleep(2)


def clear_s3_prefix(prefix):
    """Delete all objects under a prefix (needed before CTAS)."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=GOLD_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
    print(f"  Cleared s3://{GOLD_BUCKET}/{prefix}")


def refresh_table(table_name, s3_prefix, select_sql, partition_by=None):
    """Drop, clear S3, and recreate a table via CTAS."""
    # Drop existing table
    run_query(f"DROP TABLE IF EXISTS {table_name}", f"DROP {table_name}")

    # Clear S3 data (Athena CTAS fails if path has existing data)
    clear_s3_prefix(s3_prefix)

    # Build CTAS
    props = [
        f"external_location = 's3://{GOLD_BUCKET}/{s3_prefix}'",
        "format = 'PARQUET'",
    ]
    if partition_by:
        props.append(f"partitioned_by = ARRAY{partition_by}")

    ctas = f"""
    CREATE TABLE {table_name}
    WITH ({', '.join(props)})
    AS
    {select_sql}
    """
    run_query(ctas, f"CREATE {table_name}")


# ── SQL for each table (compiled from dbt models) ──────────────────────

GOLD_DAILY_ROLLUP_SQL = """
WITH readiness AS (
    SELECT * FROM bio_gold_silver."stg_readiness"
),
sleep AS (
    SELECT * FROM bio_gold_silver."stg_sleep"
),
activity AS (
    SELECT day AS date, score AS activity_score, active_calories, steps, total_calories
    FROM bio_silver."oura_daily_activity"
),
peloton AS (
    SELECT * FROM bio_gold_silver."stg_peloton_workouts"
),
hk_vitals AS (
    SELECT date,
        LAST_VALUE(resting_heart_rate_bpm) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS resting_heart_rate_bpm,
        LAST_VALUE(hrv_ms) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS hrv_ms,
        LAST_VALUE(vo2_max) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS vo2_max,
        AVG(blood_oxygen_pct) OVER (PARTITION BY date) AS blood_oxygen_pct,
        AVG(respiratory_rate) OVER (PARTITION BY date) AS respiratory_rate,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS rn
    FROM bio_silver."healthkit_daily_vitals"
),
hk_vitals_deduped AS (
    SELECT date, resting_heart_rate_bpm, hrv_ms, vo2_max, blood_oxygen_pct, respiratory_rate
    FROM hk_vitals WHERE rn = 1
),
hk_workouts AS (
    SELECT * FROM bio_gold_silver."stg_healthkit_workouts"
),
hk_body AS (
    SELECT date,
        LAST_VALUE(weight_lbs) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS weight_lbs,
        LAST_VALUE(body_fat_pct) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS body_fat_pct,
        LAST_VALUE(bmi) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS bmi,
        LAST_VALUE(lean_body_mass_lbs) IGNORE NULLS OVER (PARTITION BY date ORDER BY date) AS lean_body_mass_lbs,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS rn
    FROM bio_silver."healthkit_body"
),
hk_body_deduped AS (
    SELECT date, weight_lbs, body_fat_pct, bmi, lean_body_mass_lbs
    FROM hk_body WHERE rn = 1
),
hk_mindfulness AS (
    SELECT date, SUM(duration_minutes) AS mindfulness_minutes, SUM(session_count) AS mindfulness_session_count
    FROM bio_silver."healthkit_mindfulness"
    GROUP BY date
),
nutrition AS (
    SELECT date, calories AS daily_calories, protein_g, carbohydrates_g AS carbs_g,
        fat_g, fiber_g, sugar_g, sodium_mg, cholesterol_mg,
        protein_pct, carb_pct, fat_pct, meal_count, meals_logged
    FROM bio_silver."mfp_daily_nutrition"
),
joined AS (
    SELECT
        COALESCE(r.date, s.date, a.date, p.date, v.date) AS date,
        r.readiness_score, r.hrv_balance_score, r.resting_hr_score,
        r.previous_night_score, r.recovery_index_score, r.temperature_deviation,
        s.sleep_score, s.deep_sleep_score, s.rem_sleep_score,
        s.sleep_efficiency_score, s.total_sleep_score,
        a.activity_score, a.active_calories, a.steps, a.total_calories,
        p.workout_count, p.total_output_kj, p.peloton_calories, p.avg_watts,
        p.max_avg_hr, p.avg_output_per_minute, p.workout_categories,
        p.total_workout_minutes, p.disciplines,
        v.resting_heart_rate_bpm, v.hrv_ms, v.vo2_max, v.blood_oxygen_pct, v.respiratory_rate,
        hw.hk_workout_count, hw.hk_calories, hw.hk_workout_minutes,
        hw.hk_workout_categories, hw.hk_workout_types,
        b.weight_lbs, b.body_fat_pct, b.bmi, b.lean_body_mass_lbs,
        m.mindfulness_minutes, m.mindfulness_session_count,
        n.daily_calories, n.protein_g, n.carbs_g, n.fat_g, n.fiber_g,
        n.sugar_g, n.sodium_mg, n.protein_pct, n.carb_pct, n.fat_pct, n.meal_count
    FROM readiness r
    FULL OUTER JOIN sleep s ON r.date = s.date
    FULL OUTER JOIN activity a ON COALESCE(r.date, s.date) = a.date
    FULL OUTER JOIN peloton p ON COALESCE(r.date, s.date, a.date) = p.date
    FULL OUTER JOIN hk_vitals_deduped v ON COALESCE(r.date, s.date, a.date, p.date) = v.date
    LEFT JOIN hk_workouts hw ON COALESCE(r.date, s.date, a.date, p.date, v.date) = hw.date
    LEFT JOIN hk_body_deduped b ON COALESCE(r.date, s.date, a.date, p.date, v.date) = b.date
    LEFT JOIN hk_mindfulness m ON COALESCE(r.date, s.date, a.date, p.date, v.date) = m.date
    LEFT JOIN nutrition n ON COALESCE(r.date, s.date, a.date, p.date, v.date) = n.date
),
with_derived AS (
    SELECT *,
        CASE WHEN readiness_score IS NOT NULL AND readiness_score > 0 AND total_output_kj IS NOT NULL
            THEN ROUND(CAST(total_output_kj AS double) / readiness_score, 2) ELSE NULL
        END AS readiness_to_output_ratio,
        CASE WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
            THEN ROUND(readiness_score * 0.6 + sleep_score * 0.4, 1)
            ELSE COALESCE(readiness_score, sleep_score)
        END AS combined_wellness_score,
        COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) AS total_workout_count,
        COALESCE(peloton_calories, 0) + COALESCE(hk_calories, 0) AS total_calories_all_sources,
        COALESCE(total_workout_minutes, 0) + COALESCE(hk_workout_minutes, 0) AS total_workout_minutes_all,
        CASE WHEN COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) > 0 THEN true ELSE false END AS had_workout,
        CASE
            WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
                 AND mindfulness_minutes IS NOT NULL AND mindfulness_minutes > 0
                THEN LEAST(ROUND(readiness_score * 0.6 + sleep_score * 0.4 + LEAST(mindfulness_minutes / 10.0, 3.0), 1), 100.0)
            WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
                THEN ROUND(readiness_score * 0.6 + sleep_score * 0.4, 1)
            ELSE COALESCE(readiness_score, sleep_score)
        END AS mindfulness_adjusted_wellness,
        CASE WHEN protein_g IS NOT NULL AND weight_lbs IS NOT NULL AND weight_lbs > 0
            THEN ROUND(CAST(protein_g AS double) / weight_lbs, 2) ELSE NULL
        END AS protein_per_lb,
        SUBSTRING(date, 1, 4) AS year,
        SUBSTRING(date, 6, 2) AS month
    FROM joined
)
SELECT * FROM with_derived WHERE date IS NOT NULL
"""

FEATURE_READINESS_SQL = """
WITH base AS (
    SELECT date, readiness_score, sleep_score, deep_sleep_score, rem_sleep_score,
        total_sleep_score, hrv_balance_score, activity_score, steps, active_calories,
        total_output_kj, peloton_calories, total_workout_minutes, max_avg_hr,
        had_workout, hk_calories, hk_workout_minutes, resting_heart_rate_bpm,
        hrv_ms, combined_wellness_score, workout_count, hk_workout_count
    FROM bio_gold."gold_daily_rollup"
    WHERE date IS NOT NULL
),
with_tss AS (
    SELECT *,
        CASE
            WHEN had_workout = false THEN 0.0
            WHEN total_output_kj IS NOT NULL AND total_output_kj > 0
                THEN LEAST(300.0, total_output_kj * COALESCE(max_avg_hr, 140) / 600.0)
            WHEN peloton_calories IS NOT NULL AND peloton_calories > 0
                 AND total_workout_minutes IS NOT NULL AND total_workout_minutes > 0
                THEN LEAST(300.0, peloton_calories * total_workout_minutes / 150.0)
            WHEN active_calories IS NOT NULL AND active_calories > 0
                 AND total_workout_minutes IS NOT NULL AND total_workout_minutes > 0
                THEN LEAST(300.0, active_calories * total_workout_minutes / 450.0)
            WHEN hk_calories IS NOT NULL AND hk_calories > 0
                 AND hk_workout_minutes IS NOT NULL AND hk_workout_minutes > 0
                THEN LEAST(300.0, hk_calories * hk_workout_minutes / 450.0)
            WHEN active_calories IS NOT NULL AND active_calories > 0
                THEN LEAST(200.0, active_calories / 12.0)
            ELSE 0.0
        END AS tss
    FROM base
),
with_features AS (
    SELECT date, readiness_score, sleep_score,
        CAST(deep_sleep_score AS double) AS deep_sleep_score,
        CAST(rem_sleep_score AS double) AS rem_sleep_score,
        CAST(total_sleep_score AS double) AS total_sleep_score,
        CAST(hrv_balance_score AS double) AS hrv_balance_score,
        CAST(resting_heart_rate_bpm AS double) AS resting_hr,
        CAST(hrv_ms AS double) AS hrv_ms,
        CASE WHEN had_workout = true THEN 1 ELSE 0 END AS had_workout,
        tss,
        AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS readiness_7d_avg,
        AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sleep_score_3d_avg,
        (readiness_score - LAG(readiness_score, 2) OVER (ORDER BY date)) / 2.0 AS readiness_3d_slope,
        SUM(tss) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cumulative_tss_3d,
        SUM(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_tss_7d,
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW) AS ctl,
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS atl,
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW)
            - AVG(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tsb,
        day_of_week(COALESCE(TRY(CAST(date AS date)), TRY(date_parse(date, '%Y-%m-%d %H:%i:%s')))) AS day_of_week,
        SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS workouts_last_7d,
        AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sleep_baseline_14d,
        sleep_score - AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sleep_deficit_daily,
        CAST(hrv_ms AS double) - LAG(CAST(hrv_ms AS double), 2) OVER (ORDER BY date) AS hrv_2day_change,
        LEAD(readiness_score, 1) OVER (ORDER BY date) AS next_day_readiness
    FROM with_tss
),
with_derived AS (
    SELECT *,
        SUM(sleep_deficit_daily) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sleep_debt_7d,
        CASE WHEN hrv_2day_change > 10 THEN 'rising' WHEN hrv_2day_change < -10 THEN 'falling' ELSE 'stable' END AS hrv_velocity_flag
    FROM with_features
)
SELECT * FROM with_derived WHERE readiness_score IS NOT NULL ORDER BY date
"""

RECOVERY_WINDOWS_SQL = """
WITH daily AS (
    SELECT date, readiness_score, sleep_score,
        COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) AS total_workouts,
        COALESCE(total_workout_minutes, 0) + COALESCE(hk_workout_minutes, 0) AS total_minutes,
        COALESCE(peloton_calories, 0) + COALESCE(hk_calories, 0) AS total_calories,
        total_output_kj, max_avg_hr,
        CASE WHEN had_workout = true THEN 1 ELSE 0 END AS had_workout,
        LEAD(readiness_score, 1) OVER (ORDER BY date) AS readiness_d1,
        LEAD(readiness_score, 2) OVER (ORDER BY date) AS readiness_d2,
        LEAD(readiness_score, 3) OVER (ORDER BY date) AS readiness_d3,
        LEAD(sleep_score, 1) OVER (ORDER BY date) AS sleep_d1,
        LEAD(sleep_score, 2) OVER (ORDER BY date) AS sleep_d2,
        LAG(readiness_score, 1) OVER (ORDER BY date) AS readiness_prev,
        AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS readiness_7d_baseline
    FROM bio_gold."gold_daily_rollup"
    WHERE date IS NOT NULL
),
workout_days AS (
    SELECT date AS workout_date, readiness_score AS workout_day_readiness,
        sleep_score AS workout_day_sleep, total_workouts, total_minutes, total_calories,
        total_output_kj, max_avg_hr, readiness_7d_baseline,
        CASE
            WHEN total_output_kj >= 300 OR total_calories >= 400 OR total_minutes >= 60 THEN 'high'
            WHEN total_output_kj >= 150 OR total_calories >= 200 OR total_minutes >= 30 THEN 'moderate'
            ELSE 'light'
        END AS intensity,
        readiness_d1, readiness_d2, readiness_d3, sleep_d1, sleep_d2,
        readiness_d1 - readiness_score AS readiness_delta_d1,
        readiness_d2 - readiness_score AS readiness_delta_d2,
        readiness_d3 - readiness_score AS readiness_delta_d3,
        CASE
            WHEN readiness_d1 >= readiness_7d_baseline THEN 1
            WHEN readiness_d2 >= readiness_7d_baseline THEN 2
            WHEN readiness_d3 >= readiness_7d_baseline THEN 3
            ELSE NULL
        END AS days_to_recover
    FROM daily
    WHERE had_workout = 1 AND readiness_score IS NOT NULL
)
SELECT * FROM workout_days ORDER BY workout_date
"""


# ── Main execution ──────────────────────────────────────────────────────
print("=" * 60)
print("Gold Layer Refresh — dbt table rebuild")
print("=" * 60)

# Table 1: gold_daily_rollup (partitioned by year, month)
refresh_table(
    "gold_daily_rollup",
    "dbt/daily_readiness_performance/",
    GOLD_DAILY_ROLLUP_SQL,
    partition_by="['year', 'month']",
)

# Table 2: feature_readiness_daily (no partitions)
refresh_table(
    "feature_readiness_daily",
    "dbt/feature_readiness_daily/",
    FEATURE_READINESS_SQL,
)

# Table 3: workout_recovery_windows (no partitions)
refresh_table(
    "workout_recovery_windows",
    "dbt/workout_recovery_windows/",
    RECOVERY_WINDOWS_SQL,
)

print("\nGold layer refresh complete!")
