{{
  config(
    materialized='table',
    external_location='s3://bio-lakehouse-gold-069899605581/dbt/feature_readiness_daily/',
    format='parquet'
  )
}}

WITH base AS (
    SELECT
        date,
        readiness_score,
        sleep_score,
        deep_sleep_score,
        rem_sleep_score,
        total_sleep_score,
        hrv_balance_score,
        activity_score,
        steps,
        active_calories,
        total_output_kj,
        peloton_calories,
        total_workout_minutes,
        max_avg_hr,
        had_workout,
        hk_calories,
        hk_workout_minutes,
        resting_heart_rate_bpm,
        hrv_ms,
        combined_wellness_score,
        workout_count,
        hk_workout_count
    FROM {{ ref('gold_daily_rollup') }}
    WHERE date IS NOT NULL
),

with_tss AS (
    SELECT
        *,
        {{ tss_calculation() }} AS tss
    FROM base
),

with_features AS (
    SELECT
        date,
        readiness_score,
        sleep_score,
        CAST(deep_sleep_score AS double) AS sleep_deep_pct,
        CAST(rem_sleep_score AS double) AS sleep_rem_pct,
        CAST(total_sleep_score AS double) AS sleep_total_hours,
        CAST(hrv_balance_score AS double) AS hrv_balance_score,
        CAST(resting_heart_rate_bpm AS double) AS resting_hr,
        CASE WHEN had_workout = true THEN 1 ELSE 0 END AS had_workout,
        tss,

        -- Rolling averages
        AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS readiness_7d_avg,
        AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sleep_score_3d_avg,

        -- Readiness 3-day slope (linear approximation)
        (readiness_score - LAG(readiness_score, 2) OVER (ORDER BY date)) / 2.0 AS readiness_3d_slope,

        -- Cumulative TSS
        SUM(tss) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cumulative_tss_3d,
        SUM(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_tss_7d,

        -- CTL (Chronic Training Load — 42-day exponential moving average approximation)
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW) AS ctl,

        -- ATL (Acute Training Load — 7-day average)
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS atl,

        -- TSB (Training Stress Balance = CTL - ATL)
        AVG(tss) OVER (ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW)
            - AVG(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tsb,

        -- Day of week (1=Monday .. 7=Sunday)
        day_of_week(COALESCE(
            TRY(CAST(date AS date)),
            TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
        )) AS day_of_week,

        -- Days since last rest day (consecutive workout days)
        SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END)
            OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS workouts_last_7d,

        -- Next-day readiness (TARGET)
        LEAD(readiness_score, 1) OVER (ORDER BY date) AS next_day_readiness

    FROM with_tss
)

SELECT *
FROM with_features
WHERE readiness_score IS NOT NULL
ORDER BY date
