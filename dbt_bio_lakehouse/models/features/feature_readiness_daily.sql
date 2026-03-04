{{
  config(
    materialized='table',
    external_location='s3://' ~ var('gold_bucket') ~ '/dbt/feature_readiness_daily/',
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
        -- Sleep architecture: Oura contributor scores (0-100), not raw durations
        CAST(deep_sleep_score AS double) AS deep_sleep_score,
        CAST(rem_sleep_score AS double) AS rem_sleep_score,
        CAST(total_sleep_score AS double) AS total_sleep_score,
        CAST(hrv_balance_score AS double) AS hrv_balance_score,
        CAST(resting_heart_rate_bpm AS double) AS resting_hr,
        CAST(hrv_ms AS double) AS hrv_ms,
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

        -- Sleep Debt: 14-day baseline and daily deficit (computed in this CTE)
        AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sleep_baseline_14d,
        sleep_score - AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sleep_deficit_daily,

        -- HRV Velocity: 2-day change
        CAST(hrv_ms AS double) - LAG(CAST(hrv_ms AS double), 2) OVER (ORDER BY date) AS hrv_2day_change,

        -- Next-day readiness (TARGET)
        LEAD(readiness_score, 1) OVER (ORDER BY date) AS next_day_readiness

    FROM with_tss
),

-- Second pass: compute rolling sleep debt (needs sleep_deficit_daily from prior CTE)
-- and HRV velocity flag (needs hrv_2day_change from prior CTE)
with_derived AS (
    SELECT
        *,
        -- 7-day rolling sum of daily sleep deficits (negative = accumulating debt)
        SUM(sleep_deficit_daily) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sleep_debt_7d,

        -- HRV velocity flag: significant 2-day change
        CASE
            WHEN hrv_2day_change > 10 THEN 'rising'
            WHEN hrv_2day_change < -10 THEN 'falling'
            ELSE 'stable'
        END AS hrv_velocity_flag

    FROM with_features
)

SELECT *
FROM with_derived
WHERE readiness_score IS NOT NULL
ORDER BY date
