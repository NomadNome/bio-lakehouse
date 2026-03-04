{{
  config(
    materialized='table',
    external_location='s3://' ~ var('gold_bucket') ~ '/dbt/workout_recovery_windows/',
    format='parquet'
  )
}}

-- Recovery Window Analysis: For each workout day, track how readiness
-- and sleep scores change over the following 1-3 days to estimate
-- recovery duration and pattern.

WITH daily AS (
    SELECT
        date,
        readiness_score,
        sleep_score,
        COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) AS total_workouts,
        COALESCE(total_workout_minutes, 0) + COALESCE(hk_workout_minutes, 0) AS total_minutes,
        COALESCE(peloton_calories, 0) + COALESCE(hk_calories, 0) AS total_calories,
        total_output_kj,
        max_avg_hr,
        CASE WHEN had_workout = true THEN 1 ELSE 0 END AS had_workout,
        -- Next-day scores
        LEAD(readiness_score, 1) OVER (ORDER BY date) AS readiness_d1,
        LEAD(readiness_score, 2) OVER (ORDER BY date) AS readiness_d2,
        LEAD(readiness_score, 3) OVER (ORDER BY date) AS readiness_d3,
        LEAD(sleep_score, 1) OVER (ORDER BY date) AS sleep_d1,
        LEAD(sleep_score, 2) OVER (ORDER BY date) AS sleep_d2,
        -- Previous-day baseline
        LAG(readiness_score, 1) OVER (ORDER BY date) AS readiness_prev,
        -- 7-day readiness average (as baseline)
        AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS readiness_7d_baseline
    FROM {{ ref('gold_daily_rollup') }}
    WHERE date IS NOT NULL
),

workout_days AS (
    SELECT
        date AS workout_date,
        readiness_score AS workout_day_readiness,
        sleep_score AS workout_day_sleep,
        total_workouts,
        total_minutes,
        total_calories,
        total_output_kj,
        max_avg_hr,
        readiness_7d_baseline,

        -- Classify workout intensity
        CASE
            WHEN total_output_kj >= 300 OR total_calories >= 400 OR total_minutes >= 60 THEN 'high'
            WHEN total_output_kj >= 150 OR total_calories >= 200 OR total_minutes >= 30 THEN 'moderate'
            ELSE 'light'
        END AS intensity,

        -- Recovery trajectory
        readiness_d1,
        readiness_d2,
        readiness_d3,
        sleep_d1,
        sleep_d2,

        -- Readiness change from workout day
        readiness_d1 - readiness_score AS readiness_delta_d1,
        readiness_d2 - readiness_score AS readiness_delta_d2,
        readiness_d3 - readiness_score AS readiness_delta_d3,

        -- Recovery = when readiness returns to or exceeds baseline
        CASE
            WHEN readiness_d1 >= readiness_7d_baseline THEN 1
            WHEN readiness_d2 >= readiness_7d_baseline THEN 2
            WHEN readiness_d3 >= readiness_7d_baseline THEN 3
            ELSE NULL  -- not recovered within 3 days
        END AS days_to_recover

    FROM daily
    WHERE had_workout = 1
      AND readiness_score IS NOT NULL
)

SELECT *
FROM workout_days
ORDER BY workout_date
