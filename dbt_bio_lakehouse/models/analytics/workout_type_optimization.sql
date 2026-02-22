{{ config(materialized='view') }}

WITH workout_days AS (
    SELECT
        date,
        readiness_score,
        sleep_score,
        CASE
            WHEN readiness_score >= 85 THEN 'High (85+)'
            WHEN readiness_score >= 70 THEN 'Medium (70-84)'
            ELSE 'Low (<70)'
        END AS readiness_bucket,
        CASE
            WHEN disciplines LIKE '%Cycling%' THEN 'Cycling'
            WHEN disciplines LIKE '%Strength%' OR hk_workout_types LIKE '%strength%' THEN 'Strength'
            WHEN hk_workout_types LIKE '%walking%' OR hk_workout_types LIKE '%hiking%' THEN 'Walking'
            WHEN hk_workout_types LIKE '%running%' OR disciplines LIKE '%Bootcamp%' OR hk_workout_types LIKE '%high_intensity%' THEN 'Cardio'
            WHEN disciplines LIKE '%Yoga%' OR disciplines LIKE '%Stretching%' OR disciplines LIKE '%Meditation%'
                 OR hk_workout_types LIKE '%yoga%' OR hk_workout_types LIKE '%flexibility%' OR hk_workout_types LIKE '%pilates%' THEN 'Recovery'
            ELSE COALESCE(NULLIF(disciplines, ''), hk_workout_types, 'Other')
        END AS workout_type,
        total_output_kj,
        avg_watts,
        total_workout_minutes,
        peloton_calories,
        max_avg_hr,
        readiness_to_output_ratio
    FROM {{ ref('gold_daily_rollup') }}
    WHERE had_workout = true
      AND readiness_score IS NOT NULL
)

SELECT
    readiness_bucket,
    workout_type,
    COUNT(*) AS sample_days,
    ROUND(AVG(total_output_kj), 1) AS avg_output_kj,
    ROUND(AVG(avg_watts), 1) AS avg_watts,
    ROUND(AVG(peloton_calories), 0) AS avg_calories,
    ROUND(AVG(total_workout_minutes), 0) AS avg_duration_min,
    ROUND(AVG(max_avg_hr), 0) AS avg_max_hr,
    ROUND(AVG(readiness_to_output_ratio), 2) AS avg_ratio,
    ROUND(AVG(readiness_score), 1) AS avg_readiness_in_bucket,
    ROUND(AVG(sleep_score), 1) AS avg_sleep_in_bucket
FROM workout_days
GROUP BY readiness_bucket, workout_type
HAVING COUNT(*) >= 2
ORDER BY readiness_bucket, avg_output_kj DESC
