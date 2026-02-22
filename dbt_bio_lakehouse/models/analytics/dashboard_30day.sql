{{ config(materialized='view') }}

SELECT
    date,
    readiness_score,
    sleep_score,
    activity_score,
    combined_wellness_score,
    workout_count,
    total_output_kj,
    total_workout_minutes,
    avg_watts,
    max_avg_hr,
    readiness_to_output_ratio,
    had_workout,
    steps,
    active_calories,
    peloton_calories,
    disciplines,
    hk_workout_types,
    -- 7-day rolling averages
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS readiness_7day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sleep_7day_avg,
    AVG(total_output_kj) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS output_7day_avg,
    -- 30-day rolling averages
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS readiness_30day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sleep_30day_avg
FROM {{ ref('gold_daily_rollup') }}
ORDER BY date DESC
