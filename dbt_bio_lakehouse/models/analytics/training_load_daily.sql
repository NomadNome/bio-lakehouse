{{ config(materialized='view') }}

SELECT
    date,
    had_workout,
    total_output_kj,
    total_workout_minutes,
    max_avg_hr,
    peloton_calories,
    active_calories,
    hk_calories,
    hk_workout_minutes,
    {{ tss_calculation() }} AS tss
FROM {{ ref('gold_daily_rollup') }}
ORDER BY date
