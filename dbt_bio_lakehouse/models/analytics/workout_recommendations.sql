{{ config(materialized='view') }}

SELECT
    date,
    readiness_score,
    sleep_score,
    combined_wellness_score,
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS readiness_3day_avg,
    CASE
        WHEN readiness_score >= 85 AND sleep_score >= 80
            THEN 'high_intensity'
        WHEN readiness_score >= 70 AND sleep_score >= 65
            THEN 'moderate_intensity'
        WHEN readiness_score >= 50
            THEN 'low_intensity'
        ELSE 'rest_day'
    END AS recommended_intensity,
    CASE
        WHEN readiness_score >= 85 AND sleep_score >= 80
            THEN 'Great recovery! Go for a hard cycling or bootcamp session.'
        WHEN readiness_score >= 70 AND sleep_score >= 65
            THEN 'Decent recovery. Moderate ride or strength training recommended.'
        WHEN readiness_score >= 50
            THEN 'Below average recovery. Stick to yoga, stretching, or light walk.'
        ELSE 'Poor recovery. Consider a rest day or gentle meditation.'
    END AS recommendation_text,
    LAG(total_output_kj, 1) OVER (ORDER BY date) AS prev_day_output_kj,
    LAG(workout_count, 1) OVER (ORDER BY date) AS prev_day_workout_count,
    LAG(total_workout_minutes, 1) OVER (ORDER BY date) AS prev_day_workout_mins
FROM {{ ref('gold_daily_rollup') }}
ORDER BY date DESC
