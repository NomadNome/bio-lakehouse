{{ config(materialized='view') }}

SELECT
    r.date AS day,
    r.temperature_deviation AS temp_deviation,
    r.temperature_deviation - LAG(r.temperature_deviation, 1) OVER (ORDER BY r.date) AS temp_trend_deviation,
    AVG(r.temperature_deviation) OVER (ORDER BY r.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_dev_7day_avg,
    CASE
        WHEN ABS(r.temperature_deviation) > 0.5 THEN 'elevated'
        WHEN ABS(r.temperature_deviation) > 0.3 THEN 'mild'
        ELSE 'normal'
    END AS temp_status,
    r.readiness_score
FROM {{ ref('gold_daily_rollup') }} r
WHERE r.temperature_deviation IS NOT NULL
