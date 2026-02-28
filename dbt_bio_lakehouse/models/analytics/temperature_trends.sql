{{ config(materialized='view') }}

SELECT
    r.date AS day,
    CAST(r.temperature_deviation AS double) AS temp_deviation,
    CAST(r.temperature_deviation AS double) - LAG(CAST(r.temperature_deviation AS double), 1) OVER (ORDER BY r.date) AS temp_trend_deviation,
    AVG(CAST(r.temperature_deviation AS double)) OVER (ORDER BY r.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_dev_7day_avg,
    CASE
        WHEN ABS(CAST(r.temperature_deviation AS double)) > 0.5 THEN 'elevated'
        WHEN ABS(CAST(r.temperature_deviation AS double)) > 0.3 THEN 'mild'
        ELSE 'normal'
    END AS temp_status,
    r.readiness_score
FROM {{ ref('gold_daily_rollup') }} r
WHERE r.temperature_deviation IS NOT NULL
