{{ config(materialized='view') }}

SELECT
    s.date AS day,
    g.sleep_score,
    s.deep_sleep_score AS deep_sleep,
    s.rem_sleep_score AS rem_sleep
FROM {{ ref('stg_sleep') }} s
JOIN {{ ref('gold_daily_rollup') }} g ON s.date = g.date
WHERE s.deep_sleep_score IS NOT NULL
   OR s.rem_sleep_score IS NOT NULL
