{{ config(materialized='view') }}

SELECT
    a.date AS sleep_date,
    b.date AS performance_date,
    a.sleep_score AS prev_night_sleep,
    CASE
        WHEN a.sleep_score >= 88 THEN 'Excellent (88+)'
        WHEN a.sleep_score >= 75 THEN 'Good (75-87)'
        WHEN a.sleep_score >= 60 THEN 'Fair (60-74)'
        ELSE 'Poor (<60)'
    END AS sleep_quality,
    b.readiness_score AS next_day_readiness,
    b.total_output_kj AS next_day_output,
    b.avg_watts AS next_day_avg_watts,
    b.had_workout AS next_day_worked_out,
    b.disciplines AS next_day_disciplines,
    b.combined_wellness_score AS next_day_wellness,
    -- Sleep-to-readiness conversion efficiency
    CASE
        WHEN a.sleep_score > 0
            THEN ROUND(CAST(b.readiness_score AS double) / a.sleep_score, 2)
        ELSE NULL
    END AS sleep_to_readiness_ratio
FROM {{ ref('gold_daily_rollup') }} a
JOIN {{ ref('gold_daily_rollup') }} b
    ON COALESCE(
        TRY(CAST(b.date AS date)),
        TRY(date_parse(b.date, '%Y-%m-%d %H:%i:%s'))
    ) = date_add('day', 1, COALESCE(
        TRY(CAST(a.date AS date)),
        TRY(date_parse(a.date, '%Y-%m-%d %H:%i:%s'))
    ))
WHERE a.sleep_score IS NOT NULL
  AND b.readiness_score IS NOT NULL
