{{ config(materialized='view') }}

WITH weekly AS (
    SELECT
        date_trunc('week', COALESCE(
            TRY(CAST(date AS date)),
            TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
        )) AS week_start,
        AVG(readiness_score) AS avg_readiness,
        AVG(sleep_score) AS avg_sleep,
        AVG(combined_wellness_score) AS avg_wellness,
        SUM(total_output_kj) AS weekly_output_kj,
        SUM(peloton_calories) AS weekly_calories,
        COUNT(CASE WHEN had_workout = true THEN 1 END) AS workout_days,
        AVG(CASE WHEN had_workout = true THEN avg_watts END) AS avg_watts,
        AVG(CASE WHEN had_workout = true THEN max_avg_hr END) AS avg_max_hr,
        SUM(steps) AS weekly_steps,
        SUM(active_calories) AS weekly_active_cal
    FROM {{ ref('gold_daily_rollup') }}
    WHERE readiness_score IS NOT NULL
    GROUP BY date_trunc('week', COALESCE(
        TRY(CAST(date AS date)),
        TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
    ))
)

SELECT
    week_start,
    ROUND(avg_readiness, 1) AS avg_readiness,
    ROUND(avg_sleep, 1) AS avg_sleep,
    ROUND(avg_wellness, 1) AS avg_wellness,
    ROUND(weekly_output_kj, 1) AS weekly_output_kj,
    weekly_calories,
    workout_days,
    ROUND(avg_watts, 1) AS avg_watts,
    ROUND(avg_max_hr, 0) AS avg_max_hr,
    weekly_steps,
    weekly_active_cal,
    -- Week-over-week changes
    ROUND(avg_readiness - LAG(avg_readiness) OVER (ORDER BY week_start), 1) AS readiness_change,
    ROUND(avg_sleep - LAG(avg_sleep) OVER (ORDER BY week_start), 1) AS sleep_change,
    ROUND(weekly_output_kj - LAG(weekly_output_kj) OVER (ORDER BY week_start), 1) AS output_change,
    workout_days - LAG(workout_days) OVER (ORDER BY week_start) AS workout_days_change,
    -- Trend indicator
    CASE
        WHEN avg_readiness > LAG(avg_readiness) OVER (ORDER BY week_start)
             AND weekly_output_kj > LAG(weekly_output_kj) OVER (ORDER BY week_start)
            THEN 'improving'
        WHEN avg_readiness < LAG(avg_readiness) OVER (ORDER BY week_start)
             AND weekly_output_kj < LAG(weekly_output_kj) OVER (ORDER BY week_start)
            THEN 'declining'
        WHEN avg_readiness < LAG(avg_readiness) OVER (ORDER BY week_start)
             AND weekly_output_kj > LAG(weekly_output_kj) OVER (ORDER BY week_start)
            THEN 'overreaching'
        WHEN avg_readiness > LAG(avg_readiness) OVER (ORDER BY week_start)
             AND weekly_output_kj < LAG(weekly_output_kj) OVER (ORDER BY week_start)
            THEN 'recovering'
        ELSE 'stable'
    END AS trend
FROM weekly
ORDER BY week_start DESC
