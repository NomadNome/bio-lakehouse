{{ config(materialized='view') }}

SELECT
    date,
    readiness_score,
    sleep_score,
    CAST(hrv_balance_score AS integer) AS hrv_balance,
    combined_wellness_score,
    total_output_kj,
    workout_count,
    disciplines,
    readiness_to_output_ratio,
    -- 3-day readiness trend (negative = declining)
    readiness_score - AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS readiness_vs_3day,
    -- Consecutive workout days
    SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END)
        OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS workouts_last_3_days,
    -- Risk assessment
    CASE
        WHEN readiness_score < 65
             AND readiness_score < LAG(readiness_score, 1) OVER (ORDER BY date)
             AND readiness_score < LAG(readiness_score, 2) OVER (ORDER BY date)
            THEN 'high_risk'
        WHEN readiness_score < 70
             AND SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END)
                 OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) >= 3
            THEN 'moderate_risk'
        WHEN readiness_to_output_ratio > 4.0
            THEN 'moderate_risk'
        ELSE 'low_risk'
    END AS overtraining_risk,
    CASE
        WHEN readiness_score < 65
             AND readiness_score < LAG(readiness_score, 1) OVER (ORDER BY date)
             AND readiness_score < LAG(readiness_score, 2) OVER (ORDER BY date)
            THEN 'Readiness declining 3+ days in a row and below 65. Take a rest day.'
        WHEN readiness_score < 70
             AND SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END)
                 OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) >= 3
            THEN 'Low readiness with 3 consecutive workout days. Schedule recovery.'
        WHEN readiness_to_output_ratio > 4.0
            THEN 'Output-to-readiness ratio is very high. You pushed hard despite low recovery.'
        ELSE 'Recovery looks good. Train as planned.'
    END AS risk_guidance
FROM {{ ref('gold_daily_rollup') }}
WHERE readiness_score IS NOT NULL
