{{ config(materialized='view') }}

SELECT
    date,
    readiness_score,
    sleep_score,
    CAST(hrv_balance_score AS integer) AS hrv_balance,
    activity_score,
    combined_wellness_score,
    had_workout,
    workout_count,
    total_output_kj,
    avg_watts,
    disciplines,
    -- Energy state classification
    CASE
        WHEN readiness_score >= 85 AND sleep_score >= 88
             AND CAST(hrv_balance_score AS integer) >= 75
            THEN 'peak'
        WHEN readiness_score >= 85 AND sleep_score >= 80
            THEN 'high'
        WHEN readiness_score >= 70 AND sleep_score >= 65
            THEN 'moderate'
        WHEN readiness_score >= 50
            THEN 'low'
        ELSE 'recovery_needed'
    END AS energy_state,
    -- Human-readable guidance
    CASE
        WHEN readiness_score >= 85 AND sleep_score >= 88
             AND CAST(hrv_balance_score AS integer) >= 75
            THEN '125% Energy -- Peak state. Go all out: HIIT, Tabata, hard cycling, or high-stakes interview prep.'
        WHEN readiness_score >= 85 AND sleep_score >= 80
            THEN 'High energy. Great for hard cycling, bootcamp, or deep technical study sessions.'
        WHEN readiness_score >= 70 AND sleep_score >= 65
            THEN 'Moderate energy. Good for endurance rides, strength training, or steady interview prep.'
        WHEN readiness_score >= 50
            THEN 'Low energy. Stick to yoga, stretching, or light walk. Avoid draining meetings.'
        ELSE 'Recovery needed. Rest day or gentle meditation. No high-pressure activities.'
    END AS guidance,
    -- Readiness-to-output ratio zone
    CASE
        WHEN readiness_to_output_ratio > 4.0 THEN 'overreaching'
        WHEN readiness_to_output_ratio >= 2.5 THEN 'high_performance'
        WHEN readiness_to_output_ratio >= 1.5 THEN 'moderate'
        WHEN readiness_to_output_ratio > 0 THEN 'undertrained'
        ELSE 'no_workout'
    END AS output_zone,
    readiness_to_output_ratio,
    -- Trailing indicators
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS readiness_3day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sleep_3day_avg,
    -- Day-over-day changes
    readiness_score - LAG(readiness_score, 1) OVER (ORDER BY date) AS readiness_delta,
    sleep_score - LAG(sleep_score, 1) OVER (ORDER BY date) AS sleep_delta
FROM {{ ref('gold_daily_rollup') }}
WHERE readiness_score IS NOT NULL
