{{ config(materialized='view') }}

SELECT
    'All Days' AS segment,
    COUNT(*) AS sample_size,
    ROUND(CORR(readiness_score, total_output_kj), 3) AS readiness_output_corr,
    ROUND(CORR(sleep_score, total_output_kj), 3) AS sleep_output_corr,
    ROUND(CORR(CAST(hrv_balance_score AS double), total_output_kj), 3) AS hrv_output_corr,
    ROUND(CORR(sleep_score, readiness_score), 3) AS sleep_readiness_corr,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    ROUND(AVG(sleep_score), 1) AS avg_sleep,
    ROUND(AVG(total_output_kj), 1) AS avg_output_kj,
    ROUND(AVG(avg_watts), 1) AS avg_watts
FROM {{ ref('gold_daily_rollup') }}
WHERE had_workout = true
  AND total_output_kj > 0
  AND readiness_score IS NOT NULL

UNION ALL

SELECT
    'High Readiness (85+)' AS segment,
    COUNT(*) AS sample_size,
    ROUND(CORR(readiness_score, total_output_kj), 3) AS readiness_output_corr,
    ROUND(CORR(sleep_score, total_output_kj), 3) AS sleep_output_corr,
    ROUND(CORR(CAST(hrv_balance_score AS double), total_output_kj), 3) AS hrv_output_corr,
    ROUND(CORR(sleep_score, readiness_score), 3) AS sleep_readiness_corr,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    ROUND(AVG(sleep_score), 1) AS avg_sleep,
    ROUND(AVG(total_output_kj), 1) AS avg_output_kj,
    ROUND(AVG(avg_watts), 1) AS avg_watts
FROM {{ ref('gold_daily_rollup') }}
WHERE had_workout = true
  AND total_output_kj > 0
  AND readiness_score >= 85

UNION ALL

SELECT
    'Low Readiness (<70)' AS segment,
    COUNT(*) AS sample_size,
    ROUND(CORR(readiness_score, total_output_kj), 3) AS readiness_output_corr,
    ROUND(CORR(sleep_score, total_output_kj), 3) AS sleep_output_corr,
    ROUND(CORR(CAST(hrv_balance_score AS double), total_output_kj), 3) AS hrv_output_corr,
    ROUND(CORR(sleep_score, readiness_score), 3) AS sleep_readiness_corr,
    ROUND(AVG(readiness_score), 1) AS avg_readiness,
    ROUND(AVG(sleep_score), 1) AS avg_sleep,
    ROUND(AVG(total_output_kj), 1) AS avg_output_kj,
    ROUND(AVG(avg_watts), 1) AS avg_watts
FROM {{ ref('gold_daily_rollup') }}
WHERE had_workout = true
  AND total_output_kj > 0
  AND readiness_score < 70
