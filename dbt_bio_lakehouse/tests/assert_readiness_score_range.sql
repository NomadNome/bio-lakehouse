-- Verify all readiness scores are within valid range (0-100)
SELECT *
FROM {{ ref('gold_daily_rollup') }}
WHERE readiness_score IS NOT NULL
  AND (readiness_score < 0 OR readiness_score > 100)
