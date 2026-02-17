-- Bio Lakehouse - Athena Views and Queries
-- Run these after Gold layer is populated and crawler has run

-- ============================================================
-- View: 30-Day Rolling Dashboard
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.dashboard_30day AS
SELECT
    date,
    readiness_score,
    sleep_score,
    activity_score,
    combined_wellness_score,
    workout_count,
    total_output_kj,
    total_workout_minutes,
    avg_watts,
    max_avg_hr,
    readiness_to_output_ratio,
    had_workout,
    steps,
    active_calories,
    peloton_calories,
    disciplines,
    -- 7-day rolling averages
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS readiness_7day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sleep_7day_avg,
    AVG(total_output_kj) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS output_7day_avg,
    -- 30-day rolling averages
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS readiness_30day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sleep_30day_avg
FROM bio_gold.daily_readiness_performance
ORDER BY date DESC;


-- ============================================================
-- View: Workout Recommendations
-- Based on readiness score trends, suggest workout intensity
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.workout_recommendations AS
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
    -- Previous day workout load
    LAG(total_output_kj, 1) OVER (ORDER BY date) AS prev_day_output_kj,
    LAG(workout_count, 1) OVER (ORDER BY date) AS prev_day_workout_count,
    LAG(total_workout_minutes, 1) OVER (ORDER BY date) AS prev_day_workout_mins
FROM bio_gold.daily_readiness_performance
ORDER BY date DESC;


-- ============================================================
-- Query: Top Performance Days
-- Days with highest output relative to readiness
-- ============================================================
-- SELECT
--     date,
--     readiness_score,
--     sleep_score,
--     total_output_kj,
--     readiness_to_output_ratio,
--     workout_count,
--     disciplines,
--     avg_watts
-- FROM bio_gold.daily_readiness_performance
-- WHERE total_output_kj IS NOT NULL
--   AND readiness_score IS NOT NULL
-- ORDER BY readiness_to_output_ratio DESC
-- LIMIT 20;


-- ============================================================
-- Query: Readiness-Output Correlation Analysis
-- Weekly aggregation for trend analysis
-- ============================================================
-- SELECT
--     date_trunc('week', CAST(date AS DATE)) AS week_start,
--     AVG(readiness_score) AS avg_readiness,
--     AVG(sleep_score) AS avg_sleep,
--     SUM(total_output_kj) AS weekly_output_kj,
--     COUNT(CASE WHEN had_workout THEN 1 END) AS workout_days,
--     AVG(avg_watts) AS avg_watts,
--     CORR(readiness_score, total_output_kj) AS readiness_output_correlation
-- FROM bio_gold.daily_readiness_performance
-- GROUP BY date_trunc('week', CAST(date AS DATE))
-- ORDER BY week_start DESC;


-- ============================================================
-- Query: Sleep Impact on Next-Day Performance
-- ============================================================
-- SELECT
--     a.date,
--     a.sleep_score AS prev_night_sleep,
--     b.readiness_score AS next_day_readiness,
--     b.total_output_kj AS next_day_output,
--     b.avg_watts AS next_day_avg_watts
-- FROM bio_gold.daily_readiness_performance a
-- JOIN bio_gold.daily_readiness_performance b
--     ON CAST(b.date AS DATE) = DATE_ADD('day', 1, CAST(a.date AS DATE))
-- WHERE a.sleep_score IS NOT NULL
--   AND b.total_output_kj IS NOT NULL
-- ORDER BY a.sleep_score DESC;
