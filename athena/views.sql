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
-- View: Energy State Classification
-- Identifies the "125% Energy" optimal state and classifies
-- each day into actionable energy zones
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.energy_state AS
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
            THEN '125% Energy — Peak state. Go all out: HIIT, Tabata, hard cycling, or high-stakes interview prep.'
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
    -- Day-over-day readiness change
    readiness_score - LAG(readiness_score, 1) OVER (ORDER BY date) AS readiness_delta,
    sleep_score - LAG(sleep_score, 1) OVER (ORDER BY date) AS sleep_delta
FROM bio_gold.daily_readiness_performance
WHERE readiness_score IS NOT NULL;


-- ============================================================
-- View: Workout Type Optimization
-- Answers: "Given my readiness level, which workout type
-- historically produces the best output?"
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.workout_type_optimization AS
WITH workout_days AS (
    SELECT
        date,
        readiness_score,
        sleep_score,
        CASE
            WHEN readiness_score >= 85 THEN 'High (85+)'
            WHEN readiness_score >= 70 THEN 'Medium (70-84)'
            ELSE 'Low (<70)'
        END AS readiness_bucket,
        -- Extract primary discipline (first in comma-separated list)
        CASE
            WHEN disciplines LIKE '%Cycling%' AND disciplines NOT LIKE '%,%' THEN 'Cycling Only'
            WHEN disciplines LIKE '%Strength%' AND disciplines NOT LIKE '%,%' THEN 'Strength Only'
            WHEN disciplines LIKE '%Cycling%' AND disciplines LIKE '%Strength%' THEN 'Cycling + Strength'
            WHEN disciplines LIKE '%Yoga%' OR disciplines LIKE '%Stretching%' OR disciplines LIKE '%Meditation%' THEN 'Recovery (Yoga/Stretch/Meditate)'
            WHEN disciplines LIKE '%Cycling%' THEN 'Cycling + Other'
            ELSE disciplines
        END AS workout_type,
        total_output_kj,
        avg_watts,
        total_workout_minutes,
        peloton_calories,
        max_avg_hr,
        readiness_to_output_ratio
    FROM bio_gold.daily_readiness_performance
    WHERE had_workout = true
      AND readiness_score IS NOT NULL
)
SELECT
    readiness_bucket,
    workout_type,
    COUNT(*) AS sample_days,
    ROUND(AVG(total_output_kj), 1) AS avg_output_kj,
    ROUND(AVG(avg_watts), 1) AS avg_watts,
    ROUND(AVG(peloton_calories), 0) AS avg_calories,
    ROUND(AVG(total_workout_minutes), 0) AS avg_duration_min,
    ROUND(AVG(max_avg_hr), 0) AS avg_max_hr,
    ROUND(AVG(readiness_to_output_ratio), 2) AS avg_ratio,
    ROUND(AVG(readiness_score), 1) AS avg_readiness_in_bucket,
    ROUND(AVG(sleep_score), 1) AS avg_sleep_in_bucket
FROM workout_days
GROUP BY readiness_bucket, workout_type
HAVING COUNT(*) >= 2
ORDER BY readiness_bucket, avg_output_kj DESC;


-- ============================================================
-- View: Sleep-to-Performance Prediction
-- Answers: "How does last night's sleep predict today's
-- workout output and readiness?"
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.sleep_performance_prediction AS
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
FROM bio_gold.daily_readiness_performance a
JOIN bio_gold.daily_readiness_performance b
    ON COALESCE(
        TRY(CAST(b.date AS date)),
        TRY(date_parse(b.date, '%Y-%m-%d %H:%i:%s'))
    ) = date_add('day', 1, COALESCE(
        TRY(CAST(a.date AS date)),
        TRY(date_parse(a.date, '%Y-%m-%d %H:%i:%s'))
    ))
WHERE a.sleep_score IS NOT NULL
  AND b.readiness_score IS NOT NULL;


-- ============================================================
-- View: Readiness-Performance Correlation
-- Statistical correlations between recovery metrics and output
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.readiness_performance_correlation AS
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
FROM bio_gold.daily_readiness_performance
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
FROM bio_gold.daily_readiness_performance
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
FROM bio_gold.daily_readiness_performance
WHERE had_workout = true
  AND total_output_kj > 0
  AND readiness_score < 70;


-- ============================================================
-- View: Weekly Performance Trends
-- Week-over-week progression with change indicators
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.weekly_trends AS
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
    FROM bio_gold.daily_readiness_performance
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
ORDER BY week_start DESC;


-- ============================================================
-- View: Overtraining Risk Monitor
-- Flags when you're pushing too hard relative to recovery
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.overtraining_risk AS
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
FROM bio_gold.daily_readiness_performance
WHERE readiness_score IS NOT NULL;


-- ============================================================
-- View: Training Load Daily (TSS / CTL / ATL)
-- Computes Training Stress Score per day using Peloton output
-- or HealthKit calorie fallback. Used for periodization charts.
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.training_load_daily AS
SELECT
    date,
    had_workout,
    total_output_kj,
    total_workout_minutes,
    max_avg_hr,
    peloton_calories,
    active_calories,
    -- Training Stress Score (TSS)
    CASE
        WHEN had_workout = false OR total_workout_minutes IS NULL OR total_workout_minutes = 0
            THEN 0.0
        WHEN total_output_kj IS NOT NULL AND total_output_kj > 0
            -- Peloton-based TSS: (output_kj / 100) * (minutes / 60) * HR_factor, clamped 0-300
            THEN LEAST(300.0, GREATEST(0.0,
                (total_output_kj / 100.0)
                * (total_workout_minutes / 60.0)
                * LEAST(COALESCE(max_avg_hr, 140) / 180.0, 1.5)
            ))
        WHEN COALESCE(peloton_calories, 0) + COALESCE(active_calories, 0) > 0
            -- HealthKit calorie fallback: (calories / 500) * (minutes / 30) * 0.85
            THEN LEAST(300.0, GREATEST(0.0,
                ((COALESCE(peloton_calories, 0) + COALESCE(active_calories, 0)) / 500.0)
                * (total_workout_minutes / 30.0)
                * 0.85
            ))
        ELSE 0.0
    END AS tss
FROM bio_gold.daily_readiness_performance
ORDER BY date;
