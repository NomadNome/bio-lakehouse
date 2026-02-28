-- Bio Lakehouse - Athena Views (bio_gold schema)
-- Run these after Gold layer is populated and crawler has run.
--
-- MIGRATION NOTE (2026-02-27):
-- Most analytics views have been ported to dbt models in
-- dbt_bio_lakehouse/models/analytics/. The dbt versions live in
-- bio_gold_gold schema. These manual views remain because they are
-- deployed directly in the bio_gold schema, which existing code
-- (Streamlit, health alerts Lambda, weekly report) references.
--
-- See athena/views_LEGACY.sql for the full original file.
-- Canonical source for all view logic: dbt_bio_lakehouse/models/

-- ============================================================
-- View: 30-Day Rolling Dashboard
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/dashboard_30day.sql
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
    hk_workout_types,
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS readiness_7day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sleep_7day_avg,
    AVG(total_output_kj) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS output_7day_avg,
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS readiness_30day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sleep_30day_avg
FROM bio_gold.daily_readiness_performance
ORDER BY date DESC;


-- ============================================================
-- View: Workout Recommendations
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/workout_recommendations.sql
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
    LAG(total_output_kj, 1) OVER (ORDER BY date) AS prev_day_output_kj,
    LAG(workout_count, 1) OVER (ORDER BY date) AS prev_day_workout_count,
    LAG(total_workout_minutes, 1) OVER (ORDER BY date) AS prev_day_workout_mins
FROM bio_gold.daily_readiness_performance
ORDER BY date DESC;


-- ============================================================
-- View: Energy State Classification
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/energy_state.sql
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
    CASE
        WHEN readiness_to_output_ratio > 4.0 THEN 'overreaching'
        WHEN readiness_to_output_ratio >= 2.5 THEN 'high_performance'
        WHEN readiness_to_output_ratio >= 1.5 THEN 'moderate'
        WHEN readiness_to_output_ratio > 0 THEN 'undertrained'
        ELSE 'no_workout'
    END AS output_zone,
    readiness_to_output_ratio,
    AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS readiness_3day_avg,
    AVG(sleep_score) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sleep_3day_avg,
    readiness_score - LAG(readiness_score, 1) OVER (ORDER BY date) AS readiness_delta,
    sleep_score - LAG(sleep_score, 1) OVER (ORDER BY date) AS sleep_delta
FROM bio_gold.daily_readiness_performance
WHERE readiness_score IS NOT NULL;


-- ============================================================
-- View: Overtraining Risk Monitor
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/overtraining_risk.sql
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
    readiness_score - AVG(readiness_score) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS readiness_vs_3day,
    SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END)
        OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS workouts_last_3_days,
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
-- View: Training Load Daily (TSS)
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/training_load_daily.sql
-- Canonical TSS formula: dbt_bio_lakehouse/macros/tss_calculation.sql
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
    hk_calories,
    hk_workout_minutes,
    CASE
        WHEN had_workout = false THEN 0.0
        WHEN total_output_kj IS NOT NULL AND total_output_kj > 0
            THEN LEAST(300.0, total_output_kj * COALESCE(max_avg_hr, 140) / 600.0)
        WHEN peloton_calories IS NOT NULL AND peloton_calories > 0
             AND total_workout_minutes IS NOT NULL AND total_workout_minutes > 0
            THEN LEAST(300.0, peloton_calories * total_workout_minutes / 150.0)
        WHEN active_calories IS NOT NULL AND active_calories > 0
             AND total_workout_minutes IS NOT NULL AND total_workout_minutes > 0
            THEN LEAST(300.0, active_calories * total_workout_minutes / 450.0)
        WHEN hk_calories IS NOT NULL AND hk_calories > 0
             AND hk_workout_minutes IS NOT NULL AND hk_workout_minutes > 0
            THEN LEAST(300.0, hk_calories * hk_workout_minutes / 450.0)
        WHEN active_calories IS NOT NULL AND active_calories > 0
            THEN LEAST(200.0, active_calories / 12.0)
        ELSE 0.0
    END AS tss
FROM bio_gold.daily_readiness_performance
ORDER BY date;


-- ============================================================
-- View: Temperature Trends
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/temperature_trends.sql
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.temperature_trends AS
SELECT
    date AS day,
    CAST(temperature_deviation AS double) AS temp_deviation,
    CAST(temperature_deviation AS double) - LAG(CAST(temperature_deviation AS double), 1) OVER (ORDER BY date) AS temp_trend_deviation,
    AVG(CAST(temperature_deviation AS double)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_dev_7day_avg,
    CASE
        WHEN ABS(CAST(temperature_deviation AS double)) > 0.5 THEN 'elevated'
        WHEN ABS(CAST(temperature_deviation AS double)) > 0.3 THEN 'mild'
        ELSE 'normal'
    END AS temp_status,
    readiness_score
FROM bio_gold.daily_readiness_performance
WHERE temperature_deviation IS NOT NULL;


-- ============================================================
-- View: Sleep Architecture
-- dbt equivalent: dbt_bio_lakehouse/models/analytics/sleep_architecture.sql
-- ============================================================
CREATE OR REPLACE VIEW bio_gold.sleep_architecture AS
SELECT
    date AS day,
    sleep_score,
    deep_sleep_score AS deep_sleep,
    rem_sleep_score AS rem_sleep
FROM bio_gold.daily_readiness_performance
WHERE deep_sleep_score IS NOT NULL
   OR rem_sleep_score IS NOT NULL;
