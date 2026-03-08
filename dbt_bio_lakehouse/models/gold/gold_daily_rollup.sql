{{
  config(
    materialized='table',
    external_location='s3://' ~ var('gold_bucket') ~ '/dbt/daily_readiness_performance/',
    format='parquet',
    partitioned_by=['year', 'month']
  )
}}

WITH readiness AS (
    SELECT * FROM {{ ref('stg_readiness') }}
),

sleep AS (
    SELECT * FROM {{ ref('stg_sleep') }}
),

activity AS (
    SELECT
        day AS date,
        score AS activity_score,
        active_calories,
        steps,
        total_calories
    FROM {{ source('bio_silver', 'oura_daily_activity') }}
),

peloton AS (
    SELECT * FROM {{ ref('stg_peloton_workouts') }}
),

hk_vitals AS (
    SELECT
        date,
        LAST_VALUE(resting_heart_rate_bpm) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS resting_heart_rate_bpm,
        LAST_VALUE(hrv_ms) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS hrv_ms,
        LAST_VALUE(vo2_max) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS vo2_max,
        AVG(blood_oxygen_pct) OVER (PARTITION BY date) AS blood_oxygen_pct,
        AVG(respiratory_rate) OVER (PARTITION BY date) AS respiratory_rate,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS rn
    FROM {{ source('bio_silver', 'healthkit_daily_vitals') }}
),

hk_vitals_deduped AS (
    SELECT date, resting_heart_rate_bpm, hrv_ms, vo2_max, blood_oxygen_pct, respiratory_rate
    FROM hk_vitals
    WHERE rn = 1
),

hk_workouts AS (
    SELECT * FROM {{ ref('stg_healthkit_workouts') }}
),

hk_body AS (
    SELECT
        date,
        LAST_VALUE(weight_lbs) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS weight_lbs,
        LAST_VALUE(body_fat_pct) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS body_fat_pct,
        LAST_VALUE(bmi) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS bmi,
        LAST_VALUE(lean_body_mass_lbs) IGNORE NULLS
            OVER (PARTITION BY date ORDER BY date) AS lean_body_mass_lbs,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS rn
    FROM {{ source('bio_silver', 'healthkit_body') }}
),

hk_body_deduped AS (
    SELECT date, weight_lbs, body_fat_pct, bmi, lean_body_mass_lbs
    FROM hk_body
    WHERE rn = 1
),

hk_mindfulness AS (
    SELECT
        date,
        SUM(duration_minutes) AS mindfulness_minutes,
        SUM(session_count) AS mindfulness_session_count
    FROM {{ source('bio_silver', 'healthkit_mindfulness') }}
    GROUP BY date
),

nutrition AS (
    SELECT * FROM {{ ref('stg_nutrition') }}
),

joined AS (
    SELECT
        COALESCE(r.date, s.date, a.date, p.date, v.date) AS date,
        r.readiness_score,
        r.hrv_balance_score,
        r.resting_hr_score,
        r.previous_night_score,
        r.recovery_index_score,
        r.temperature_deviation,
        s.sleep_score,
        s.deep_sleep_score,
        s.rem_sleep_score,
        s.sleep_efficiency_score,
        s.total_sleep_score,
        a.activity_score,
        a.active_calories,
        a.steps,
        a.total_calories,
        p.workout_count,
        p.total_output_kj,
        p.peloton_calories,
        p.avg_watts,
        p.max_avg_hr,
        p.avg_output_per_minute,
        p.workout_categories,
        p.total_workout_minutes,
        p.disciplines,
        v.resting_heart_rate_bpm,
        v.hrv_ms,
        v.vo2_max,
        v.blood_oxygen_pct,
        v.respiratory_rate,
        hw.hk_workout_count,
        hw.hk_calories,
        hw.hk_workout_minutes,
        hw.hk_workout_categories,
        hw.hk_workout_types,
        b.weight_lbs,
        b.body_fat_pct,
        b.bmi,
        b.lean_body_mass_lbs,
        m.mindfulness_minutes,
        m.mindfulness_session_count,
        n.daily_calories,
        n.protein_g,
        n.carbs_g,
        n.fat_g,
        n.fiber_g,
        n.sugar_g,
        n.sodium_mg,
        n.protein_pct,
        n.carb_pct,
        n.fat_pct,
        n.meal_count
    FROM readiness r
    FULL OUTER JOIN sleep s ON r.date = s.date
    FULL OUTER JOIN activity a ON COALESCE(r.date, s.date) = a.date
    FULL OUTER JOIN peloton p ON COALESCE(r.date, s.date, a.date) = p.date
    FULL OUTER JOIN hk_vitals_deduped v ON COALESCE(r.date, s.date, a.date, p.date) = v.date
    LEFT JOIN hk_workouts hw ON COALESCE(r.date, s.date, a.date, p.date, v.date) = hw.date
    LEFT JOIN hk_body_deduped b ON COALESCE(r.date, s.date, a.date, p.date, v.date) = b.date
    LEFT JOIN hk_mindfulness m ON COALESCE(r.date, s.date, a.date, p.date, v.date) = m.date
    LEFT JOIN nutrition n ON COALESCE(r.date, s.date, a.date, p.date, v.date) = n.date
),

with_derived AS (
    SELECT
        *,
        -- Readiness-to-output ratio
        CASE
            WHEN readiness_score IS NOT NULL AND readiness_score > 0 AND total_output_kj IS NOT NULL
                THEN ROUND(CAST(total_output_kj AS double) / readiness_score, 2)
            ELSE NULL
        END AS readiness_to_output_ratio,

        -- Combined wellness score (60% readiness + 40% sleep)
        CASE
            WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
                THEN ROUND(readiness_score * 0.6 + sleep_score * 0.4, 1)
            ELSE COALESCE(readiness_score, sleep_score)
        END AS combined_wellness_score,

        -- Total workout count (Peloton + HealthKit)
        COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) AS total_workout_count,

        -- Total calories from all workout sources
        COALESCE(peloton_calories, 0) + COALESCE(hk_calories, 0) AS total_calories_all_sources,

        -- Total workout minutes from all sources
        COALESCE(total_workout_minutes, 0) + COALESCE(hk_workout_minutes, 0) AS total_workout_minutes_all,

        -- Had workout flag
        CASE
            WHEN COALESCE(workout_count, 0) + COALESCE(hk_workout_count, 0) > 0 THEN true
            ELSE false
        END AS had_workout,

        -- Mindfulness-adjusted wellness
        CASE
            WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
                 AND mindfulness_minutes IS NOT NULL AND mindfulness_minutes > 0
                THEN LEAST(
                    ROUND(readiness_score * 0.6 + sleep_score * 0.4 + LEAST(mindfulness_minutes / 10.0, 3.0), 1),
                    100.0
                )
            WHEN readiness_score IS NOT NULL AND sleep_score IS NOT NULL
                THEN ROUND(readiness_score * 0.6 + sleep_score * 0.4, 1)
            ELSE COALESCE(readiness_score, sleep_score)
        END AS mindfulness_adjusted_wellness,

        -- Protein per pound of body weight
        CASE
            WHEN protein_g IS NOT NULL AND weight_lbs IS NOT NULL AND weight_lbs > 0
                THEN ROUND(CAST(protein_g AS double) / weight_lbs, 2)
            ELSE NULL
        END AS protein_per_lb,

        -- Partition columns
        SUBSTRING(date, 1, 4) AS year,
        SUBSTRING(date, 6, 2) AS month
    FROM joined
)

SELECT * FROM with_derived
WHERE date IS NOT NULL
