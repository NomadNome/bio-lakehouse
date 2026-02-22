SELECT
    date,
    COUNT(*) AS hk_workout_count,
    SUM(calories_burned) AS hk_calories,
    SUM(duration_minutes) AS hk_workout_minutes,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT workout_category), ',') AS hk_workout_categories,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT workout_type), ',') AS hk_workout_types
FROM {{ source('bio_silver', 'healthkit_workouts') }}
GROUP BY date
