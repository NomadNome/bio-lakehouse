SELECT
    CAST(DATE(workout_date) AS varchar) AS date,
    COUNT(*) AS workout_count,
    SUM(total_output_kj) AS total_output_kj,
    SUM(calories_burned) AS peloton_calories,
    AVG(avg_watts) AS avg_watts,
    MAX(avg_heartrate) AS max_avg_hr,
    AVG(output_per_minute) AS avg_output_per_minute,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT workout_category), ',') AS workout_categories,
    SUM(length_minutes) AS total_workout_minutes,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT fitness_discipline), ',') AS disciplines
FROM {{ source('bio_silver', 'peloton_workouts') }}
GROUP BY CAST(DATE(workout_date) AS varchar)
