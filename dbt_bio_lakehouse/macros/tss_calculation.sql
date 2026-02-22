{% macro tss_calculation(
    had_workout='had_workout',
    total_output_kj='total_output_kj',
    max_avg_hr='max_avg_hr',
    peloton_calories='peloton_calories',
    total_workout_minutes='total_workout_minutes',
    active_calories='active_calories',
    hk_calories='hk_calories',
    hk_workout_minutes='hk_workout_minutes'
) %}
CASE
    WHEN {{ had_workout }} = false THEN 0.0
    -- Tier 1: Peloton power data (most accurate)
    WHEN {{ total_output_kj }} IS NOT NULL AND {{ total_output_kj }} > 0
        THEN LEAST(300.0, {{ total_output_kj }} * COALESCE({{ max_avg_hr }}, 140) / 600.0)
    -- Tier 2: Peloton calories + duration (ride without power)
    WHEN {{ peloton_calories }} IS NOT NULL AND {{ peloton_calories }} > 0
         AND {{ total_workout_minutes }} IS NOT NULL AND {{ total_workout_minutes }} > 0
        THEN LEAST(300.0, {{ peloton_calories }} * {{ total_workout_minutes }} / 150.0)
    -- Tier 3: HealthKit active calories + duration (non-Peloton workout)
    WHEN {{ active_calories }} IS NOT NULL AND {{ active_calories }} > 0
         AND {{ total_workout_minutes }} IS NOT NULL AND {{ total_workout_minutes }} > 0
        THEN LEAST(300.0, {{ active_calories }} * {{ total_workout_minutes }} / 450.0)
    -- Tier 4: HealthKit workout calories + duration (Solidcore, yoga, etc.)
    WHEN {{ hk_calories }} IS NOT NULL AND {{ hk_calories }} > 0
         AND {{ hk_workout_minutes }} IS NOT NULL AND {{ hk_workout_minutes }} > 0
        THEN LEAST(300.0, {{ hk_calories }} * {{ hk_workout_minutes }} / 450.0)
    -- Tier 5: HealthKit active calories only (no duration recorded)
    WHEN {{ active_calories }} IS NOT NULL AND {{ active_calories }} > 0
        THEN LEAST(200.0, {{ active_calories }} / 12.0)
    ELSE 0.0
END
{% endmacro %}
