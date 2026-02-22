SELECT
    day AS date,
    score AS readiness_score,
    contributors_hrv_balance AS hrv_balance_score,
    contributors_resting_heart_rate AS resting_hr_score,
    contributors_previous_night AS previous_night_score,
    contributors_recovery_index AS recovery_index_score,
    temperature_deviation
FROM {{ source('bio_silver', 'oura_daily_readiness') }}
