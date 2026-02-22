SELECT
    day AS date,
    score AS sleep_score,
    contributors_deep_sleep AS deep_sleep_score,
    contributors_rem_sleep AS rem_sleep_score,
    contributors_efficiency AS sleep_efficiency_score,
    contributors_total_sleep AS total_sleep_score
FROM {{ source('bio_silver', 'oura_daily_sleep') }}
