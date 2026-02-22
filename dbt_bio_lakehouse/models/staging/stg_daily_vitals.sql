SELECT
    date,
    resting_heart_rate_bpm,
    hrv_ms,
    vo2_max,
    blood_oxygen_pct,
    respiratory_rate
FROM {{ source('bio_silver', 'healthkit_daily_vitals') }}
