# Apple HealthKit — OpenClaw Landing Zone Specification

## Overview

OpenClaw parses the Apple Health `export.xml` file (exported via iPhone Health app → Share → Export All Health Data) and uploads 4 CSV data types to the Bio Lakehouse Bronze S3 bucket.

## Target Bucket

```
s3://bio-lakehouse-bronze-<AWS_ACCOUNT_ID>/healthkit/
```

## Hive Partitioning

All uploads must follow this path structure:

```
healthkit/{data_type}/year=YYYY/month=MM/day=DD/{data_type}.csv
```

Example:
```
healthkit/daily_vitals/year=2026/month=02/day=19/daily_vitals.csv
healthkit/workouts/year=2026/month=02/day=19/workouts.csv
healthkit/body/year=2026/month=02/day=19/body.csv
healthkit/mindfulness/year=2026/month=02/day=19/mindfulness.csv
```

## CSV Format

- **Delimiter:** `,` (comma)
- **Encoding:** UTF-8
- **Header row:** Required (first line)
- **Quoting:** Standard CSV quoting for values containing commas

## Data Type Schemas

### 1. `healthkit/daily_vitals/`

One row per day. Aggregated from Apple Watch / iPhone sensor readings.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `date` | String | Yes | `YYYY-MM-DD` |
| `resting_heart_rate_bpm` | Double | Yes | Actual BPM (e.g., 58.0). Last-of-day value. |
| `hrv_ms` | Double | No | Heart rate variability in milliseconds (SDNN). Last-of-day. |
| `vo2_max` | Double | No | VO2 Max estimate. Sparse — only when Apple Watch calculates it. |
| `blood_oxygen_pct` | Double | No | SpO2 as percentage (97.5, not 0.975). Daily mean. |
| `respiratory_rate` | Double | No | Breaths per minute. Daily mean. |

### 2. `healthkit/workouts/`

One row per workout session. **Exclude any workouts from Peloton** (check `sourceName`).

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `date` | String | Yes | `YYYY-MM-DD` from startDate |
| `start_time` | String | No | ISO 8601 (e.g., `2026-02-19T07:30:00-05:00`) |
| `end_time` | String | No | ISO 8601 |
| `workout_type` | String | Yes | Normalized snake_case (e.g., `hiking`, `swimming`, `functional_strength_training`). Strip `HKWorkoutActivityType` prefix. |
| `duration_minutes` | Double | Yes | From `duration` attribute |
| `calories_burned` | Integer | No | From `totalEnergyBurned` |
| `avg_heart_rate` | Integer | No | From `WorkoutStatistics` sub-element for `HKQuantityTypeIdentifierHeartRate` |
| `distance_mi` | Double | No | Convert km to miles if unit is km (multiply by 0.621371) |
| `source_app` | String | No | Source app name. Must NOT contain "peloton" (case-insensitive). |

### 3. `healthkit/body/`

One row per day with body measurements. Sparse data — not every day will have entries.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `date` | String | Yes | `YYYY-MM-DD` |
| `weight_lbs` | Double | Yes | Convert kg to lbs if source unit is kg (multiply by 2.20462) |
| `body_fat_pct` | Double | No | As percentage (18.0, not 0.18). Convert if decimal. |
| `bmi` | Double | No | Body mass index |
| `lean_body_mass_lbs` | Double | No | Convert kg to lbs if needed |

### 4. `healthkit/mindfulness/`

One row per day, aggregated from all mindfulness sessions.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `date` | String | Yes | `YYYY-MM-DD` |
| `duration_minutes` | Double | Yes | Sum of all mindfulness session durations that day |
| `session_count` | Integer | No | Count of sessions that day |

## Deduplication Rules

1. **Peloton workouts**: Filter out any workout where `sourceName` contains "peloton" (case-insensitive). Peloton data is ingested separately via `peloton/workouts/`.
2. **Steps**: Do NOT extract step counts. Oura Ring is the source of truth for steps (`oura/activity`).
3. **Sleep**: Do NOT extract sleep data. Oura Ring is the source of truth for sleep (`oura/sleep`).

## Unit Conversions

| Source Unit | Target Column | Conversion |
|-------------|---------------|------------|
| kg | `weight_lbs` / `lean_body_mass_lbs` | Multiply by 2.20462 |
| km | `distance_mi` | Multiply by 0.621371 |
| SpO2 as decimal (0.975) | `blood_oxygen_pct` | Multiply by 100 |
| Body fat as decimal (0.18) | `body_fat_pct` | Multiply by 100 |

## Aggregation Rules

For `daily_vitals`, when multiple readings exist per day:
- **Resting heart rate**: Use the **last** reading of the day
- **HRV**: Use the **last** reading of the day
- **VO2 Max**: Use the **last** reading of the day
- **Blood oxygen**: Use the **mean** of all readings
- **Respiratory rate**: Use the **mean** of all readings

## S3 Upload Requirements

- Files must be uploaded with `AES256` server-side encryption
- The Bronze bucket enforces encryption via bucket policy
- Upload via `aws s3 cp` with `--sse AES256` flag or via SDK with SSE parameter

## Pipeline Trigger

Uploading a CSV to the Bronze bucket triggers the ingestion Lambda, which:
1. Validates the CSV headers against the expected schema
2. Logs the ingestion to DynamoDB
3. Triggers the `bio-lakehouse-healthkit-normalizer` Glue job

## Workflow

```
1. Export Apple Health data from iPhone
2. Run parser: python3 scripts/parse_healthkit_export.py --input export.xml
3. Upload staged CSVs to S3:
   aws s3 sync bronze_staged/healthkit/ s3://bio-lakehouse-bronze-<AWS_ACCOUNT_ID>/healthkit/ --sse AES256
4. Pipeline auto-triggers: Lambda → Glue normalizer → Silver Parquet → Gold aggregation
```
