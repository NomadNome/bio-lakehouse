# Peloton OpenClaw Landing Zone Specification

**Version:** 1.0  
**Last Updated:** 2026-02-18  
**Status:** Active

## Overview

This document specifies the S3 landing zone for Peloton workout data pushed by the user's OpenClaw automation agent. The existing Bio Lakehouse ingestion pipeline automatically processes files uploaded to this location.

## S3 Path Structure

```
s3://bio-lakehouse-bronze-000000000000/peloton/workouts/year=YYYY/month=MM/day=DD/workouts.csv
```

**Example:**
```
s3://bio-lakehouse-bronze-000000000000/peloton/workouts/year=2026/month=02/day=18/workouts.csv
```

### Path Components

- **Bucket:** `bio-lakehouse-bronze-000000000000` (bronze layer, environment-specific)
- **Prefix:** `peloton/workouts/` (data source identifier)
- **Partitioning:** Hive-style partitions by year, month, day
- **Filename:** `workouts.csv` (or timestamped variant: `workouts_20260218_173534.csv`)

## File Format

### Delimiter

**Comma (`,`)** — matches existing ingestion trigger validation logic.

**Do NOT use semicolon (`;`)** — the ingestion Lambda (`handler.py:55`) splits headers by comma.

### Encoding

UTF-8

### Headers

Required headers (case-sensitive, order-independent):

```
workout_timestamp,live_on_demand,instructor_name,length_minutes,fitness_discipline,type,title,total_output,avg_watts,avg_resistance,avg_cadence_rpm,avg_speed_mph,distance_mi,calories_burned,avg_heartrate,workout_date,workout_time,utc_offset
```

#### Critical Fields

These fields are validated by the ingestion trigger (`handler.py:64-72`):

- `workout_timestamp` — **Required.** Format: `YYYY-MM-DD HH:MM (-04)` or `YYYY-MM-DD HH:MM (EST)`
- `fitness_discipline` — **Required.** Example: `Cycling`, `Strength`, `Walking`
- `total_output` — **Required.** Numeric or empty for non-cycling workouts
- `calories_burned` — **Required.** Numeric

#### Full Schema

Defined in `glue/bio_etl_utils.py:61-82` (`PELOTON_SCHEMA`):

| Column                | Type    | Description                              | Example                         |
|-----------------------|---------|------------------------------------------|---------------------------------|
| `workout_timestamp`   | String  | Original workout timestamp from Peloton   | `2021-05-29 09:27 (-04)`        |
| `live_on_demand`      | String  | Live or On Demand                        | `On Demand`                     |
| `instructor_name`     | String  | Instructor name                          | `Hannah Corbin`                 |
| `length_minutes`      | Integer | Workout duration in minutes              | `20`                            |
| `fitness_discipline`  | String  | Workout category                         | `Cycling`, `Strength`           |
| `type`                | String  | Workout sub-type                         | `Music`, `Beginner`             |
| `title`               | String  | Workout title                            | `20 min 90s Pop Ride`           |
| `total_output`        | Integer | Total output (cycling only)              | `136` or empty                  |
| `avg_watts`           | Integer | Average watts (cycling only)             | `113` or empty                  |
| `avg_resistance`      | String  | Average resistance                       | `42%` or empty                  |
| `avg_cadence_rpm`     | Integer | Average cadence in RPM                   | `76` or empty                   |
| `avg_speed_mph`       | Float   | Average speed in mph                     | `16.59` or empty                |
| `distance_mi`         | Float   | Distance in miles                        | `5.53` or empty                 |
| `calories_burned`     | Integer | Total calories burned                    | `199`                           |
| `avg_heartrate`       | Integer | Average heart rate                       | `142` or empty                  |
| `workout_date`        | Date    | Workout date (extracted during ETL)      | `2021-05-29`                    |
| `workout_time`        | String  | Workout time (extracted during ETL)      | `09:27:00`                      |
| `utc_offset`          | String  | UTC offset (extracted during ETL)        | `-04:00`                        |

### Data Formatting Rules

1. **Empty values:** Use empty string (consecutive commas: `,,`) for missing data
2. **Numeric precision:** Match Peloton export (e.g., `16.59` for speed, not `16.6`)
3. **Percentages:** Include `%` symbol if present in export (e.g., `42%`)
4. **No quotes:** Unless the field contains a comma (standard CSV escaping)

### Example Row

```csv
workout_timestamp,live_on_demand,instructor_name,length_minutes,fitness_discipline,type,title,total_output,avg_watts,avg_resistance,avg_cadence_rpm,avg_speed_mph,distance_mi,calories_burned,avg_heartrate,workout_date,workout_time,utc_offset
2021-05-29 09:29 (-04),On Demand,Hannah Corbin,20,Cycling,Music,20 min 90s Pop Ride,136,113,42%,76,16.59,5.53,199,,,
```

## Upload Requirements

### S3 PutObject Parameters

**Must include:**

```python
s3_client.put_object(
    Bucket='bio-lakehouse-bronze-000000000000',
    Key='peloton/workouts/year=2026/month=02/day=18/workouts.csv',
    Body=csv_content,
    ServerSideEncryption='AES256',  # REQUIRED by bucket policy
    Metadata={
        'source': 'peloton',
        'ingestion_ts': '20260218_173534',
        'type': 'workouts'
    }
)
```

**Critical:** `ServerSideEncryption='AES256'` is **mandatory**. The bronze bucket policy (`bronze-stack.yaml:48-55`) denies all PutObject requests without AES256 encryption.

### IAM Permissions

The OpenClaw agent must have:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject"
  ],
  "Resource": "arn:aws:s3:::bio-lakehouse-bronze-000000000000/peloton/workouts/*"
}
```

## Automated Pipeline Flow

Once the file lands in S3:

1. **S3 Event Notification** → Lambda `bio-lakehouse-ingestion-trigger`
2. **Validation** (`lambda/ingestion_trigger/handler.py`):
   - Header validation (comma-delimited, required columns present)
   - Logs to DynamoDB `bio_ingestion_log` table
3. **Trigger Glue Job** → `bio-lakehouse-peloton-normalizer`
4. **ETL Processing** (`glue/jobs/peloton_normalizer.py`):
   - Parse `workout_timestamp` into `workout_date`, `workout_time`, `utc_offset`
   - Convert types per schema
   - Write Parquet to Silver: `s3://bio-lakehouse-silver-000000000000/peloton/workouts/`
5. **Silver Available** for downstream analytics/aggregation

## OpenClaw Agent Implementation

### Current Setup (2026-02-18)

**Script:** `~/.openclaw/workspace/scripts/peloton-sync.sh`

**Execution:**
1. User downloads Peloton CSV export (browser automation via OpenClaw)
2. Script locates latest CSV in `~/Downloads`
3. Uploads to S3 with correct path structure and encryption
4. Logs to DynamoDB with metadata

**Schedule:** Every Sunday at 8:00 AM EST (OpenClaw cron job)

**Next Run:** Sunday, February 22, 2026 at 8:00 AM EST

### Manual Trigger

```bash
cd ~/.openclaw/workspace/scripts
./peloton-manual-sync
```

## Verification

### 1. Confirm S3 Upload

```bash
aws s3 ls s3://bio-lakehouse-bronze-000000000000/peloton/workouts/year=2026/month=02/day=18/ --human-readable
```

Expected output:
```
2026-02-18 17:36:28  113.4 KiB workouts_20260218_173627.csv
```

### 2. Check DynamoDB Ingestion Log

```bash
aws dynamodb query \
  --table-name bio_ingestion_log \
  --key-condition-expression "file_path = :fp" \
  --expression-attribute-values '{":fp":{"S":"s3://bio-lakehouse-bronze-000000000000/peloton/workouts/year=2026/month=02/day=18/workouts_20260218_173627.csv"}}' \
  --limit 1
```

Expected fields:
- `status`: `completed` or `validation_failed`
- `record_count`: Number of workouts
- `upload_timestamp`: Unix timestamp

### 3. Verify Glue Job Execution

```bash
aws glue get-job-runs --job-name bio-lakehouse-peloton-normalizer --max-results 5
```

Look for:
- `JobRunState`: `SUCCEEDED`
- `StartedOn`: Recent timestamp matching upload

### 4. Query Silver Data (Athena)

```sql
SELECT COUNT(*) AS workout_count
FROM "bio_silver"."peloton_workouts"
WHERE year = 2026 AND month = 02 AND day = 18;
```

## Troubleshooting

### Common Issues

**Problem:** `AccessDenied` on S3 PutObject

**Solution:** Verify `ServerSideEncryption='AES256'` is set

---

**Problem:** Ingestion trigger marks file as `validation_failed`

**Solution:** Check header format:
- Delimiter must be comma (not semicolon)
- Required fields: `workout_timestamp`, `fitness_discipline`, `total_output`, `calories_burned`

---

**Problem:** Glue job fails with parsing errors

**Solution:** Verify timestamp format matches `YYYY-MM-DD HH:MM (±HH)` or `YYYY-MM-DD HH:MM (TZ)`

### Logs

**Ingestion Trigger Lambda:**
```bash
aws logs tail /aws/lambda/bio-lakehouse-ingestion-trigger --follow
```

**Glue Job:**
```bash
aws logs tail /aws-glue/jobs/output --log-stream-names bio-peloton-normalizer
```

## Schema Evolution

If Peloton adds new columns to their export:

1. **Bronze (this spec):** Add column to expected headers (maintain backward compatibility)
2. **Silver normalizer:** Update `PELOTON_SCHEMA` in `bio_etl_utils.py`
3. **Glue job:** Update parsing logic in `peloton_normalizer.py`
4. **Athena:** Run `MSCK REPAIR TABLE` if partitioning changes

## Contact

**Maintained by:** Bio Lakehouse Team  
**Questions:** Check existing ingestion logic in:
- `lambda/ingestion_trigger/handler.py` (validation)
- `glue/bio_etl_utils.py` (schema definitions)
- `glue/jobs/peloton_normalizer.py` (ETL logic)

---

**Note:** This specification describes the landing zone only. Ingestion pipeline components are managed separately in CloudFormation stacks (`bronze-stack.yaml`, `glue-stack.yaml`).
