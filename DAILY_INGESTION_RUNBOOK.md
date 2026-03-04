# Bio Lakehouse - Daily Data Ingestion Runbook

**Purpose:** Automated daily workflow to ingest HealthKit + Peloton data from Downloads into the Bio Lakehouse, then push through Bronze → Silver → Gold.

**Schedule:** Run daily at 8:30 AM EST
**Working directory:** `/Users/nomathadejenkins/Desktop/Bio Lakehouse`
**Python:** `.venv/bin/python` (Python 3.12 venv in project root)
**AWS Region:** `us-east-1`

---

## Overview

Every morning the user exports two files to `~/Downloads/`:
1. **HealthKit** — `export <N>.zip` (Apple Health export, ~178MB)
2. **Peloton** — `KnownasNoma_workouts-<N>.csv` (full workout history)

Oura data is ingested automatically via Lambda (no action needed).

This workflow: parses the exports → uploads to Bronze S3 → runs Glue normalizers → crawls Silver → rebuilds Gold → restarts Streamlit.

---

## Step 1: Find the Latest Files

Find the most recently modified HealthKit zip and Peloton CSV in `~/Downloads/`:

```bash
# HealthKit — newest zip matching "export*.zip"
HK_ZIP=$(ls -t ~/Downloads/export*.zip 2>/dev/null | head -1)

# Peloton — newest csv matching "KnownasNoma_workouts*.csv"
PELO_CSV=$(ls -t ~/Downloads/KnownasNoma_workouts*.csv 2>/dev/null | head -1)
```

**Validation:** Both files must exist and have today's date (or yesterday's) as their modification date. If either file is missing or stale (modified more than 24 hours ago), STOP and notify the user: "HealthKit/Peloton export not found in Downloads. Please export from your phone first."

---

## Step 2: Parse HealthKit Export

```bash
# Unzip to temp directory
rm -rf /tmp/healthkit_daily_parse
mkdir -p /tmp/healthkit_daily_parse
unzip -q "$HK_ZIP" -d /tmp/healthkit_daily_parse

# Calculate yesterday's date for --since flag (captures yesterday + today)
SINCE_DATE=$(date -v-1d +%Y-%m-%d)

# Parse XML → Hive-partitioned CSVs
cd "/Users/nomathadejenkins/Desktop/Bio Lakehouse"
.venv/bin/python scripts/parse_healthkit_export.py \
    --input /tmp/healthkit_daily_parse/apple_health_export/export.xml \
    --since "$SINCE_DATE" \
    --output-dir /tmp/healthkit_daily_csvs
```

**Expected output:** Up to 4 subdirectories in `/tmp/healthkit_daily_csvs/`:
- `daily_vitals/year=YYYY/month=MM/day=DD/daily_vitals.csv`
- `workouts/year=YYYY/month=MM/day=DD/workouts.csv`
- `body/year=YYYY/month=MM/day=DD/body.csv`
- `mindfulness/year=YYYY/month=MM/day=DD/mindfulness.csv`

Each CSV has Hive partition path structure. Typically 1-3 days of data per type.

**Timeout:** ~15 seconds for the XML parse. If it takes >60s, something is wrong.

---

## Step 3: Split Peloton CSV

The Peloton CSV contains the user's full workout history. Extract only recent dates (last 2 days) into Hive-partitioned layout:

```python
# Run this as inline Python via .venv/bin/python3 -c "..."
import csv, os
from datetime import datetime, timedelta

INPUT = "<PELO_CSV path from Step 1>"
OUTDIR = "/tmp/peloton_daily_split"
SINCE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

os.makedirs(OUTDIR, exist_ok=True)

with open(INPUT) as f:
    reader = csv.DictReader(f)
    rows_by_date = {}
    for row in reader:
        ts = row.get("Workout Timestamp", "")
        try:
            dt = datetime.strptime(ts[:10], "%Y-%m-%d")
            if dt >= datetime.strptime(SINCE, "%Y-%m-%d"):
                d = dt.strftime("%Y-%m-%d")
                rows_by_date.setdefault(d, []).append(row)
        except ValueError:
            continue

for date, rows in sorted(rows_by_date.items()):
    y, m, d = date.split("-")
    outpath = f"{OUTDIR}/year={y}/month={m}/day={d}/peloton_workouts.csv"
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", newline="") as out:
        w = csv.DictWriter(out, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  {date}: {len(rows)} workout(s)")

print(f"Total dates: {len(rows_by_date)}")
```

**Expected output:** 1-2 date directories under `/tmp/peloton_daily_split/year=YYYY/month=MM/day=DD/peloton_workouts.csv`

**Note:** Some days may have 0 Peloton workouts (rest days). That's fine — just skip the Peloton upload if no recent dates are found.

---

## Step 4: Upload to Bronze S3

**Bucket:** `bio-lakehouse-bronze-<ACCOUNT_ID>`

### 4a. Upload HealthKit CSVs

```bash
BUCKET="bio-lakehouse-bronze-<ACCOUNT_ID>"

for type_dir in daily_vitals workouts body mindfulness; do
    for csv_file in /tmp/healthkit_daily_csvs/$type_dir/year=*/month=*/day=*/*.csv; do
        [ -f "$csv_file" ] || continue
        part=$(echo "$csv_file" | grep -o 'year=.*')
        s3key="healthkit/${type_dir}/${part}"
        aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet
        echo "  Uploaded: ${s3key}"
    done
done
```

### 4b. Upload Peloton CSVs

```bash
for csv_file in /tmp/peloton_daily_split/year=*/month=*/day=*/*.csv; do
    [ -f "$csv_file" ] || continue
    part=$(echo "$csv_file" | grep -o 'year=.*')
    s3key="peloton/workouts/${part}"
    aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet
    echo "  Uploaded: ${s3key}"
done
```

### 4c. Upload Batch Manifests

Manifests trigger the Lambda once per source (instead of per-file). **This is critical** — without manifests, individual file uploads cause Lambda spam and Glue throttling.

```bash
BATCH_DATE=$(date +%Y-%m-%d)

# HealthKit manifest
cat > /tmp/hk_manifest.json <<EOF
{"batch_id": "healthkit-${BATCH_DATE}", "source_types": ["healthkit/daily_vitals", "healthkit/workouts", "healthkit/body", "healthkit/mindfulness"], "file_count": $(find /tmp/healthkit_daily_csvs -name '*.csv' | wc -l | tr -d ' ')}
EOF
aws s3 cp /tmp/hk_manifest.json "s3://${BUCKET}/healthkit/healthkit-${BATCH_DATE}_manifest.json" --quiet

# Peloton manifest (only if we have Peloton files)
PELO_COUNT=$(find /tmp/peloton_daily_split -name '*.csv' 2>/dev/null | wc -l | tr -d ' ')
if [ "$PELO_COUNT" -gt 0 ]; then
    cat > /tmp/pelo_manifest.json <<EOF
{"batch_id": "peloton-${BATCH_DATE}", "source_types": ["peloton/workouts"], "file_count": ${PELO_COUNT}}
EOF
    aws s3 cp /tmp/pelo_manifest.json "s3://${BUCKET}/peloton/peloton-${BATCH_DATE}_manifest.json" --quiet
fi
```

---

## Step 5: Run Glue Normalizers (Bronze → Silver)

Start all three normalizers in parallel. Oura runs even though it's Lambda-triggered — this ensures any missed data gets picked up.

```bash
OURA_RUN=$(aws glue start-job-run --job-name bio-lakehouse-oura-normalizer --region us-east-1 \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-<ACCOUNT_ID>","--source_type":"oura"}' \
    --query 'JobRunId' --output text)

HK_RUN=$(aws glue start-job-run --job-name bio-lakehouse-healthkit-normalizer --region us-east-1 \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-<ACCOUNT_ID>","--source_type":"healthkit"}' \
    --query 'JobRunId' --output text)

PELO_RUN=$(aws glue start-job-run --job-name bio-lakehouse-peloton-normalizer --region us-east-1 \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-<ACCOUNT_ID>","--source_type":"peloton"}' \
    --query 'JobRunId' --output text)

echo "Started: Oura=$OURA_RUN  HK=$HK_RUN  Peloton=$PELO_RUN"
```

**Poll until all three complete:**

```bash
while true; do
    OURA=$(aws glue get-job-run --job-name bio-lakehouse-oura-normalizer --run-id "$OURA_RUN" \
        --region us-east-1 --query 'JobRun.JobRunState' --output text | head -1)
    HK=$(aws glue get-job-run --job-name bio-lakehouse-healthkit-normalizer --run-id "$HK_RUN" \
        --region us-east-1 --query 'JobRun.JobRunState' --output text | head -1)
    PELO=$(aws glue get-job-run --job-name bio-lakehouse-peloton-normalizer --run-id "$PELO_RUN" \
        --region us-east-1 --query 'JobRun.JobRunState' --output text | head -1)
    echo "$(date +%H:%M:%S) Oura=$OURA  HK=$HK  Peloton=$PELO"

    # Check for failures
    for state in "$OURA" "$HK" "$PELO"; do
        if [ "$state" = "FAILED" ]; then
            echo "ERROR: A normalizer FAILED. Check AWS Glue console."
        fi
    done

    if [ "$OURA" != "RUNNING" ] && [ "$OURA" != "STARTING" ] && \
       [ "$HK" != "RUNNING" ] && [ "$HK" != "STARTING" ] && \
       [ "$PELO" != "RUNNING" ] && [ "$PELO" != "STARTING" ]; then
        break
    fi
    sleep 20
done
```

**Expected time:** ~3-5 min for Oura/Peloton, ~13 min for HealthKit (largest dataset).
**All three must show SUCCEEDED before continuing.**

---

## Step 6: Run Silver Crawler

```bash
aws glue start-crawler --name bio-lakehouse-silver-crawler --region us-east-1

# Poll until READY
while true; do
    STATE=$(aws glue get-crawler --name bio-lakehouse-silver-crawler --region us-east-1 \
        --query 'Crawler.State' --output text | head -1)
    echo "$(date +%H:%M:%S) Silver crawler: $STATE"
    if [ "$STATE" = "READY" ]; then break; fi
    sleep 10
done
```

**Expected time:** ~30-60 seconds.

---

## Step 7: Run Gold Refresh (Silver → Gold)

This Glue Python Shell job runs Athena CTAS queries to rebuild the 3 Gold tables:
- `gold_daily_rollup` (main daily metrics table)
- `feature_readiness_daily` (ML features)
- `workout_recovery_windows` (recovery analysis)

```bash
GOLD_RUN=$(aws glue start-job-run --job-name bio-lakehouse-dbt-gold-refresh --region us-east-1 \
    --query 'JobRunId' --output text)

while true; do
    STATE=$(aws glue get-job-run --job-name bio-lakehouse-dbt-gold-refresh --run-id "$GOLD_RUN" \
        --region us-east-1 --query 'JobRun.JobRunState' --output text | head -1)
    echo "$(date +%H:%M:%S) Gold refresh: $STATE"
    if [ "$STATE" = "SUCCEEDED" ] || [ "$STATE" = "FAILED" ] || [ "$STATE" = "STOPPED" ]; then break; fi
    sleep 15
done
```

**Expected time:** ~1 minute. **Must show SUCCEEDED.**

---

## Step 8: Run Gold Crawler

```bash
aws glue start-crawler --name bio-lakehouse-gold-crawler --region us-east-1

while true; do
    STATE=$(aws glue get-crawler --name bio-lakehouse-gold-crawler --region us-east-1 \
        --query 'Crawler.State' --output text | head -1)
    echo "$(date +%H:%M:%S) Gold crawler: $STATE"
    if [ "$STATE" = "READY" ]; then break; fi
    sleep 10
done
```

**Expected time:** ~1-2 minutes.

---

## Step 9: Verify Data in Gold

Run a quick Athena query to confirm today's data landed:

```bash
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d)

QID=$(aws athena start-query-execution \
    --query-string "SELECT date, readiness_score, sleep_score, activity_score, workout_count, hk_workout_count, resting_heart_rate_bpm, weight_lbs, daily_calories FROM bio_gold.daily_readiness_performance WHERE date >= '${YESTERDAY}' ORDER BY date DESC" \
    --query-execution-context Database=bio_gold \
    --result-configuration OutputLocation=s3://bio-lakehouse-athena-results-<ACCOUNT_ID>/ \
    --region us-east-1 --output text --query 'QueryExecutionId')

sleep 5

aws athena get-query-results --query-execution-id "$QID" --region us-east-1 --output table
```

**Expected:** Rows for yesterday and today with non-null readiness/sleep scores. Some fields (activity_score) won't populate until end-of-day for today's row.

---

## Step 10: Restart Streamlit

```bash
cd "/Users/nomathadejenkins/Desktop/Bio Lakehouse"
bash run_streamlit.sh
```

This kills any existing Streamlit process, clears all caches (10-min TTL), and restarts on port 8501.

**Streamlit URL:** http://localhost:8501

---

## Quick Reference

| Resource | Value |
|----------|-------|
| Project dir | `/Users/nomathadejenkins/Desktop/Bio Lakehouse` |
| Python venv | `.venv/bin/python` (Python 3.12) |
| Bronze bucket | `bio-lakehouse-bronze-<ACCOUNT_ID>` |
| Silver bucket | `bio-lakehouse-silver-<ACCOUNT_ID>` |
| Gold bucket | `bio-lakehouse-gold-<ACCOUNT_ID>` |
| Athena results | `bio-lakehouse-athena-results-<ACCOUNT_ID>` |
| AWS region | `us-east-1` |
| Oura normalizer | `bio-lakehouse-oura-normalizer` |
| HealthKit normalizer | `bio-lakehouse-healthkit-normalizer` |
| Peloton normalizer | `bio-lakehouse-peloton-normalizer` |
| Gold refresh job | `bio-lakehouse-dbt-gold-refresh` |
| Silver crawler | `bio-lakehouse-silver-crawler` |
| Gold crawler | `bio-lakehouse-gold-crawler` |

## Error Handling

- **File not found in Downloads:** Stop and tell the user to export from their phone first.
- **Normalizer FAILED:** Check the Glue console log. Common cause: schema change in source data.
- **Gold refresh FAILED:** Usually an Athena SQL error. Check the Glue job log for the Athena error message.
- **ConcurrentRunsExceededException:** A job is already running. Wait for it to finish, then retry.
- **Crawler stuck in STOPPING:** Wait — it will transition to READY within ~2 minutes.

## Total Expected Runtime

| Step | Time |
|------|------|
| Parse HealthKit XML | ~15s |
| Split Peloton CSV | ~1s |
| Upload to Bronze | ~10s |
| Normalizers (parallel) | ~13 min (HealthKit is slowest) |
| Silver crawler | ~1 min |
| Gold refresh | ~1 min |
| Gold crawler | ~2 min |
| **Total** | **~18 minutes** |
