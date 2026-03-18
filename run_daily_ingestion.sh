#!/bin/bash
# Bio Lakehouse - Daily Data Ingestion (Full Pipeline)
# Runs Steps 1-11: Parse → Upload → Normalize → Crawl → Gold → Verify → Streamlit → Briefing
# Schedule: Daily at 8:30 AM ET via launchd
# Prereq: User must export HealthKit + Peloton to ~/Downloads each morning

set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="bio-lakehouse-bronze-${AWS_ACCOUNT_ID}"
REGION="us-east-1"
PROJECT_DIR="$HOME/Desktop/Bio Lakehouse"
BATCH_DATE=$(date +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)
SINCE_DATE="$YESTERDAY"
LOG_PREFIX="[$(date +%Y-%m-%d\ %H:%M:%S)]"

echo "========================================"
echo "$LOG_PREFIX Bio Lakehouse Daily Ingestion"
echo "Date: $BATCH_DATE"
echo "========================================"

# -----------------------------------------------
# STEP 1: Find Latest Files
# -----------------------------------------------
echo ""
echo "--- Step 1: Find Latest Files ---"

INBOX="$PROJECT_DIR/inbox"

# Check inbox first (launchd-safe), fallback to ~/Downloads (manual runs)
HK_ZIP=$(ls -t "$INBOX"/export*.zip 2>/dev/null | head -1)
[ -z "$HK_ZIP" ] && HK_ZIP=$(ls -t ~/Downloads/export*.zip 2>/dev/null | head -1)

PELO_CSV=$(ls -t "$INBOX"/KnownasNoma_workouts*.csv 2>/dev/null | head -1)
[ -z "$PELO_CSV" ] && PELO_CSV=$(ls -t ~/Downloads/KnownasNoma_workouts*.csv 2>/dev/null | head -1)

MFP_CSV=$(ls -t "$INBOX"/Nutrition-Summary*.csv 2>/dev/null | head -1)
[ -z "$MFP_CSV" ] && MFP_CSV=$(ls -t ~/Downloads/Nutrition-Summary*.csv 2>/dev/null | head -1 || true)

if [ -z "$HK_ZIP" ]; then
    echo "  ERROR: No HealthKit export found in ~/Downloads/. Please export from your phone first."
    exit 1
fi
if [ -z "$PELO_CSV" ]; then
    echo "  ERROR: No Peloton CSV found in ~/Downloads/. Please export from Peloton first."
    exit 1
fi

# Check freshness (modified within last 25 hours)
HK_AGE=$(( $(date +%s) - $(stat -f %m "$HK_ZIP") ))
PELO_AGE=$(( $(date +%s) - $(stat -f %m "$PELO_CSV") ))

if [ "$HK_AGE" -gt 90000 ]; then
    echo "  WARNING: HealthKit export is $(( HK_AGE / 3600 ))h old. Expected <25h."
    echo "  File: $HK_ZIP"
    echo "  Continuing anyway..."
fi
if [ "$PELO_AGE" -gt 90000 ]; then
    echo "  WARNING: Peloton CSV is $(( PELO_AGE / 3600 ))h old. Expected <25h."
    echo "  File: $PELO_CSV"
    echo "  Continuing anyway..."
fi

echo "  HealthKit: $HK_ZIP"
echo "  Peloton:   $PELO_CSV"
if [ -n "$MFP_CSV" ]; then
    echo "  MFP:       $MFP_CSV"
else
    echo "  MFP:       (none found — skipping)"
fi

# -----------------------------------------------
# STEP 2: Parse HealthKit Export
# -----------------------------------------------
echo ""
echo "--- Step 2: Parse HealthKit ---"

rm -rf /tmp/healthkit_daily_parse /tmp/healthkit_daily_csvs
mkdir -p /tmp/healthkit_daily_parse /tmp/healthkit_daily_csvs

echo "  Extracting export.xml from zip..."
unzip -q -o "$HK_ZIP" "apple_health_export/export.xml" -d /tmp/healthkit_daily_parse

echo "  Parsing since $SINCE_DATE..."
cd "$PROJECT_DIR"
.venv/bin/python scripts/parse_healthkit_export.py \
    --input /tmp/healthkit_daily_parse/apple_health_export/export.xml \
    --since "$SINCE_DATE" \
    --output-dir /tmp/healthkit_daily_csvs

echo "  HealthKit parse complete!"

# -----------------------------------------------
# STEP 3: Split Peloton CSV
# -----------------------------------------------
echo ""
echo "--- Step 3: Split Peloton ---"

rm -rf /tmp/peloton_daily_split
.venv/bin/python3 -c "
import csv, os, re
from datetime import datetime, timedelta

INPUT = '$PELO_CSV'
OUTDIR = '/tmp/peloton_daily_split'
SINCE = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

os.makedirs(OUTDIR, exist_ok=True)

# Normalize header: 'Workout Timestamp' -> 'workout_timestamp'
def snake(name):
    return re.sub(r'[.\s/()]+', '_', name.strip()).lower().strip('_')

with open(INPUT) as f:
    reader = csv.DictReader(f)
    rows_by_date = {}
    for raw_row in reader:
        row = {snake(k): v for k, v in raw_row.items()}
        ts = row.get('workout_timestamp', '')
        try:
            dt = datetime.strptime(ts[:10], '%Y-%m-%d')
            if dt >= datetime.strptime(SINCE, '%Y-%m-%d'):
                d = dt.strftime('%Y-%m-%d')
                rows_by_date.setdefault(d, []).append(row)
        except ValueError:
            continue

for date, rows in sorted(rows_by_date.items()):
    y, m, d = date.split('-')
    outpath = f'{OUTDIR}/year={y}/month={m}/day={d}/peloton_workouts.csv'
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, 'w', newline='') as out:
        w = csv.DictWriter(out, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f'  {date}: {len(rows)} workout(s)')

print(f'  Total dates: {len(rows_by_date)}')
"

echo "  Peloton split complete!"

# -----------------------------------------------
# STEP 4: Upload to Bronze S3
# -----------------------------------------------
echo ""
echo "--- Step 4: Upload to Bronze S3 ---"

echo "  Uploading HealthKit CSVs..."
for type_dir in daily_vitals workouts body mindfulness; do
    for csv_file in /tmp/healthkit_daily_csvs/$type_dir/year=*/month=*/day=*/*.csv; do
        [ -f "$csv_file" ] || continue
        part=$(echo "$csv_file" | grep -o 'year=.*')
        s3key="healthkit/${type_dir}/${part}"
        aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet --sse AES256 --region "$REGION"
        echo "    Uploaded: ${s3key}"
    done
done

echo "  Uploading Peloton CSVs..."
PELO_COUNT=0
for csv_file in /tmp/peloton_daily_split/year=*/month=*/day=*/*.csv; do
    [ -f "$csv_file" ] || continue
    part=$(echo "$csv_file" | grep -o 'year=.*')
    s3key="peloton/workouts/${part}"
    aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet --sse AES256 --region "$REGION"
    echo "    Uploaded: ${s3key}"
    PELO_COUNT=$((PELO_COUNT + 1))
done

# Also upload the raw Peloton CSV (normalizer reads the top-level bulk export)
echo "  Uploading raw Peloton CSV..."
PELO_BASENAME=$(basename "$PELO_CSV")
aws s3 cp "$PELO_CSV" "s3://${BUCKET}/peloton/workouts/${PELO_BASENAME}" --quiet --sse AES256 --region "$REGION"
echo "    Uploaded: peloton/workouts/${PELO_BASENAME}"

if [ -n "$MFP_CSV" ]; then
    echo "  Uploading MFP CSV..."
    MFP_BASENAME=$(basename "$MFP_CSV")
    aws s3 cp "$MFP_CSV" "s3://${BUCKET}/mfp/nutrition/${MFP_BASENAME}" \
        --quiet --sse AES256 --region "$REGION"
    echo "    Uploaded: mfp/nutrition/${MFP_BASENAME}"
fi

echo "  Uploading batch manifests..."
HK_FILE_COUNT=$(find /tmp/healthkit_daily_csvs -name '*.csv' | wc -l | tr -d ' ')
cat > /tmp/hk_manifest.json <<EOF
{"batch_id": "healthkit-${BATCH_DATE}", "source_types": ["healthkit/daily_vitals", "healthkit/workouts", "healthkit/body", "healthkit/mindfulness"], "file_count": ${HK_FILE_COUNT}}
EOF
aws s3 cp /tmp/hk_manifest.json "s3://${BUCKET}/healthkit/healthkit-${BATCH_DATE}_manifest.json" --quiet --sse AES256 --region "$REGION"
echo "    Uploaded: healthkit manifest (${HK_FILE_COUNT} files)"

if [ "$PELO_COUNT" -gt 0 ]; then
    cat > /tmp/pelo_manifest.json <<EOF
{"batch_id": "peloton-${BATCH_DATE}", "source_types": ["peloton/workouts"], "file_count": ${PELO_COUNT}}
EOF
    aws s3 cp /tmp/pelo_manifest.json "s3://${BUCKET}/peloton/peloton-${BATCH_DATE}_manifest.json" --quiet --sse AES256 --region "$REGION"
    echo "    Uploaded: peloton manifest (${PELO_COUNT} files)"
fi

echo "  Bronze upload complete!"

# -----------------------------------------------
# STEP 5: Run Glue Normalizers (Bronze → Silver)
# -----------------------------------------------
echo ""
echo "--- Step 5: Run Glue Normalizers ---"

# Helper: start a Glue job or attach to an already-running one
start_or_attach() {
    local job_name="$1"; shift
    local run_id
    run_id=$(aws glue start-job-run --job-name "$job_name" --region "$REGION" "$@" \
        --query 'JobRunId' --output text 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$run_id" ]; then
        echo "$run_id"
        return
    fi
    # ConcurrentRunsExceededException — attach to the existing run
    aws glue get-job-runs --job-name "$job_name" --region "$REGION" --max-results 1 \
        --query 'JobRuns[0].Id' --output text
}

OURA_RUN=$(start_or_attach bio-lakehouse-oura-normalizer \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-'"${AWS_ACCOUNT_ID}"'","--source_type":"oura"}')

HK_RUN=$(start_or_attach bio-lakehouse-healthkit-normalizer \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-'"${AWS_ACCOUNT_ID}"'","--source_type":"healthkit"}')

PELO_RUN=$(start_or_attach bio-lakehouse-peloton-normalizer \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-'"${AWS_ACCOUNT_ID}"'","--source_type":"peloton"}')

MFP_RUN=$(start_or_attach bio-lakehouse-mfp-normalizer \
    --arguments '{"--bronze_bucket":"bio-lakehouse-bronze-'"${AWS_ACCOUNT_ID}"'","--silver_bucket":"bio-lakehouse-silver-'"${AWS_ACCOUNT_ID}"'"}')

echo "  Started/attached: Oura=$OURA_RUN  HK=$HK_RUN  Peloton=$PELO_RUN  MFP=$MFP_RUN"
echo "  Polling (expect ~13 min for HealthKit)..."

while true; do
    OURA=$(aws glue get-job-run --job-name bio-lakehouse-oura-normalizer --run-id "$OURA_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    HK=$(aws glue get-job-run --job-name bio-lakehouse-healthkit-normalizer --run-id "$HK_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    PELO=$(aws glue get-job-run --job-name bio-lakehouse-peloton-normalizer --run-id "$PELO_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    MFP=$(aws glue get-job-run --job-name bio-lakehouse-mfp-normalizer --run-id "$MFP_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    echo "  $(date +%H:%M:%S) Oura=$OURA  HK=$HK  Peloton=$PELO  MFP=$MFP"

    FAILED=0
    for state in "$OURA" "$HK" "$PELO" "$MFP"; do
        if [ "$state" = "FAILED" ]; then
            FAILED=1
        fi
    done
    if [ "$FAILED" -eq 1 ]; then
        echo "  ERROR: A normalizer FAILED. Check AWS Glue console."
        exit 1
    fi

    if [ "$OURA" != "RUNNING" ] && [ "$OURA" != "STARTING" ] && \
       [ "$HK" != "RUNNING" ] && [ "$HK" != "STARTING" ] && \
       [ "$PELO" != "RUNNING" ] && [ "$PELO" != "STARTING" ] && \
       [ "$MFP" != "RUNNING" ] && [ "$MFP" != "STARTING" ]; then
        break
    fi
    sleep 20
done

echo "  All normalizers SUCCEEDED!"

# -----------------------------------------------
# STEP 6: Run Silver Crawler
# -----------------------------------------------
echo ""
echo "--- Step 6: Silver Crawler ---"

aws glue start-crawler --name bio-lakehouse-silver-crawler --region "$REGION"
echo "  Started silver crawler..."

while true; do
    STATE=$(aws glue get-crawler --name bio-lakehouse-silver-crawler --region "$REGION" \
        --query 'Crawler.State' --output text | head -1)
    echo "  $(date +%H:%M:%S) Silver crawler: $STATE"
    if [ "$STATE" = "READY" ]; then break; fi
    sleep 10
done

echo "  Silver crawler complete!"

# -----------------------------------------------
# STEP 7: Gold Refresh (Silver → Gold)
# -----------------------------------------------
echo ""
echo "--- Step 7: Gold Refresh ---"

GOLD_RUN=$(aws glue start-job-run --job-name bio-lakehouse-dbt-gold-refresh --region "$REGION" \
    --query 'JobRunId' --output text)

echo "  Started gold refresh: $GOLD_RUN"

while true; do
    STATE=$(aws glue get-job-run --job-name bio-lakehouse-dbt-gold-refresh --run-id "$GOLD_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    echo "  $(date +%H:%M:%S) Gold refresh: $STATE"
    if [ "$STATE" = "SUCCEEDED" ] || [ "$STATE" = "FAILED" ] || [ "$STATE" = "STOPPED" ]; then break; fi
    sleep 15
done

if [ "$STATE" != "SUCCEEDED" ]; then
    echo "  ERROR: Gold refresh $STATE. Check Glue console."
    exit 1
fi

echo "  Gold refresh SUCCEEDED!"

# -----------------------------------------------
# STEP 8: Gold Crawler
# -----------------------------------------------
echo ""
echo "--- Step 8: Gold Crawler ---"

aws glue start-crawler --name bio-lakehouse-gold-crawler --region "$REGION"
echo "  Started gold crawler..."

while true; do
    STATE=$(aws glue get-crawler --name bio-lakehouse-gold-crawler --region "$REGION" \
        --query 'Crawler.State' --output text | head -1)
    echo "  $(date +%H:%M:%S) Gold crawler: $STATE"
    if [ "$STATE" = "READY" ]; then break; fi
    sleep 10
done

echo "  Gold crawler complete!"

# -----------------------------------------------
# STEP 9: Verify Data
# -----------------------------------------------
echo ""
echo "--- Step 9: Verify Gold Data ---"

QID=$(aws athena start-query-execution \
    --query-string "SELECT date, readiness_score, sleep_score, activity_score, workout_count, hk_workout_count, resting_heart_rate_bpm, weight_lbs, daily_calories FROM bio_gold.daily_readiness_performance WHERE date >= '${YESTERDAY}' ORDER BY date DESC" \
    --query-execution-context Database=bio_gold \
    --result-configuration OutputLocation=s3://bio-lakehouse-athena-results-${AWS_ACCOUNT_ID}/ \
    --region "$REGION" --output text --query 'QueryExecutionId')

echo "  Athena query: $QID"
sleep 5

aws athena get-query-results --query-execution-id "$QID" --region "$REGION" --output table

# -----------------------------------------------
# STEP 10: Restart Streamlit
# -----------------------------------------------
echo ""
echo "--- Step 10: Restart Streamlit ---"

cd "$PROJECT_DIR"
# Kill existing Streamlit and restart in background (non-blocking)
/usr/sbin/lsof -ti :8501 | xargs kill -9 2>/dev/null || true
sleep 1
bash run_streamlit.sh &
echo "  Streamlit started (PID $!)"

# -----------------------------------------------
# STEP 11: Send Morning Briefing
# -----------------------------------------------
echo ""
echo "--- Step 11: Morning Briefing ---"

RESPONSE=$(aws lambda invoke \
    --function-name bio-lakehouse-morning-briefing \
    --region "$REGION" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    /tmp/briefing_response.json \
    --output text --query 'StatusCode' 2>/dev/null || echo "SKIP")

if [ "$RESPONSE" = "200" ]; then
    echo "  Morning briefing sent!"
elif [ "$RESPONSE" = "SKIP" ]; then
    echo "  Morning briefing Lambda not deployed yet — skipping."
else
    echo "  WARNING: Morning briefing returned status $RESPONSE"
    cat /tmp/briefing_response.json 2>/dev/null
fi

# -----------------------------------------------
# STEP 12: Weekly Correlation Discovery (Sundays only)
# -----------------------------------------------
if [ "$(date +%u)" = "7" ]; then
    echo ""
    echo "--- Step 12: Weekly Correlation Discovery ---"
    cd "$PROJECT_DIR"
    PYTHONPATH=$(pwd) .venv/bin/python scripts/run_correlation_discovery.py 2>&1 | tail -20
    echo "  Weekly discovery complete."
else
    echo ""
    echo "--- Step 12: Weekly Correlation Discovery (skipped — not Sunday) ---"
fi

echo ""
echo "========================================"
echo "Daily ingestion COMPLETE!"
echo "Streamlit: http://localhost:8501"
echo "========================================"
