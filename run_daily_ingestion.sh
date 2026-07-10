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
PROJECT_DIR="$HOME/Projects/bio-lakehouse"
VENV="$HOME/.local/share/bio-lakehouse-venv"
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

# Pick the newest file across inbox AND Downloads (whichever is fresher wins)
pick_newest() {
    local pattern="$1"
    { ls -t "$INBOX"/$pattern ~/Downloads/$pattern 2>/dev/null || true; } | head -1
}

HK_ZIP=$(pick_newest "export*.zip")
PELO_CSV=$(pick_newest "KnownasNoma_workouts*.csv")
MFP_CSV=$(pick_newest "Nutrition-Summary*.csv" || true)

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
"$VENV/bin/python" scripts/parse_healthkit_export.py \
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
"$VENV/bin/python3" -c "
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
    # ConcurrentRunsExceededException — attach to the existing RUNNING/STARTING run
    run_id=$(aws glue get-job-runs --job-name "$job_name" --region "$REGION" --max-results 5 \
        --query 'JobRuns[?JobRunState==`RUNNING` || JobRunState==`STARTING`]|[0].Id' --output text)
    if [ -z "$run_id" ] || [ "$run_id" = "None" ]; then
        echo "ERROR: could not start $job_name and no active run to attach to" >&2
        return 1
    fi
    echo "$run_id"
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
        case "$state" in
            FAILED|STOPPED|TIMEOUT|ERROR) FAILED=1 ;;
        esac
    done
    if [ "$FAILED" -eq 1 ]; then
        echo "  ERROR: A normalizer ended in a failed state (FAILED/STOPPED/TIMEOUT/ERROR). Check AWS Glue console."
        exit 1
    fi

    if [ "$OURA" = "SUCCEEDED" ] && [ "$HK" = "SUCCEEDED" ] && \
       [ "$PELO" = "SUCCEEDED" ] && [ "$MFP" = "SUCCEEDED" ]; then
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
    case "$STATE" in
        SUCCEEDED|FAILED|STOPPED|TIMEOUT|ERROR) break ;;
    esac
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

# Poll for completion (up to ~2 min) instead of a fixed sleep; a slow or failed
# verification query must not abort the pipeline before Streamlit/briefing run.
QSTATE="RUNNING"
for _ in $(seq 1 24); do
    QSTATE=$(aws athena get-query-execution --query-execution-id "$QID" --region "$REGION" \
        --query 'QueryExecution.Status.State' --output text)
    case "$QSTATE" in SUCCEEDED|FAILED|CANCELLED) break ;; esac
    sleep 5
done

if [ "$QSTATE" = "SUCCEEDED" ]; then
    aws athena get-query-results --query-execution-id "$QID" --region "$REGION" --output table
else
    echo "  WARNING: verification query ended in state $QSTATE — continuing anyway."
fi

# -----------------------------------------------
# STEP 10: Restart Streamlit
# -----------------------------------------------
echo ""
echo "--- Step 10: Restart Streamlit ---"

cd "$PROJECT_DIR"
# Kill existing Streamlit and restart in background (non-blocking)
# Use nohup + disown so Streamlit survives when the pipeline script exits
# (LaunchD kills child processes of completed jobs otherwise)
/usr/sbin/lsof -ti :8501 | xargs kill -9 2>/dev/null || true
sleep 1
nohup bash run_streamlit.sh > /dev/null 2>&1 &
disown
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
    PYTHONPATH="$PROJECT_DIR" BIO_PROJECT_ROOT="$PROJECT_DIR" "$VENV/bin/python" scripts/run_correlation_discovery.py 2>&1 | tail -20
    echo "  Weekly discovery complete."
else
    echo ""
    echo "--- Step 12: Weekly Correlation Discovery (skipped — not Sunday) ---"
fi

# -----------------------------------------------
# STEP 13: Cleanup Old Files
# -----------------------------------------------
echo ""
echo "--- Step 13: Cleanup Old Files ---"

# Keep only the 2 newest exports in inbox, delete the rest
for pattern in "export*.zip" "KnownasNoma_workouts*.csv" "Nutrition-Summary*.csv"; do
    { ls -t "$INBOX"/$pattern 2>/dev/null || true; } | tail -n +3 | while read f; do
        rm "$f" && echo "  Deleted old: $(basename "$f")"
    done
done

# Keep only the 2 newest exports in Downloads too
for pattern in "export*.zip" "KnownasNoma_workouts*.csv"; do
    { ls -t ~/Downloads/$pattern 2>/dev/null || true; } | tail -n +3 | while read f; do
        rm "$f" && echo "  Deleted old download: $(basename "$f")"
    done
done

echo "  Cleanup complete!"

echo ""
echo "========================================"
echo "Daily ingestion COMPLETE!"
echo "Streamlit: http://localhost:8501"
echo "========================================"
