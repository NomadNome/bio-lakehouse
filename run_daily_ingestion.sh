#!/bin/bash
# Bio Lakehouse - Daily Ingestion (Steps 4-10: AWS Pipeline)
# Generated: 2026-03-04
# Run this from your Mac Terminal after HealthKit/Peloton parsing is complete.
# Parsed data should already be in /tmp/healthkit_daily_csvs and /tmp/peloton_daily_split

set -euo pipefail

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="bio-lakehouse-bronze-${AWS_ACCOUNT_ID}"
REGION="us-east-1"
PROJECT_DIR="$HOME/Desktop/Bio Lakehouse"
BATCH_DATE=$(date +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)

echo "========================================"
echo "Bio Lakehouse Daily Ingestion"
echo "Date: $BATCH_DATE"
echo "========================================"

# -----------------------------------------------
# STEP 1-3: LOCAL PARSING (already done by Cowork)
# If you need to re-run parsing, uncomment below:
# -----------------------------------------------
# HK_ZIP=$(ls -t ~/Downloads/export*.zip 2>/dev/null | head -1)
# PELO_CSV=$(ls -t ~/Downloads/KnownasNoma_workouts*.csv 2>/dev/null | head -1)
# rm -rf /tmp/healthkit_daily_parse /tmp/healthkit_daily_csvs
# mkdir -p /tmp/healthkit_daily_parse /tmp/healthkit_daily_csvs
# unzip -q "$HK_ZIP" -d /tmp/healthkit_daily_parse
# SINCE_DATE=$(date -v-1d +%Y-%m-%d)
# cd "$PROJECT_DIR"
# .venv/bin/python scripts/parse_healthkit_export.py \
#     --input /tmp/healthkit_daily_parse/apple_health_export/export.xml \
#     --since "$SINCE_DATE" \
#     --output-dir /tmp/healthkit_daily_csvs

# Use bronze_staged copies (placed by Cowork) if /tmp doesn't have them
if [ ! -d "/tmp/healthkit_daily_csvs/daily_vitals" ]; then
    echo "Restoring parsed data from bronze_staged..."
    rm -rf /tmp/healthkit_daily_csvs /tmp/peloton_daily_split
    mkdir -p /tmp/healthkit_daily_csvs /tmp/peloton_daily_split
    cp -r "$PROJECT_DIR/bronze_staged/healthkit_daily/"* /tmp/healthkit_daily_csvs/ 2>/dev/null || true
    cp -r "$PROJECT_DIR/bronze_staged/peloton_daily/"* /tmp/peloton_daily_split/ 2>/dev/null || true
fi

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
        aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet --region "$REGION"
        echo "    Uploaded: ${s3key}"
    done
done

echo "  Uploading Peloton CSVs..."
PELO_COUNT=0
for csv_file in /tmp/peloton_daily_split/year=*/month=*/day=*/*.csv; do
    [ -f "$csv_file" ] || continue
    part=$(echo "$csv_file" | grep -o 'year=.*')
    s3key="peloton/workouts/${part}"
    aws s3 cp "$csv_file" "s3://${BUCKET}/${s3key}" --quiet --region "$REGION"
    echo "    Uploaded: ${s3key}"
    PELO_COUNT=$((PELO_COUNT + 1))
done

echo "  Uploading batch manifests..."
HK_FILE_COUNT=$(find /tmp/healthkit_daily_csvs -name '*.csv' | wc -l | tr -d ' ')
cat > /tmp/hk_manifest.json <<EOF
{"batch_id": "healthkit-${BATCH_DATE}", "source_types": ["healthkit/daily_vitals", "healthkit/workouts", "healthkit/body", "healthkit/mindfulness"], "file_count": ${HK_FILE_COUNT}}
EOF
aws s3 cp /tmp/hk_manifest.json "s3://${BUCKET}/healthkit/healthkit-${BATCH_DATE}_manifest.json" --quiet --region "$REGION"
echo "    Uploaded: healthkit manifest (${HK_FILE_COUNT} files)"

if [ "$PELO_COUNT" -gt 0 ]; then
    cat > /tmp/pelo_manifest.json <<EOF
{"batch_id": "peloton-${BATCH_DATE}", "source_types": ["peloton/workouts"], "file_count": ${PELO_COUNT}}
EOF
    aws s3 cp /tmp/pelo_manifest.json "s3://${BUCKET}/peloton/peloton-${BATCH_DATE}_manifest.json" --quiet --region "$REGION"
    echo "    Uploaded: peloton manifest (${PELO_COUNT} files)"
fi

echo "  Bronze upload complete!"

# -----------------------------------------------
# STEP 5: Run Glue Normalizers (Bronze → Silver)
# -----------------------------------------------
echo ""
echo "--- Step 5: Run Glue Normalizers ---"

OURA_RUN=$(aws glue start-job-run --job-name bio-lakehouse-oura-normalizer --region "$REGION" \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-${AWS_ACCOUNT_ID}","--source_type":"oura"}' \
    --query 'JobRunId' --output text)

HK_RUN=$(aws glue start-job-run --job-name bio-lakehouse-healthkit-normalizer --region "$REGION" \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-${AWS_ACCOUNT_ID}","--source_type":"healthkit"}' \
    --query 'JobRunId' --output text)

PELO_RUN=$(aws glue start-job-run --job-name bio-lakehouse-peloton-normalizer --region "$REGION" \
    --arguments '{"--source_bucket":"bio-lakehouse-bronze-${AWS_ACCOUNT_ID}","--source_type":"peloton"}' \
    --query 'JobRunId' --output text)

echo "  Started: Oura=$OURA_RUN  HK=$HK_RUN  Peloton=$PELO_RUN"
echo "  Polling (expect ~13 min for HealthKit)..."

while true; do
    OURA=$(aws glue get-job-run --job-name bio-lakehouse-oura-normalizer --run-id "$OURA_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    HK=$(aws glue get-job-run --job-name bio-lakehouse-healthkit-normalizer --run-id "$HK_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    PELO=$(aws glue get-job-run --job-name bio-lakehouse-peloton-normalizer --run-id "$PELO_RUN" \
        --region "$REGION" --query 'JobRun.JobRunState' --output text | head -1)
    echo "  $(date +%H:%M:%S) Oura=$OURA  HK=$HK  Peloton=$PELO"

    FAILED=0
    for state in "$OURA" "$HK" "$PELO"; do
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
       [ "$PELO" != "RUNNING" ] && [ "$PELO" != "STARTING" ]; then
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
bash run_streamlit.sh

echo ""
echo "========================================"
echo "Daily ingestion COMPLETE!"
echo "Streamlit: http://localhost:8501"
echo "========================================"
