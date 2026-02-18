#!/bin/bash
# Oura Ring data sync to S3 bronze bucket
# Pulls: readiness, sleep, activity, heart rate, SpO2
# Usage: ./oura-sync.sh [YYYY-MM-DD]

set -euo pipefail

# Load credentials
source "$(dirname "$0")/../.env.oura"

# Config
# Set your S3 bronze bucket name (or use environment variable)
BRONZE_BUCKET="${BRONZE_BUCKET:-bio-lakehouse-bronze-YOUR_AWS_ACCOUNT}"
S3_PREFIX="oura"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TMP_DIR="/tmp/oura-sync-$$"

# Date range (defaults to last 7 days through yesterday)
END_DATE="${1:-$(date -v-1d +%Y-%m-%d)}"
START_DATE=$(date -v-7d +%Y-%m-%d)

echo "🔵 Starting Oura data sync..."
echo "📅 Date range: $START_DATE to $END_DATE"
echo ""

mkdir -p "$TMP_DIR"

# Function to fetch and upload endpoint data
fetch_and_upload() {
  local endpoint="$1"
  local data_type="$2"
  
  echo "📥 Fetching $data_type..."
  
  local response=$(curl -s "https://api.ouraring.com/v2/usercollection/$endpoint?start_date=$START_DATE&end_date=$END_DATE" \
    -H "Authorization: Bearer $OURA_ACCESS_TOKEN")
  
  local record_count=$(echo "$response" | jq -r '.data | length')
  
  if [[ "$record_count" -eq 0 ]]; then
    echo "⚠️  No $data_type data for this period"
    return 0
  fi
  
  # Save to temp file
  local tmp_file="$TMP_DIR/${data_type}_${END_DATE}.json"
  echo "$response" | jq '.data' > "$tmp_file"
  
  # Upload to S3
  local s3_key="${S3_PREFIX}/${data_type}/year=$(date -j -f "%Y-%m-%d" "$END_DATE" +%Y)/month=$(date -j -f "%Y-%m-%d" "$END_DATE" +%m)/day=$(date -j -f "%Y-%m-%d" "$END_DATE" +%d)/${data_type}_${TIMESTAMP}.json"
  
  aws s3 cp "$tmp_file" "s3://${BRONZE_BUCKET}/${s3_key}" \
    --sse AES256 \
    --metadata "source=oura,data_type=${data_type},date=${END_DATE}" \
    --quiet
  
  echo "✅ Uploaded $record_count $data_type records to s3://${BRONZE_BUCKET}/${s3_key}"
  
  # Log to DynamoDB
  local upload_ts_unix=$(date +%s)
  local upload_ts_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  
  aws dynamodb put-item \
    --table-name bio_ingestion_log \
    --item "{
      \"file_path\": {\"S\": \"s3://${BRONZE_BUCKET}/${s3_key}\"},
      \"upload_timestamp\": {\"N\": \"${upload_ts_unix}\"},
      \"source\": {\"S\": \"oura\"},
      \"data_type\": {\"S\": \"${data_type}\"},
      \"status\": {\"S\": \"completed\"},
      \"record_count\": {\"N\": \"${record_count}\"},
      \"upload_timestamp_iso\": {\"S\": \"${upload_ts_iso}\"},
      \"date_range\": {\"S\": \"${START_DATE} to ${END_DATE}\"}
    }" > /dev/null
}

# Fetch all data types
fetch_and_upload "daily_readiness" "readiness"
fetch_and_upload "daily_sleep" "sleep"
fetch_and_upload "daily_activity" "activity"
fetch_and_upload "heartrate" "heartrate"
fetch_and_upload "daily_spo2" "spo2"

# Cleanup
rm -rf "$TMP_DIR"

echo "🎉 Oura sync complete!"
