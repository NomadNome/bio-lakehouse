#!/bin/bash
# Peloton workout export → S3 bronze bucket sync
# Usage: ./peloton-sync.sh
# Note: Assumes CSV has already been downloaded to ~/Downloads

set -euo pipefail

# Config
# Set your S3 bronze bucket name (or use environment variable)
BRONZE_BUCKET="${BRONZE_BUCKET:-bio-lakehouse-bronze-YOUR_AWS_ACCOUNT}"
DOWNLOAD_DIR="$HOME/Downloads"
S3_PREFIX="peloton/workouts"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "🏋️  Starting Peloton workout sync..."

# Step 1: Find the most recent CSV
echo "🔍 Locating downloaded file..."
LATEST_CSV=$(ls -t "$DOWNLOAD_DIR"/YOUR_PELOTON_USERNAME_workouts*.csv 2>/dev/null | head -1)

if [ -z "$LATEST_CSV" ]; then
  echo "❌ No workout CSV found in Downloads"
  exit 1
fi

echo "📄 Found: $LATEST_CSV"

# Step 3: Upload to S3 bronze bucket
echo "☁️  Uploading to S3..."
S3_KEY="${S3_PREFIX}/YOUR_PELOTON_USERNAME_workouts_${TIMESTAMP}.csv"
aws s3 cp "$LATEST_CSV" "s3://${BRONZE_BUCKET}/${S3_KEY}" \
  --sse AES256 \
  --metadata "source=peloton,ingestion_ts=${TIMESTAMP},type=workouts"

echo "✅ Uploaded to s3://${BRONZE_BUCKET}/${S3_KEY}"

# Step 4: Log ingestion to DynamoDB
echo "📝 Logging ingestion..."
RECORD_COUNT=$(tail -n +2 "$LATEST_CSV" | wc -l | tr -d ' ')
UPLOAD_TS_UNIX=$(date +%s)
UPLOAD_TS_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
aws dynamodb put-item \
  --table-name bio_ingestion_log \
  --item "{
    \"file_path\": {\"S\": \"s3://${BRONZE_BUCKET}/${S3_KEY}\"},
    \"upload_timestamp\": {\"N\": \"${UPLOAD_TS_UNIX}\"},
    \"source\": {\"S\": \"peloton\"},
    \"data_type\": {\"S\": \"workouts\"},
    \"status\": {\"S\": \"completed\"},
    \"record_count\": {\"N\": \"${RECORD_COUNT}\"},
    \"upload_timestamp_iso\": {\"S\": \"${UPLOAD_TS_ISO}\"}
  }" && echo "✅ Logged to DynamoDB" || echo "⚠️  DynamoDB logging failed"

# Optional: Clean up local file after 7 days
find "$DOWNLOAD_DIR" -name "YOUR_PELOTON_USERNAME_workouts*.csv" -mtime +7 -delete 2>/dev/null || true

echo "🎉 Peloton sync complete!"
