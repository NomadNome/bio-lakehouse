#!/usr/bin/env bash
# batch_upload.sh — Sync files to Bronze and drop a manifest to trigger
# the normalizer exactly once (instead of per-file Lambda invocations).
#
# Usage:
#   ./scripts/batch_upload.sh <local_dir> <source_type>
#
# Example:
#   ./scripts/batch_upload.sh ./exports/healthkit/body healthkit/body

set -euo pipefail

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="bio-lakehouse-bronze-${ACCOUNT_ID}"

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <local_directory> <source_type>"
    echo "  source_type: oura/readiness, healthkit/body, peloton/workouts, etc."
    exit 1
fi

LOCAL_DIR="$1"
SOURCE_TYPE="$2"

if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Error: directory '$LOCAL_DIR' does not exist"
    exit 1
fi

# Count files to upload
FILE_COUNT=$(find "$LOCAL_DIR" -type f -name '*.csv' | wc -l | tr -d ' ')
if [[ "$FILE_COUNT" -eq 0 ]]; then
    echo "Error: no CSV files found in '$LOCAL_DIR'"
    exit 1
fi

BATCH_ID="${SOURCE_TYPE//\//-}-$(date -u +%Y-%m-%d)"
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "Batch upload: $FILE_COUNT files from '$LOCAL_DIR'"
echo "  Source type: $SOURCE_TYPE"
echo "  Batch ID:    $BATCH_ID"
echo "  Destination: s3://$BUCKET/$SOURCE_TYPE/"
echo ""

# 1. Sync files to Bronze prefix
echo "Syncing files..."
aws s3 sync "$LOCAL_DIR" "s3://$BUCKET/$SOURCE_TYPE/" \
    --sse AES256 \
    --exclude '*' --include '*.csv'
echo "Sync complete."
echo ""

# 2. Generate and upload manifest
MANIFEST=$(cat <<EOF
{
  "batch_id": "$BATCH_ID",
  "source_types": ["$SOURCE_TYPE"],
  "file_count": $FILE_COUNT,
  "uploaded_at": "$TIMESTAMP"
}
EOF
)

echo "Uploading manifest..."
echo "$MANIFEST" | aws s3 cp - "s3://$BUCKET/batch/_manifest.json" \
    --sse AES256 \
    --content-type "application/json"
echo ""

echo "Done. Manifest uploaded to s3://$BUCKET/batch/_manifest.json"
echo "Lambda will detect the manifest and trigger the normalizer once."
echo ""
echo "Manifest contents:"
echo "$MANIFEST"
