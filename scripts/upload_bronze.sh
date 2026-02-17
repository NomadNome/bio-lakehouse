#!/usr/bin/env bash
# upload_bronze.sh — Upload date-partitioned Bronze data to S3
#
# Prerequisites:
#   1. Run split_oura_data.py and split_peloton_data.py first
#   2. AWS CLI configured with appropriate credentials
#   3. Bronze CloudFormation stack deployed
#
# Usage:
#   ./scripts/upload_bronze.sh [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STAGED_DIR="$PROJECT_DIR/bronze_staged"

# Get AWS account ID for bucket name
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "UNKNOWN")
BUCKET_NAME="bio-lakehouse-bronze-${AWS_ACCOUNT_ID}"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN MODE ==="
fi

echo "Bio Lakehouse - Bronze Upload"
echo "Source:  $STAGED_DIR"
echo "Target:  s3://$BUCKET_NAME"
echo "Account: $AWS_ACCOUNT_ID"
echo ""

# Check staged data exists
if [[ ! -d "$STAGED_DIR" ]]; then
    echo "ERROR: Staged data directory not found: $STAGED_DIR"
    echo "Run split_oura_data.py and split_peloton_data.py first."
    exit 1
fi

# Count files to upload
FILE_COUNT=$(find "$STAGED_DIR" -name "*.csv" -type f | wc -l | tr -d ' ')
echo "Found $FILE_COUNT CSV files to upload"
echo ""

if [[ "$FILE_COUNT" -eq 0 ]]; then
    echo "No files to upload. Exiting."
    exit 0
fi

# Upload Oura data
echo "--- Uploading Oura data ---"
for dataset in readiness sleep activity workout; do
    OURA_DIR="$STAGED_DIR/oura/$dataset"
    if [[ -d "$OURA_DIR" ]]; then
        count=$(find "$OURA_DIR" -name "*.csv" -type f | wc -l | tr -d ' ')
        echo "  oura/$dataset: $count files"
        if [[ "$DRY_RUN" == false ]]; then
            aws s3 cp "$OURA_DIR" "s3://$BUCKET_NAME/oura/$dataset/" \
                --recursive \
                --exclude "*" \
                --include "*.csv" \
                --sse AES256 \
                --quiet
        fi
    else
        echo "  oura/$dataset: SKIP (directory not found)"
    fi
done

echo ""

# Upload Peloton data
echo "--- Uploading Peloton data ---"
PELOTON_DIR="$STAGED_DIR/peloton/workouts"
if [[ -d "$PELOTON_DIR" ]]; then
    count=$(find "$PELOTON_DIR" -name "*.csv" -type f | wc -l | tr -d ' ')
    echo "  peloton/workouts: $count files"
    if [[ "$DRY_RUN" == false ]]; then
        aws s3 cp "$PELOTON_DIR" "s3://$BUCKET_NAME/peloton/workouts/" \
            --recursive \
            --exclude "*" \
            --include "*.csv" \
            --sse AES256 \
            --quiet
    fi
else
    echo "  peloton/workouts: SKIP (directory not found)"
fi

echo ""
echo "Upload complete!"
if [[ "$DRY_RUN" == true ]]; then
    echo "(Dry run — no files were actually uploaded)"
fi
