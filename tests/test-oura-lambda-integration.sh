#!/bin/bash

# Integration test for Oura Lambda
# Invokes the deployed Lambda function and verifies S3/DynamoDB results

set -e

echo "🧪 Oura Lambda Integration Test"
echo

# Configuration (override with environment variables if needed)
LAMBDA_NAME="${LAMBDA_NAME:-bio-lakehouse-oura-api-ingest}"
BUCKET="${BRONZE_BUCKET:-bio-lakehouse-bronze-YOUR_AWS_ACCOUNT}"
TABLE="${INGESTION_TABLE:-bio_ingestion_log}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test 1: Verify Lambda exists
echo -n "1️⃣  Checking Lambda function exists... "
if aws lambda get-function --function-name "$LAMBDA_NAME" &>/dev/null; then
  echo -e "${GREEN}✓${NC}"
else
  echo -e "${RED}✗${NC}"
  echo "   Lambda function $LAMBDA_NAME not found"
  exit 1
fi

# Test 2: Verify EventBridge rule
echo -n "2️⃣  Checking EventBridge rule... "
if aws events describe-rule --name bio-lakehouse-oura-daily-ingest &>/dev/null; then
  echo -e "${GREEN}✓${NC}"
else
  echo -e "${RED}✗${NC}"
  echo "   EventBridge rule not found"
  exit 1
fi

# Test 3: Verify SSM parameter
echo -n "3️⃣  Checking Oura API token in SSM... "
if aws ssm get-parameter --name /bio-lakehouse/oura-api-token --with-decryption &>/dev/null; then
  echo -e "${GREEN}✓${NC}"
else
  echo -e "${RED}✗${NC}"
  echo "   SSM parameter /bio-lakehouse/oura-api-token not found"
  exit 1
fi

# Test 4: Invoke Lambda (dry run with test date)
echo -n "4️⃣  Invoking Lambda (test mode)... "
INVOKE_OUTPUT=$(aws lambda invoke \
  --function-name "$LAMBDA_NAME" \
  --payload '{"test": true, "date": "2026-02-17"}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/lambda-test-response.json 2>&1)

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓${NC}"
  
  # Check response
  RESPONSE=$(cat /tmp/lambda-test-response.json)
  echo "   Response: $RESPONSE"
  
  # Verify statusCode
  STATUS_CODE=$(echo "$RESPONSE" | jq -r '.statusCode' 2>/dev/null || echo "unknown")
  if [ "$STATUS_CODE" = "200" ]; then
    echo -e "   ${GREEN}Lambda returned 200 OK${NC}"
  else
    echo -e "   ${YELLOW}Lambda returned status: $STATUS_CODE${NC}"
  fi
else
  echo -e "${RED}✗${NC}"
  echo "   Invocation failed: $INVOKE_OUTPUT"
  exit 1
fi

# Test 5: Verify S3 files exist
echo -n "5️⃣  Checking S3 bronze files... "
FILE_COUNT=$(aws s3 ls s3://$BUCKET/oura/ --recursive | wc -l | xargs)
if [ "$FILE_COUNT" -gt 0 ]; then
  echo -e "${GREEN}✓${NC} ($FILE_COUNT files)"
else
  echo -e "${YELLOW}⚠${NC} (no files yet, may be first run)"
fi

# Test 6: Verify DynamoDB logs
echo -n "6️⃣  Checking DynamoDB logs... "
LOG_COUNT=$(aws dynamodb scan \
  --table-name "$TABLE" \
  --filter-expression "#src = :source" \
  --expression-attribute-names '{"#src": "source"}' \
  --expression-attribute-values '{":source": {"S": "oura"}}' \
  --select COUNT \
  --query 'Count' \
  --output text 2>/dev/null || echo "0")

if [ "$LOG_COUNT" -gt 0 ]; then
  echo -e "${GREEN}✓${NC} ($LOG_COUNT records)"
else
  echo -e "${YELLOW}⚠${NC} (no logs yet, may be first run)"
fi

# Test 7: Check latest ingestion
echo
echo "📊 Latest Oura ingestions:"
aws dynamodb scan \
  --table-name "$TABLE" \
  --filter-expression "#src = :source" \
  --expression-attribute-names '{"#src": "source"}' \
  --expression-attribute-values '{":source": {"S": "oura"}}' \
  --max-items 5 \
  --query 'Items[*].[data_type.S, record_count.N, upload_timestamp.N, file_path.S]' \
  --output table 2>/dev/null || echo "No records found"

echo
echo -e "${GREEN}✅ Integration tests complete${NC}"
echo
