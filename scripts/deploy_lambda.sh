#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../lambda/ingestion_trigger"
zip -j /tmp/ingestion_trigger.zip handler.py
aws lambda update-function-code \
  --function-name bio-lakehouse-ingestion-trigger \
  --zip-file fileb:///tmp/ingestion_trigger.zip \
  --region us-east-1
echo "Lambda deployed successfully"
