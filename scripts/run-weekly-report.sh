#!/bin/bash
set -euo pipefail

PROJECT_DIR="/Users/nomathadejenkins/Desktop/Bio Lakehouse"
cd "$PROJECT_DIR"

# Load environment variables (ANTHROPIC_API_KEY, AWS config, etc.)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Activate virtual environment
source .venv/bin/activate

# Run the weekly report generator
python scripts/run_weekly_report.py "$@"
