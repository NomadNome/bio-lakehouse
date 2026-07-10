#!/bin/bash
set -euo pipefail

PROJECT_DIR="$HOME/Projects/bio-lakehouse"
VENV="$HOME/.local/share/bio-lakehouse-venv"
cd "$PROJECT_DIR"

# Load environment variables (ANTHROPIC_API_KEY, AWS config, etc.)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export BIO_PROJECT_ROOT="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR"

"$VENV/bin/python" scripts/run_weekly_report.py "$@"
