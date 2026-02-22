#!/usr/bin/env bash
# Run dbt build for the Bio Lakehouse project
# Usage: ./scripts/run_dbt.sh [dbt-args]
#
# Examples:
#   ./scripts/run_dbt.sh                    # Full build (run + test)
#   ./scripts/run_dbt.sh --select gold      # Only gold models
#   ./scripts/run_dbt.sh --select analytics # Only analytics views

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."
DBT_DIR="$PROJECT_DIR/dbt_bio_lakehouse"

echo "=== Bio Lakehouse dbt Build ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

cd "$DBT_DIR"

# Run dbt build (compile + run + test)
dbt build --profiles-dir . "$@"

echo ""
echo "=== dbt build complete ==="
