#!/usr/bin/env bash
# Retrain the readiness predictor model (Phase 7)
# Run weekly via cron or manually after new data uploads
#
# Usage:
#   ./scripts/retrain_model.sh
#   ENV_FILE=.env.diego ./scripts/retrain_model.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."
VENV="${BIO_VENV:-$HOME/.local/share/bio-lakehouse-venv}"
ENV_FILE="${ENV_FILE:-$PROJECT_DIR/.env}"

if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

PYTHON_BIN="${PYTHON_BIN:-$VENV/bin/python}"
DBT_BIN="${DBT_BIN:-$VENV/bin/dbt}"
MODEL_ROOT="${BIO_MODEL_DIR:-models}"
case "$MODEL_ROOT" in
    /*) ;;
    *) MODEL_ROOT="$PROJECT_DIR/$MODEL_ROOT" ;;
esac

export BIO_PROJECT_ROOT="$PROJECT_DIR"
export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-sqlite:///${MODEL_ROOT%/}/mlflow.db}"

echo "=== Readiness Predictor Retraining (Phase 7) ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

cd "$PROJECT_DIR"

# Step 1: Rebuild feature table via dbt
echo "Rebuilding feature table..."
cd dbt_bio_lakehouse
"$DBT_BIN" run --profiles-dir . --select feature_readiness_daily
cd "$PROJECT_DIR"

# Step 2: Retrain model (feature selection + multi-model comparison + Optuna tuning)
echo "Training model..."
"$PYTHON_BIN" -m models.readiness_predictor.train

echo ""
echo "=== Retraining complete ==="
echo ""
echo "To view experiment history:"
echo "  mlflow ui --backend-store-uri $MLFLOW_TRACKING_URI"
echo "  → http://127.0.0.1:5000"
