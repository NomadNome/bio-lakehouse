#!/usr/bin/env bash
# Retrain the readiness predictor model (Phase 7)
# Run weekly via cron or manually after new data uploads
#
# Usage: ./scripts/retrain_model.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."

echo "=== Readiness Predictor Retraining (Phase 7) ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

cd "$PROJECT_DIR"

# Set MLflow tracking URI
export MLFLOW_TRACKING_URI="sqlite:///${PROJECT_DIR}/mlflow.db"

# Step 1: Rebuild feature table via dbt
echo "Rebuilding feature table..."
cd dbt_bio_lakehouse
dbt run --profiles-dir . --select feature_readiness_daily
cd "$PROJECT_DIR"

# Step 2: Retrain model (feature selection + multi-model comparison + Optuna tuning)
echo "Training model..."
python -m models.readiness_predictor.train

echo ""
echo "=== Retraining complete ==="
echo ""
echo "To view experiment history:"
echo "  mlflow ui --backend-store-uri $MLFLOW_TRACKING_URI"
echo "  → http://127.0.0.1:5000"
