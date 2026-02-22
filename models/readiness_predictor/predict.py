"""
Next-Day Readiness Predictor — Inference Script

Loads the trained model and predicts tomorrow's readiness score
from the most recent day's feature data.

Usage:
    python -m models.readiness_predictor.predict
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# Ensure project root is on path before importing sibling module
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from models.readiness_predictor.train import FEATURE_COLS, MODEL_PATH

MODEL_DIR = Path(__file__).parent
METRICS_PATH = MODEL_DIR / "metrics.json"


def load_latest_features() -> pd.DataFrame:
    """Load the most recent row from the feature table."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from insights_engine.core.athena_client import AthenaClient

    athena = AthenaClient()
    df = athena.execute_query("""
        SELECT *
        FROM bio_gold_gold.feature_readiness_daily
        ORDER BY date DESC
        LIMIT 1
    """)
    return df


def predict_next_day() -> dict:
    """Predict tomorrow's readiness score."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"No trained model found at {MODEL_PATH}. Run train.py first."
        )

    pipe = joblib.load(MODEL_PATH)
    df = load_latest_features()

    if df.empty:
        raise ValueError("No feature data available. Run dbt build first.")

    # Ensure numeric
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    latest_date = df["date"].iloc[0]
    X = df[FEATURE_COLS].values

    prediction = pipe.predict(X)[0]
    prediction = float(np.clip(prediction, 0, 100))

    # Load model metrics for confidence context
    confidence = {}
    if METRICS_PATH.exists():
        with open(METRICS_PATH) as f:
            metrics = json.load(f)
        mae = metrics.get("cv_mae", 0)
        confidence = {
            "range_low": round(max(0, prediction - mae), 1),
            "range_high": round(min(100, prediction + mae), 1),
            "cv_mae": mae,
            "cv_r2": metrics.get("cv_r2", 0),
            "n_training_samples": metrics.get("n_samples", 0),
        }

    # Feature contributions (approximate via feature importances)
    importances = {}
    if METRICS_PATH.exists():
        with open(METRICS_PATH) as f:
            metrics = json.load(f)
        importances = metrics.get("feature_importances", {})

    result = {
        "prediction_date": latest_date,
        "predicted_readiness": round(prediction, 1),
        "confidence": confidence,
        "feature_importances": importances,
        "input_features": {
            col: float(df[col].iloc[0]) if pd.notna(df[col].iloc[0]) else None
            for col in FEATURE_COLS
        },
    }

    return result


if __name__ == "__main__":
    result = predict_next_day()
    print(f"\nPrediction for day after {result['prediction_date']}:")
    print(f"  Predicted readiness: {result['predicted_readiness']}")
    if result["confidence"]:
        c = result["confidence"]
        print(f"  Confidence range:    {c['range_low']} - {c['range_high']}")
        print(f"  Model MAE:           {c['cv_mae']}")
        print(f"  Model R²:            {c['cv_r2']}")
    print("\nTop input features:")
    for feat, val in list(result["input_features"].items())[:10]:
        print(f"  {feat:30s} = {val}")
