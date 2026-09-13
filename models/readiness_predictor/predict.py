"""
Next-Day Readiness Predictor — Inference Script (Phase 7)

Loads the trained model (MLflow registry or local joblib) and predicts
tomorrow's readiness score from the most recent day's feature data.

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

from models.readiness_predictor.paths import METRICS_PATH, MODEL_PATH


def _load_feature_cols() -> list[str]:
    """Load feature column list from metrics.json."""
    if METRICS_PATH.exists():
        with open(METRICS_PATH) as f:
            metrics = json.load(f)
        cols = metrics.get("feature_cols")
        if cols:
            return cols

    # Fallback to legacy hardcoded features
    return [
        "resting_hr", "ctl", "tsb", "tss",
        "sleep_score_3d_avg", "rem_sleep_score",
        "readiness_7d_avg", "readiness_3d_slope",
    ]


def _load_model():
    """Load model from MLflow registry first, then fall back to local joblib."""
    try:
        from models.readiness_predictor.mlflow_config import get_best_model_uri
        import mlflow

        uri = get_best_model_uri()
        if uri:
            return mlflow.sklearn.load_model(uri)
    except (ImportError, Exception):
        pass

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"No trained model found at {MODEL_PATH}. Run train.py first."
        )
    return joblib.load(MODEL_PATH)


def load_latest_features() -> pd.DataFrame:
    """Load the most recent row from the feature table."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from insights_engine.core.athena_client import AthenaClient

    athena = AthenaClient()
    df = athena.execute_query("""
        SELECT *
        FROM feature_readiness_daily
        ORDER BY date DESC
        LIMIT 1
    """)
    return df


def predict_next_day() -> dict:
    """Predict tomorrow's readiness score."""
    feature_cols = _load_feature_cols()
    df = load_latest_features()

    if df.empty:
        raise ValueError("No feature data available. Run dbt build first.")

    # Ensure numeric
    for col in feature_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    latest_date = df["date"].iloc[0]

    metrics = {}
    if METRICS_PATH.exists():
        with open(METRICS_PATH) as f:
            metrics = json.load(f)

    use_baseline = (
        metrics.get("model_recommended") is False
        and "readiness_7d_avg" in df.columns
        and pd.notna(df["readiness_7d_avg"].iloc[0])
    )
    if use_baseline:
        prediction = float(df["readiness_7d_avg"].iloc[0])
        prediction_source = "rolling_7d_baseline"
        model_prediction = None
    else:
        pipe = _load_model()
        model_prediction = pipe.predict(df[feature_cols].values)[0]
        prediction = model_prediction
        prediction_source = "model"
    prediction = float(np.clip(prediction, 0, 100))

    # Load held-out evaluation and empirically calibrated error context.
    # A single MAE is not a confidence interval, so older metrics deliberately
    # do not produce a range.
    confidence = {}
    if metrics:
        evaluation = metrics.get("evaluation", {})
        holdout_metrics = evaluation.get("model", {})
        interval = metrics.get("prediction_interval", {})
        error_radius = interval.get("absolute_error_p80")
        confidence = {
            "cv_mae": metrics.get("cv_mae"),
            "cv_r2": metrics.get("cv_r2", 0),
            "holdout_mae": holdout_metrics.get("mae"),
            "holdout_r2": holdout_metrics.get("r2"),
            "n_training_samples": metrics.get("n_samples", 0),
            "best_model": metrics.get("best_model", "unknown"),
            "sample_size_warning": metrics.get("sample_size_warning", False),
            "model_recommended": metrics.get("model_recommended"),
        }
        if error_radius is not None:
            confidence.update({
                "range_low": round(max(0, prediction - error_radius), 1),
                "range_high": round(min(100, prediction + error_radius), 1),
                "interval_level": 0.80,
                "interval_method": interval.get("method"),
                "n_calibration": interval.get("n_calibration"),
            })

    importances = metrics.get("feature_importances", {})

    result = {
        "prediction_date": latest_date,
        "predicted_readiness": round(prediction, 1),
        "prediction_source": prediction_source,
        "model_prediction": (
            round(float(np.clip(model_prediction, 0, 100)), 1)
            if model_prediction is not None
            else None
        ),
        "confidence": confidence,
        "feature_importances": importances,
        "input_features": {
            col: float(df[col].iloc[0]) if col in df.columns and pd.notna(df[col].iloc[0]) else None
            for col in feature_cols
        },
    }

    return result


if __name__ == "__main__":
    result = predict_next_day()
    print(f"\nPrediction for day after {result['prediction_date']}:")
    print(f"  Predicted readiness: {result['predicted_readiness']}")
    print(f"  Prediction source:   {result['prediction_source']}")
    if result["confidence"]:
        c = result["confidence"]
        if "range_low" in c:
            print(f"  Empirical 80% range: {c['range_low']} - {c['range_high']}")
        print(f"  Model:               {c.get('best_model', '?')}")
        print(f"  Holdout MAE:         {c.get('holdout_mae', '?')}")
        print(f"  Holdout R²:          {c.get('holdout_r2', '?')}")
        if c.get("sample_size_warning"):
            print("  WARNING: Model trained on < 50 samples — predictions may be unreliable")
    print("\nTop input features:")
    for feat, val in list(result["input_features"].items())[:10]:
        print(f"  {feat:30s} = {val}")
