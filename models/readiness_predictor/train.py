"""
Next-Day Readiness Predictor — Training Script

Trains a GradientBoostingRegressor to predict tomorrow morning's Oura readiness
score from today's data. Uses walk-forward time-series cross-validation.

Usage:
    python -m models.readiness_predictor.train
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline

# Feature columns used for prediction
FEATURE_COLS = [
    # Core biological signal
    "resting_hr",
    # Training load engine
    "ctl",
    "tsb",
    "tss",
    # Sleep architecture
    "sleep_score_3d_avg",
    "sleep_rem_pct",
    # Momentum metrics
    "readiness_7d_avg",
    "readiness_3d_slope",
]

TARGET_COL = "next_day_readiness"

MODEL_DIR = Path(__file__).parent
MODEL_PATH = MODEL_DIR / "model.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"


def load_feature_data() -> pd.DataFrame:
    """Load feature data from Athena via the project's AthenaClient."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from insights_engine.core.athena_client import AthenaClient

    athena = AthenaClient()
    df = athena.execute_query("""
        SELECT *
        FROM bio_gold_gold.feature_readiness_daily
        ORDER BY date
    """)
    return df


def walk_forward_cv(
    df: pd.DataFrame,
    min_train_size: int = 45,
    test_window: int = 14,
    step: int = 14,
) -> list[dict]:
    """
    Walk-forward time-series cross-validation.

    Train on first N days, predict next `test_window` days, slide forward by `step`.
    """
    results = []
    n = len(df)
    start = min_train_size

    while start + test_window <= n:
        train = df.iloc[:start]
        test = df.iloc[start : start + test_window]

        X_train = train[FEATURE_COLS].values
        y_train = train[TARGET_COL].values
        X_test = test[FEATURE_COLS].values
        y_test = test[TARGET_COL].values

        pipe = _build_pipeline()
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)

        fold_result = {
            "train_end": start,
            "test_start": start,
            "test_end": start + test_window,
            "mae": mean_absolute_error(y_test, y_pred),
            "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
            "r2": r2_score(y_test, y_pred),
        }
        results.append(fold_result)
        start += step

    return results


def _build_pipeline() -> Pipeline:
    """Build the imputer + model pipeline."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        (
            "model",
            GradientBoostingRegressor(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.05,
                min_samples_leaf=10,
                random_state=42,
            ),
        ),
    ])


def train_and_save() -> dict:
    """Train on all available data, evaluate with walk-forward CV, save model."""
    print("Loading feature data from Athena...")
    df = load_feature_data()

    # Ensure numeric types
    for col in FEATURE_COLS + [TARGET_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows without a target (last row has no next-day readiness)
    df = df.dropna(subset=[TARGET_COL]).reset_index(drop=True)
    print(f"Training samples: {len(df)}")

    if len(df) < 90:
        print(f"WARNING: Only {len(df)} samples. Model may have limited accuracy.")

    # Walk-forward cross-validation
    print("Running walk-forward cross-validation...")
    cv_results = walk_forward_cv(df)

    avg_mae = np.mean([r["mae"] for r in cv_results])
    avg_rmse = np.mean([r["rmse"] for r in cv_results])
    avg_r2 = np.mean([r["r2"] for r in cv_results])

    print(f"\nCross-Validation Results ({len(cv_results)} folds):")
    print(f"  MAE:  {avg_mae:.2f}")
    print(f"  RMSE: {avg_rmse:.2f}")
    print(f"  R²:   {avg_r2:.3f}")

    # Train final model on all data
    print("\nTraining final model on all data...")
    X = df[FEATURE_COLS].values
    y = df[TARGET_COL].values

    pipe = _build_pipeline()
    pipe.fit(X, y)

    # Feature importances
    model = pipe.named_steps["model"]
    importances = dict(zip(FEATURE_COLS, model.feature_importances_.tolist()))
    sorted_importances = dict(
        sorted(importances.items(), key=lambda x: x[1], reverse=True)
    )

    print("\nTop 10 Feature Importances:")
    for feat, imp in list(sorted_importances.items())[:10]:
        print(f"  {feat:30s} {imp:.4f}")

    # In-sample backtest (for Streamlit visualization)
    y_pred_all = pipe.predict(X)
    backtest = df[["date", TARGET_COL]].copy()
    backtest["predicted"] = y_pred_all
    backtest.to_csv(MODEL_DIR / "backtest.csv", index=False)

    # Save model
    joblib.dump(pipe, MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH}")

    # Save metrics
    metrics = {
        "n_samples": len(df),
        "cv_folds": len(cv_results),
        "cv_mae": round(avg_mae, 2),
        "cv_rmse": round(avg_rmse, 2),
        "cv_r2": round(avg_r2, 3),
        "feature_importances": sorted_importances,
        "cv_details": cv_results,
    }
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics saved to {METRICS_PATH}")

    return metrics


if __name__ == "__main__":
    train_and_save()
