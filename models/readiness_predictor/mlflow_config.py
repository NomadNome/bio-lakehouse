"""
MLflow Configuration for Readiness Predictor

Sets up local MLflow tracking with SQLite backend.
Each training run logs parameters, metrics, and model artifacts.
"""

from __future__ import annotations

import os
from pathlib import Path

MLFLOW_DIR = Path(__file__).resolve().parents[2]
TRACKING_URI = f"sqlite:///{MLFLOW_DIR / 'mlflow.db'}"
EXPERIMENT_NAME = "readiness-predictor"


def setup_mlflow():
    """Configure MLflow tracking. Returns the mlflow module."""
    import mlflow

    os.environ["MLFLOW_TRACKING_URI"] = TRACKING_URI
    mlflow.set_tracking_uri(TRACKING_URI)

    # Create or get experiment
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        mlflow.create_experiment(EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)

    return mlflow


def get_best_model_uri() -> str | None:
    """Return the MLflow model URI for the run tagged best_model=True, or None."""
    try:
        import mlflow

        mlflow.set_tracking_uri(TRACKING_URI)
        experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
        if experiment is None:
            return None

        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="tags.best_model = 'True'",
            order_by=["start_time DESC"],
            max_results=1,
        )
        if runs.empty:
            return None

        run_id = runs.iloc[0]["run_id"]
        return f"runs:/{run_id}/model"
    except Exception:
        return None
