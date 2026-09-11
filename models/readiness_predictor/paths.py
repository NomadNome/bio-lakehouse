"""Instance-aware filesystem paths for readiness model artifacts."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _configured_model_root() -> Path:
    root = Path(os.environ.get("BIO_MODEL_DIR", "models"))
    return root if root.is_absolute() else PROJECT_ROOT / root


MODEL_ROOT = _configured_model_root()
MODEL_DIR = MODEL_ROOT / "readiness_predictor"
MODEL_PATH = MODEL_DIR / "model.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"
BACKTEST_PATH = MODEL_DIR / "backtest.csv"


def default_mlflow_db_path() -> Path:
    """Keep the original primary DB path while isolating named instances."""
    if os.environ.get("BIO_MODEL_DIR", "models") == "models":
        return PROJECT_ROOT / "mlflow.db"
    return MODEL_ROOT / "mlflow.db"
