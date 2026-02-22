"""
Next-Day Readiness Predictor — Training Script (Phase 7)

Trains multiple model candidates, evaluates with walk-forward CV,
tunes top performers with Optuna, and logs everything to MLflow.

Models (conservative for small N):
  - Ridge, ElasticNet, GradientBoosting, XGBoost, LightGBM

Usage:
    python -m models.readiness_predictor.train
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

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


def _get_model_catalog() -> dict:
    """Return model candidates with conservative hyperparameters for small N."""
    catalog = {
        "Ridge": Ridge(alpha=10.0),
        "ElasticNet": ElasticNet(alpha=1.0, l1_ratio=0.5, max_iter=5000),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=50, max_depth=2, min_samples_leaf=15,
            learning_rate=0.05, random_state=42,
        ),
    }

    # Optional: XGBoost
    try:
        from xgboost import XGBRegressor
        catalog["XGBoost"] = XGBRegressor(
            n_estimators=50, max_depth=2, reg_alpha=1.0, reg_lambda=5.0,
            learning_rate=0.05, random_state=42, verbosity=0,
        )
    except ImportError:
        print("XGBoost not installed, skipping.")

    # Optional: LightGBM
    try:
        from lightgbm import LGBMRegressor
        catalog["LightGBM"] = LGBMRegressor(
            n_estimators=50, max_depth=2, min_child_samples=15,
            learning_rate=0.05, random_state=42, verbose=-1,
        )
    except ImportError:
        print("LightGBM not installed, skipping.")

    return catalog


def _build_pipeline(model) -> Pipeline:
    """Build imputer + scaler + model pipeline."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", model),
    ])


def walk_forward_cv(
    df: pd.DataFrame,
    feature_cols: list[str],
    model,
    min_train_size: int = 30,
    test_window: int = 7,
    step: int = 7,
) -> list[dict]:
    """Walk-forward time-series cross-validation."""
    results = []
    n = len(df)
    start = min_train_size

    while start + test_window <= n:
        train = df.iloc[:start]
        test = df.iloc[start : start + test_window]

        X_train = train[feature_cols].values
        y_train = train[TARGET_COL].values
        X_test = test[feature_cols].values
        y_test = test[TARGET_COL].values

        pipe = _build_pipeline(model)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)

        fold_result = {
            "train_end": int(start),
            "test_start": int(start),
            "test_end": int(start + test_window),
            "n_train": int(start),
            "mae": float(mean_absolute_error(y_test, y_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
            "r2": float(r2_score(y_test, y_pred)),
        }
        results.append(fold_result)
        start += step

    return results


def naive_baseline_cv(
    df: pd.DataFrame,
    min_train_size: int = 30,
    test_window: int = 7,
    step: int = 7,
) -> dict:
    """Naive baseline: predict 7-day rolling average of readiness."""
    results = []
    n = len(df)
    start = min_train_size

    while start + test_window <= n:
        train = df.iloc[:start]
        test = df.iloc[start : start + test_window]

        # Use 7-day rolling mean from training set as prediction
        rolling_mean = train[TARGET_COL].iloc[-7:].mean()
        y_test = test[TARGET_COL].values
        y_pred = np.full_like(y_test, rolling_mean)

        results.append({
            "mae": float(mean_absolute_error(y_test, y_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
            "r2": float(r2_score(y_test, y_pred)),
        })
        start += step

    return {
        "name": "NaiveBaseline_7d_avg",
        "cv_mae": round(float(np.mean([r["mae"] for r in results])), 2),
        "cv_rmse": round(float(np.mean([r["rmse"] for r in results])), 2),
        "cv_r2": round(float(np.mean([r["r2"] for r in results])), 3),
        "cv_folds": len(results),
    }


def _optuna_tune(
    model_name: str,
    df: pd.DataFrame,
    feature_cols: list[str],
    n_trials: int = 30,
) -> dict:
    """Hyperparameter tuning with Optuna for a given model type."""
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("Optuna not installed, skipping tuning.")
        return {}

    def objective(trial):
        if model_name == "Ridge":
            alpha = trial.suggest_float("alpha", 0.1, 100.0, log=True)
            model = Ridge(alpha=alpha)
        elif model_name == "ElasticNet":
            alpha = trial.suggest_float("alpha", 0.01, 10.0, log=True)
            l1_ratio = trial.suggest_float("l1_ratio", 0.1, 0.9)
            model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, max_iter=5000)
        elif model_name == "GradientBoosting":
            model = GradientBoostingRegressor(
                n_estimators=trial.suggest_int("n_estimators", 20, 100),
                max_depth=trial.suggest_int("max_depth", 1, 3),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 10, 25),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                random_state=42,
            )
        elif model_name == "XGBoost":
            from xgboost import XGBRegressor
            model = XGBRegressor(
                n_estimators=trial.suggest_int("n_estimators", 20, 100),
                max_depth=trial.suggest_int("max_depth", 1, 3),
                reg_alpha=trial.suggest_float("reg_alpha", 0.1, 10.0, log=True),
                reg_lambda=trial.suggest_float("reg_lambda", 0.1, 10.0, log=True),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                random_state=42, verbosity=0,
            )
        elif model_name == "LightGBM":
            from lightgbm import LGBMRegressor
            model = LGBMRegressor(
                n_estimators=trial.suggest_int("n_estimators", 20, 100),
                max_depth=trial.suggest_int("max_depth", 1, 3),
                min_child_samples=trial.suggest_int("min_child_samples", 10, 25),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                random_state=42, verbose=-1,
            )
        else:
            return float("inf")

        cv_results = walk_forward_cv(df, feature_cols, model)
        return float(np.mean([r["mae"] for r in cv_results]))

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    return {
        "best_params": study.best_params,
        "best_mae": round(study.best_value, 2),
    }


def _rebuild_model(model_name: str, params: dict):
    """Rebuild a model from name + params."""
    if model_name == "Ridge":
        return Ridge(**params)
    elif model_name == "ElasticNet":
        return ElasticNet(**params, max_iter=5000)
    elif model_name == "GradientBoosting":
        return GradientBoostingRegressor(**params, random_state=42)
    elif model_name == "XGBoost":
        from xgboost import XGBRegressor
        return XGBRegressor(**params, random_state=42, verbosity=0)
    elif model_name == "LightGBM":
        from lightgbm import LGBMRegressor
        return LGBMRegressor(**params, random_state=42, verbose=-1)
    raise ValueError(f"Unknown model: {model_name}")


def train_and_save() -> dict:
    """Full training pipeline: feature selection, model comparison, tuning, save."""
    print("=" * 60)
    print("Phase 7 — Readiness Predictor Training Pipeline")
    print("=" * 60)

    # ── Step 1: Load data ──
    print("\n1. Loading feature data from Athena...")
    df = load_feature_data()

    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=[TARGET_COL]).reset_index(drop=True)
    n_samples = len(df)
    print(f"   Samples: {n_samples}")

    sample_size_warning = n_samples < 50
    if sample_size_warning:
        print(f"   WARNING: Only {n_samples} samples — consider collecting more data before trusting predictions.")

    # ── Step 2: Feature selection ──
    print("\n2. Running feature selection...")
    from models.readiness_predictor.feature_selection import select_features
    feature_cols, feature_meta = select_features(df.copy())
    print(f"   Selected {len(feature_cols)} features: {feature_cols}")

    # ── Step 3: Naive baseline ──
    print("\n3. Computing naive baseline (7-day rolling average)...")
    baseline = naive_baseline_cv(df)
    print(f"   Baseline MAE: {baseline['cv_mae']}, R²: {baseline['cv_r2']}")

    # ── Step 4: Evaluate all candidates ──
    print("\n4. Evaluating model candidates...")
    catalog = _get_model_catalog()
    candidate_results = {}

    for name, model in catalog.items():
        cv_results = walk_forward_cv(df, feature_cols, model)
        avg_mae = float(np.mean([r["mae"] for r in cv_results]))
        avg_rmse = float(np.mean([r["rmse"] for r in cv_results]))
        avg_r2 = float(np.mean([r["r2"] for r in cv_results]))

        candidate_results[name] = {
            "cv_mae": round(avg_mae, 2),
            "cv_rmse": round(avg_rmse, 2),
            "cv_r2": round(avg_r2, 3),
            "cv_folds": len(cv_results),
            "cv_details": cv_results,
        }
        print(f"   {name:20s} MAE={avg_mae:.2f}  RMSE={avg_rmse:.2f}  R²={avg_r2:.3f}")

    # ── Step 5: MLflow logging ──
    mlflow = None
    try:
        from models.readiness_predictor.mlflow_config import setup_mlflow
        mlflow = setup_mlflow()
        print("\n5. Logging to MLflow...")

        # Log each candidate as a run
        for name, result in candidate_results.items():
            with mlflow.start_run(run_name=f"candidate_{name}"):
                mlflow.log_param("model_type", name)
                mlflow.log_param("n_samples", n_samples)
                mlflow.log_param("n_features", len(feature_cols))
                mlflow.log_param("features", json.dumps(feature_cols))
                mlflow.log_metric("cv_mae", result["cv_mae"])
                mlflow.log_metric("cv_rmse", result["cv_rmse"])
                mlflow.log_metric("cv_r2", result["cv_r2"])

        # Log baseline
        with mlflow.start_run(run_name="naive_baseline"):
            mlflow.log_param("model_type", "NaiveBaseline_7d_avg")
            mlflow.log_metric("cv_mae", baseline["cv_mae"])
            mlflow.log_metric("cv_r2", baseline["cv_r2"])
    except ImportError:
        print("\n5. MLflow not installed, skipping tracking.")

    # ── Step 6: Optuna tuning on top 2 ──
    print("\n6. Hyperparameter tuning (Optuna, top 2 candidates)...")
    sorted_candidates = sorted(candidate_results.items(), key=lambda x: x[1]["cv_mae"])
    top_2 = [name for name, _ in sorted_candidates[:2]]
    print(f"   Tuning: {top_2}")

    tuning_results = {}
    for name in top_2:
        print(f"   Tuning {name}...")
        tune_result = _optuna_tune(name, df, feature_cols, n_trials=30)
        if tune_result:
            tuning_results[name] = tune_result
            print(f"   {name} best MAE after tuning: {tune_result['best_mae']}")

    # ── Step 7: Select winner ──
    print("\n7. Selecting best model...")
    best_name = sorted_candidates[0][0]
    best_mae = sorted_candidates[0][1]["cv_mae"]

    # Check if tuning improved anything
    for name, tune in tuning_results.items():
        if tune["best_mae"] < best_mae:
            best_mae = tune["best_mae"]
            best_name = name

    print(f"   Winner: {best_name} (MAE={best_mae})")

    # Rebuild winner with best params
    if best_name in tuning_results and tuning_results[best_name].get("best_params"):
        best_model = _rebuild_model(best_name, tuning_results[best_name]["best_params"])
        best_params = tuning_results[best_name]["best_params"]
    else:
        best_model = catalog[best_name]
        best_params = {}

    # ── Step 8: Train final model on all data ──
    print("\n8. Training final model on all data...")
    X = df[feature_cols].values
    y = df[TARGET_COL].values

    final_pipe = _build_pipeline(best_model)
    final_pipe.fit(X, y)

    # Re-evaluate with walk-forward CV for final metrics
    final_cv = walk_forward_cv(df, feature_cols, best_model)
    final_mae = round(float(np.mean([r["mae"] for r in final_cv])), 2)
    final_rmse = round(float(np.mean([r["rmse"] for r in final_cv])), 2)
    final_r2 = round(float(np.mean([r["r2"] for r in final_cv])), 3)

    # Feature importances (for tree-based models)
    importances = {}
    inner_model = final_pipe.named_steps["model"]
    if hasattr(inner_model, "feature_importances_"):
        importances = dict(zip(feature_cols, inner_model.feature_importances_.tolist()))
    elif hasattr(inner_model, "coef_"):
        importances = dict(zip(feature_cols, np.abs(inner_model.coef_).tolist()))
    sorted_importances = dict(
        sorted(importances.items(), key=lambda x: x[1], reverse=True)
    )

    print(f"\n   Final CV: MAE={final_mae}, RMSE={final_rmse}, R²={final_r2}")

    # ── Step 9: Backtest ──
    y_pred_all = final_pipe.predict(X)
    backtest = df[["date", TARGET_COL]].copy()
    backtest["predicted"] = y_pred_all
    backtest.to_csv(MODEL_DIR / "backtest.csv", index=False)

    # ── Step 10: Save model + metrics ──
    joblib.dump(final_pipe, MODEL_PATH)
    print(f"\n   Model saved to {MODEL_PATH}")

    # MLflow: log winner
    if mlflow is not None:
        try:
            with mlflow.start_run(run_name=f"winner_{best_name}") as run:
                mlflow.log_param("model_type", best_name)
                mlflow.log_param("n_samples", n_samples)
                mlflow.log_param("features", json.dumps(feature_cols))
                mlflow.log_params({f"hp_{k}": v for k, v in best_params.items()})
                mlflow.log_metric("cv_mae", final_mae)
                mlflow.log_metric("cv_rmse", final_rmse)
                mlflow.log_metric("cv_r2", final_r2)
                mlflow.set_tag("best_model", "True")
                mlflow.sklearn.log_model(final_pipe, "model")
        except Exception as e:
            print(f"   MLflow logging error: {e}")

    metrics = {
        "n_samples": n_samples,
        "sample_size_warning": sample_size_warning,
        "feature_cols": feature_cols,
        "feature_selection_meta": {
            "leaky_excluded": feature_meta.get("leaky_excluded", []),
            "corr_filtered_out": feature_meta.get("corr_filtered_out", []),
            "mi_scores": feature_meta.get("mi_scores", {}),
        },
        "best_model": best_name,
        "best_params": best_params,
        "cv_folds": len(final_cv),
        "cv_mae": final_mae,
        "cv_rmse": final_rmse,
        "cv_r2": final_r2,
        "naive_baseline": baseline,
        "candidate_comparison": {
            name: {k: v for k, v in res.items() if k != "cv_details"}
            for name, res in candidate_results.items()
        },
        "tuning_results": tuning_results,
        "feature_importances": sorted_importances,
        "cv_details": final_cv,
    }

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"   Metrics saved to {METRICS_PATH}")

    # ── Summary ──
    print("\n" + "=" * 60)
    print("Training Summary")
    print("=" * 60)
    print(f"  Samples:       {n_samples}" + (" (WARNING: < 50)" if sample_size_warning else ""))
    print(f"  Features:      {len(feature_cols)}")
    print(f"  Best Model:    {best_name}")
    print(f"  CV MAE:        {final_mae}")
    print(f"  CV R²:         {final_r2}")
    print(f"  Baseline MAE:  {baseline['cv_mae']}")
    improvement = baseline["cv_mae"] - final_mae
    print(f"  vs Baseline:   {'+'if improvement > 0 else ''}{improvement:.2f} MAE improvement")

    return metrics


if __name__ == "__main__":
    train_and_save()
