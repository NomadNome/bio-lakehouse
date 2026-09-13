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
from sklearn.base import clone
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

TARGET_COL = "next_day_readiness"

from models.readiness_predictor.paths import (
    BACKTEST_PATH,
    METRICS_PATH,
    MODEL_DIR,
    MODEL_PATH,
)


def load_feature_data() -> pd.DataFrame:
    """Load feature data from Athena via the project's AthenaClient."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from insights_engine.core.athena_client import AthenaClient

    athena = AthenaClient()
    df = athena.execute_query("""
        SELECT *
        FROM feature_readiness_daily
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
        ("model", clone(model)),
    ])


def temporal_holdout_split(
    df: pd.DataFrame,
    holdout_fraction: float = 0.20,
    min_holdout_size: int = 14,
    min_development_size: int = 45,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reserve the newest observations for one final, untouched evaluation."""
    if not 0 < holdout_fraction < 1:
        raise ValueError("holdout_fraction must be between 0 and 1")

    holdout_size = max(min_holdout_size, int(np.ceil(len(df) * holdout_fraction)))
    holdout_size = min(holdout_size, len(df) - min_development_size)
    if holdout_size < 7:
        raise ValueError(
            f"Need at least {min_development_size + 7} target rows for a temporal holdout; "
            f"got {len(df)}"
        )

    split_at = len(df) - holdout_size
    return (
        df.iloc[:split_at].reset_index(drop=True),
        df.iloc[split_at:].reset_index(drop=True),
    )


def summarize_predictions(y_true, y_pred) -> dict[str, float]:
    """Compute metrics across observations, avoiding unstable per-week R² means."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if len(y_true) == 0:
        raise ValueError("Cannot summarize an empty prediction set")
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else float("nan"),
    }


def walk_forward_cv(
    df: pd.DataFrame,
    feature_cols: list[str] | None,
    model,
    min_train_size: int = 30,
    test_window: int = 7,
    step: int = 7,
    select_features_per_fold: bool = False,
) -> list[dict]:
    """Walk-forward CV, optionally selecting features on each training fold."""
    results = []
    n = len(df)
    start = min_train_size

    while start + test_window <= n:
        train = df.iloc[:start]
        test = df.iloc[start : start + test_window]

        fold_features = feature_cols
        if select_features_per_fold:
            from models.readiness_predictor.feature_selection import select_features

            fold_features, _ = select_features(train.copy(), verbose=False)
        if not fold_features:
            raise ValueError("No features available for walk-forward fold")

        X_train = train[fold_features].values
        y_train = train[TARGET_COL].values
        X_test = test[fold_features].values
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
            "feature_cols": list(fold_features),
            "dates": test["date"].astype(str).tolist(),
            "actual": y_test.astype(float).tolist(),
            "predicted": y_pred.astype(float).tolist(),
        }
        results.append(fold_result)
        start += step

    return results


def summarize_cv_results(results: list[dict]) -> dict[str, float]:
    """Pool all out-of-sample fold predictions before computing metrics."""
    actual = [value for fold in results for value in fold["actual"]]
    predicted = [value for fold in results for value in fold["predicted"]]
    metrics = summarize_predictions(actual, predicted)
    metrics["folds"] = len(results)
    metrics["observations"] = len(actual)
    return metrics


def naive_baseline_cv(
    df: pd.DataFrame,
    min_train_size: int = 30,
    test_window: int = 7,
    step: int = 7,
) -> dict:
    """Naive baseline: predict 7-day rolling average of readiness."""
    actual = []
    predicted = []
    n = len(df)
    start = min_train_size

    while start + test_window <= n:
        train = df.iloc[:start]
        test = df.iloc[start : start + test_window]

        # Use 7-day rolling mean from training set as prediction
        rolling_mean = train[TARGET_COL].iloc[-7:].mean()
        y_test = test[TARGET_COL].values
        y_pred = np.full_like(y_test, rolling_mean)

        actual.extend(y_test.astype(float).tolist())
        predicted.extend(y_pred.astype(float).tolist())
        start += step

    metrics = summarize_predictions(actual, predicted)
    return {
        "name": "NaiveBaseline_7d_avg",
        "cv_mae": round(metrics["mae"], 2),
        "cv_rmse": round(metrics["rmse"], 2),
        "cv_r2": round(metrics["r2"], 3),
        "cv_observations": len(actual),
    }


def evaluate_temporal_holdout(
    development: pd.DataFrame,
    holdout: pd.DataFrame,
    feature_cols: list[str],
    model,
) -> tuple[dict, np.ndarray, np.ndarray]:
    """Fit on development data and evaluate once on the untouched tail."""
    pipeline = _build_pipeline(model)
    pipeline.fit(development[feature_cols].values, development[TARGET_COL].values)
    model_predictions = pipeline.predict(holdout[feature_cols].values)

    history = development[TARGET_COL].astype(float).tolist()
    baseline_predictions = []
    for actual in holdout[TARGET_COL].astype(float):
        baseline_predictions.append(float(np.mean(history[-7:])))
        history.append(float(actual))

    model_metrics = summarize_predictions(holdout[TARGET_COL], model_predictions)
    baseline_metrics = summarize_predictions(holdout[TARGET_COL], baseline_predictions)
    def error_interval(predictions):
        absolute_errors = np.abs(
            holdout[TARGET_COL].to_numpy(dtype=float) - np.asarray(predictions)
        )
        return {
            "method": "held_out_absolute_error",
            "n_calibration": int(len(absolute_errors)),
            "absolute_error_p80": round(
                float(np.quantile(absolute_errors, 0.80, method="higher")), 2
            ),
            "absolute_error_p95": round(
                float(np.quantile(absolute_errors, 0.95, method="higher")), 2
            ),
        }

    evaluation = {
        "n_holdout": int(len(holdout)),
        "start_date": str(holdout["date"].iloc[0]),
        "end_date": str(holdout["date"].iloc[-1]),
        "model": {key: round(value, 3) for key, value in model_metrics.items()},
        "baseline": {key: round(value, 3) for key, value in baseline_metrics.items()},
        "beats_baseline": bool(model_metrics["mae"] < baseline_metrics["mae"]),
        "prediction_interval": error_interval(model_predictions),
        "baseline_prediction_interval": error_interval(baseline_predictions),
    }
    return evaluation, model_predictions, np.asarray(baseline_predictions)


def _optuna_tune(
    model_name: str,
    df: pd.DataFrame,
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

        cv_results = walk_forward_cv(
            df,
            feature_cols=None,
            model=model,
            select_features_per_fold=True,
        )
        return summarize_cv_results(cv_results)["mae"]

    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
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
    """Train with train-only feature selection and an untouched temporal holdout."""
    print("=" * 60)
    print("Phase 7 — Readiness Predictor Training Pipeline")
    print("=" * 60)

    # ── Step 1: Load data ──
    print("\n1. Loading feature data from Athena...")
    df = load_feature_data()

    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=[TARGET_COL]).sort_values("date").reset_index(drop=True)
    n_samples = len(df)
    print(f"   Samples: {n_samples}")

    sample_size_warning = n_samples < 50
    if sample_size_warning:
        print(f"   WARNING: Only {n_samples} samples — consider collecting more data before trusting predictions.")

    development, holdout = temporal_holdout_split(df)
    print(
        f"   Development: {len(development)} rows | "
        f"Untouched holdout: {len(holdout)} rows "
        f"({holdout['date'].iloc[0]} through {holdout['date'].iloc[-1]})"
    )

    # ── Step 2: Feature selection on development data only ──
    print("\n2. Running development-only feature selection...")
    from models.readiness_predictor.feature_selection import select_features
    feature_cols, feature_meta = select_features(development.copy())
    print(f"   Selected {len(feature_cols)} features: {feature_cols}")

    # ── Step 3: Development baseline ──
    print("\n3. Computing development baseline (7-day rolling average)...")
    baseline = naive_baseline_cv(development)
    print(f"   Baseline MAE: {baseline['cv_mae']}, R²: {baseline['cv_r2']}")

    # ── Step 4: Compare candidates on development data ──
    print("\n4. Evaluating candidates with train-only feature selection...")
    catalog = _get_model_catalog()
    candidate_results = {}

    for name, model in catalog.items():
        cv_results = walk_forward_cv(
            development,
            feature_cols=None,
            model=model,
            select_features_per_fold=True,
        )
        summary = summarize_cv_results(cv_results)

        candidate_results[name] = {
            "cv_mae": round(summary["mae"], 2),
            "cv_rmse": round(summary["rmse"], 2),
            "cv_r2": round(summary["r2"], 3),
            "cv_folds": summary["folds"],
            "cv_observations": summary["observations"],
            "cv_details": cv_results,
        }
        print(
            f"   {name:20s} MAE={summary['mae']:.2f}  "
            f"RMSE={summary['rmse']:.2f}  R²={summary['r2']:.3f}"
        )

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
                mlflow.log_param("n_development_samples", len(development))
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
    except Exception as exc:
        print(f"\n5. MLflow unavailable, skipping tracking: {exc}")

    # ── Step 6: Optuna tuning on top 2 ──
    print("\n6. Hyperparameter tuning (Optuna, top 2 candidates)...")
    sorted_candidates = sorted(candidate_results.items(), key=lambda x: x[1]["cv_mae"])
    top_2 = [name for name, _ in sorted_candidates[:2]]
    print(f"   Tuning: {top_2}")

    tuning_results = {}
    for name in top_2:
        print(f"   Tuning {name}...")
        tune_result = _optuna_tune(name, development, n_trials=30)
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

    # ── Step 8: One final evaluation on untouched future data ──
    print("\n8. Evaluating once on the untouched temporal holdout...")
    evaluation, holdout_predictions, holdout_baseline = evaluate_temporal_holdout(
        development, holdout, feature_cols, best_model
    )
    heldout = evaluation["model"]
    heldout_baseline = evaluation["baseline"]
    print(
        f"   Holdout model: MAE={heldout['mae']:.2f}, R²={heldout['r2']:.3f} | "
        f"baseline MAE={heldout_baseline['mae']:.2f}"
    )

    # Produce a genuinely out-of-sample development backtest. Each fold does
    # its own feature selection using only rows available at that point.
    final_cv = walk_forward_cv(
        development,
        feature_cols=None,
        model=best_model,
        select_features_per_fold=True,
    )
    final_cv_summary = summarize_cv_results(final_cv)
    final_mae = round(final_cv_summary["mae"], 2)
    final_rmse = round(final_cv_summary["rmse"], 2)
    final_r2 = round(final_cv_summary["r2"], 3)

    # Train the production artifact on all available rows only after the
    # holdout metrics have been frozen. The selected columns came from the
    # development period, so holdout outcomes did not influence selection.
    print("\n9. Training production model on all available data...")
    final_pipe = _build_pipeline(best_model)
    final_pipe.fit(df[feature_cols].values, df[TARGET_COL].values)

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

    # ── Step 10: Save only out-of-sample backtest predictions ──
    backtest_rows = []
    for fold in final_cv:
        backtest_rows.extend(
            {
                "date": day,
                TARGET_COL: actual,
                "predicted": predicted,
                "split": "development_walk_forward",
            }
            for day, actual, predicted in zip(
                fold["dates"], fold["actual"], fold["predicted"]
            )
        )
    backtest_rows.extend(
        {
            "date": str(day),
            TARGET_COL: float(actual),
            "predicted": float(predicted),
            "baseline_predicted": float(baseline_predicted),
            "split": "untouched_holdout",
        }
        for day, actual, predicted, baseline_predicted in zip(
            holdout["date"],
            holdout[TARGET_COL],
            holdout_predictions,
            holdout_baseline,
        )
    )
    backtest = pd.DataFrame(backtest_rows).sort_values("date")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    backtest.to_csv(BACKTEST_PATH, index=False)

    # ── Step 11: Save model + metrics ──
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
                mlflow.log_metric("holdout_mae", heldout["mae"])
                mlflow.log_metric("holdout_rmse", heldout["rmse"])
                mlflow.log_metric("holdout_r2", heldout["r2"])
                mlflow.log_metric("holdout_baseline_mae", heldout_baseline["mae"])
                mlflow.set_tag("best_model", "True")
                mlflow.set_tag("beats_holdout_baseline", str(evaluation["beats_baseline"]))
                mlflow.sklearn.log_model(final_pipe, "model")
        except Exception as e:
            print(f"   MLflow logging error: {e}")

    metrics = {
        "n_samples": n_samples,
        "sample_size_warning": sample_size_warning,
        "development_samples": len(development),
        "holdout_samples": len(holdout),
        "feature_cols": feature_cols,
        "feature_selection_meta": {
            "leaky_excluded": feature_meta.get("leaky_excluded", []),
            "corr_filtered_out": feature_meta.get("corr_filtered_out", []),
            "mi_scores": feature_meta.get("mi_scores", {}),
        },
        "best_model": best_name,
        "best_params": best_params,
        "evaluation": evaluation,
        "prediction_source": (
            "model" if evaluation["beats_baseline"] else "rolling_7d_baseline"
        ),
        "prediction_interval": (
            evaluation["prediction_interval"]
            if evaluation["beats_baseline"]
            else evaluation["baseline_prediction_interval"]
        ),
        "model_recommended": evaluation["beats_baseline"],
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
    print(f"  Dev CV MAE:    {final_mae}")
    print(f"  Holdout MAE:   {heldout['mae']}")
    print(f"  Holdout R²:    {heldout['r2']}")
    improvement = heldout_baseline["mae"] - heldout["mae"]
    print(f"  vs Baseline:   {improvement:+.2f} holdout MAE")
    if not evaluation["beats_baseline"]:
        print("  WARNING: Model did not beat the rolling-average baseline on holdout data.")

    return metrics


if __name__ == "__main__":
    train_and_save()
