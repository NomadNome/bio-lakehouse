"""Tests for leakage-resistant readiness model evaluation."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from models.readiness_predictor import feature_selection
from models.readiness_predictor import predict
from models.readiness_predictor import train


def sample_frame(rows=80):
    index = np.arange(rows, dtype=float)
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=rows).astype(str),
            "feature_a": index,
            "feature_b": np.sin(index / 5),
            "next_day_readiness": 60 + index * 0.15 + np.sin(index),
        }
    )


def test_temporal_holdout_is_the_newest_data():
    development, holdout = train.temporal_holdout_split(sample_frame())

    assert len(holdout) == 16
    assert development["date"].max() < holdout["date"].min()


def test_walk_forward_feature_selection_only_sees_training_rows(monkeypatch):
    observed_training_ends = []

    def select_train_only(frame, verbose=False):
        observed_training_ends.append(frame["date"].max())
        return ["feature_a"], {}

    monkeypatch.setattr(feature_selection, "select_features", select_train_only)
    results = train.walk_forward_cv(
        sample_frame(60),
        feature_cols=None,
        model=Ridge(),
        select_features_per_fold=True,
    )

    assert len(observed_training_ends) == len(results)
    for training_end, fold in zip(observed_training_ends, results):
        assert training_end < min(fold["dates"])


def test_holdout_evaluation_compares_model_and_baseline():
    development, holdout = train.temporal_holdout_split(sample_frame())

    evaluation, predictions, baseline = train.evaluate_temporal_holdout(
        development, holdout, ["feature_a", "feature_b"], Ridge(alpha=1.0)
    )

    assert len(predictions) == len(holdout)
    assert len(baseline) == len(holdout)
    assert evaluation["n_holdout"] == len(holdout)
    assert evaluation["prediction_interval"]["absolute_error_p80"] >= 0


def test_prediction_range_uses_heldout_error_quantile(tmp_path, monkeypatch):
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "feature_cols": ["feature_a"],
                "best_model": "Ridge",
                "n_samples": 80,
                "model_recommended": True,
                "evaluation": {"model": {"mae": 3.5, "r2": 0.2}},
                "prediction_interval": {
                    "method": "held_out_absolute_error",
                    "n_calibration": 16,
                    "absolute_error_p80": 7.0,
                },
            }
        )
    )
    model = MagicMock()
    model.predict.return_value = np.array([80.0])
    monkeypatch.setattr(predict, "METRICS_PATH", metrics_path)
    monkeypatch.setattr(predict, "_load_model", lambda: model)
    monkeypatch.setattr(
        predict,
        "load_latest_features",
        lambda: pd.DataFrame({"date": ["2026-09-06"], "feature_a": [1.0]}),
    )

    result = predict.predict_next_day()

    assert result["confidence"]["range_low"] == 73.0
    assert result["confidence"]["range_high"] == 87.0
    assert result["confidence"]["interval_method"] == "held_out_absolute_error"


def test_prediction_falls_back_when_model_loses_holdout(tmp_path, monkeypatch):
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "feature_cols": ["readiness_7d_avg"],
                "best_model": "LightGBM",
                "model_recommended": False,
                "evaluation": {"model": {"mae": 4.8, "r2": -0.4}},
                "prediction_interval": {
                    "method": "held_out_absolute_error",
                    "n_calibration": 20,
                    "absolute_error_p80": 6.0,
                },
            }
        )
    )
    monkeypatch.setattr(predict, "METRICS_PATH", metrics_path)
    load_model = MagicMock(side_effect=AssertionError("baseline should not load model"))
    monkeypatch.setattr(predict, "_load_model", load_model)
    monkeypatch.setattr(
        predict,
        "load_latest_features",
        lambda: pd.DataFrame(
            {"date": ["2026-09-06"], "readiness_7d_avg": [72.0]}
        ),
    )

    result = predict.predict_next_day()

    assert result["prediction_source"] == "rolling_7d_baseline"
    assert result["predicted_readiness"] == 72.0
    assert result["model_prediction"] is None
    load_model.assert_not_called()
