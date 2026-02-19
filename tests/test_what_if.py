"""Tests for the What-If Simulator."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.config import ENERGY_THRESHOLDS
from insights_engine.insights.what_if import Scenario, SimulationResult, WhatIfSimulator


# ── Fixtures ────────────────────────────────────────────────────────────

def _mock_athena():
    """Return a mock AthenaClient with realistic gold-layer data."""
    mock = MagicMock()

    sleep_df = pd.DataFrame({
        "sleep_score": [90, 85, 80, 75, 70, 65, 60, 55, 92, 88, 82, 77, 72, 68, 58],
        "sleep_quality": [
            "Excellent (88+)", "Good (75-87)", "Good (75-87)", "Good (75-87)", "Fair (60-74)",
            "Fair (60-74)", "Fair (60-74)", "Poor (<60)", "Excellent (88+)", "Excellent (88+)",
            "Good (75-87)", "Good (75-87)", "Fair (60-74)", "Fair (60-74)", "Poor (<60)",
        ],
        "next_day_readiness": [88, 85, 82, 78, 75, 70, 65, 60, 90, 86, 80, 76, 73, 68, 55],
    })

    workout_df = pd.DataFrame({
        "workout_type": ["Cycling Only", "Cycling Only", "Strength Only", "Strength Only", "Rest Day", "Rest Day"],
        "readiness_bucket": ["High (85+)", "Moderate (70-84)", "High (85+)", "Moderate (70-84)", "High (85+)", "Moderate (70-84)"],
        "avg_readiness_in_bucket": [87.0, 76.0, 88.0, 77.0, 89.0, 78.0],
        "sample_days": [20, 15, 10, 8, 12, 10],
    })

    baseline_df = pd.DataFrame({
        "readiness_score": [80, 82, 78, 85, 76, 81, 83],
        "sleep_score": [82, 85, 75, 88, 72, 80, 84],
        "total_output_kj": [300, 450, 0, 500, 200, 350, 400],
        "had_workout": [True, True, False, True, True, True, True],
        "readiness_7day_avg": [None, None, None, None, None, None, 80.7],
    })

    risk_df = pd.DataFrame({
        "workouts_last_3_days": [3],
        "overtraining_risk": ["low_risk"],
    })

    def side_effect(sql, **kwargs):
        sql_lower = sql.lower()
        if "sleep_performance_prediction" in sql_lower:
            return sleep_df
        elif "workout_type_optimization" in sql_lower:
            return workout_df
        elif "dashboard_30day" in sql_lower:
            return baseline_df
        elif "overtraining_risk" in sql_lower:
            return risk_df
        return pd.DataFrame()

    mock.execute_query = MagicMock(side_effect=side_effect)
    return mock


@pytest.fixture
def simulator():
    mock_athena = _mock_athena()
    sim = WhatIfSimulator(mock_athena)
    sim.load_historical_models()
    return sim


# ── Dataclass tests ─────────────────────────────────────────────────────

class TestScenario:
    def test_defaults(self):
        s = Scenario()
        assert s.sleep_score == 80
        assert s.workout_type == "rest"
        assert s.workout_intensity == "none"
        assert s.consecutive_workout_days == 0

    def test_custom(self):
        s = Scenario(sleep_score=90, workout_type="cycling", workout_intensity="high", consecutive_workout_days=4)
        assert s.sleep_score == 90
        assert s.workout_type == "cycling"


class TestSimulationResult:
    def test_defaults(self):
        r = SimulationResult()
        assert r.predicted_readiness == 0.0
        assert r.energy_state == "moderate"
        assert r.overtraining_risk == "low"
        assert r.supporting_data == {}


# ── Model loading tests ─────────────────────────────────────────────────

class TestLoadHistoricalModels:
    def test_loads_all_models(self, simulator):
        models = simulator.load_historical_models()
        assert "sleep_regression" in models
        assert "sleep_buckets" in models
        assert "workout_type_effects" in models
        assert "baseline" in models
        assert "current_streak" in models

    def test_regression_is_valid(self, simulator):
        reg = simulator._models["sleep_regression"]
        assert reg["valid"] is True
        assert reg["n"] == 15
        assert reg["slope"] > 0  # sleep and readiness positively correlated

    def test_sleep_buckets(self, simulator):
        buckets = simulator._models["sleep_buckets"]
        assert "Excellent (88+)" in buckets
        assert "Good (75-87)" in buckets
        assert "Fair (60-74)" in buckets
        assert buckets["Excellent (88+)"]["n"] == 3

    def test_baseline_computed(self, simulator):
        baseline = simulator._models["baseline"]
        assert baseline["total_days"] == 7
        assert 70 < baseline["mean_readiness"] < 90
        assert baseline["avg_readiness_7d"] == 80.7

    def test_caches_models(self, simulator):
        m1 = simulator.load_historical_models()
        m2 = simulator.load_historical_models()
        assert m1 is m2  # same object, not re-queried

    def test_streak_extracted(self, simulator):
        streak = simulator._models["current_streak"]
        assert streak["consecutive_workout_days"] == 3
        assert streak["risk_level"] == "low"


# ── Simulation tests ────────────────────────────────────────────────────

class TestSimulate:
    def test_rest_day_high_sleep(self, simulator):
        result = simulator.simulate(Scenario(
            sleep_score=90, workout_type="rest", workout_intensity="none", consecutive_workout_days=0,
        ))
        assert 70 <= result.predicted_readiness <= 100
        assert result.overtraining_risk == "low"
        assert result.energy_state in ("peak", "high", "moderate")

    def test_high_intensity_low_sleep(self, simulator):
        result = simulator.simulate(Scenario(
            sleep_score=50, workout_type="cycling", workout_intensity="high", consecutive_workout_days=5,
        ))
        # Low sleep + high consecutive days → lower readiness
        assert result.predicted_readiness < 80
        assert result.overtraining_risk in ("moderate", "high")

    def test_rest_beats_high_intensity(self, simulator):
        rest = simulator.simulate(Scenario(
            sleep_score=80, workout_type="rest", workout_intensity="none", consecutive_workout_days=0,
        ))
        intense = simulator.simulate(Scenario(
            sleep_score=80, workout_type="cycling", workout_intensity="high", consecutive_workout_days=5,
        ))
        assert rest.predicted_readiness > intense.predicted_readiness

    def test_high_sleep_beats_low_sleep(self, simulator):
        high = simulator.simulate(Scenario(sleep_score=95))
        low = simulator.simulate(Scenario(sleep_score=40))
        assert high.predicted_readiness > low.predicted_readiness

    def test_readiness_clamped_0_100(self, simulator):
        result = simulator.simulate(Scenario(sleep_score=100, consecutive_workout_days=0))
        assert 0 <= result.predicted_readiness <= 100

        result_low = simulator.simulate(Scenario(sleep_score=0, consecutive_workout_days=14))
        assert 0 <= result_low.predicted_readiness <= 100


# ── Energy state classification tests ───────────────────────────────────

class TestEnergyClassification:
    def test_peak(self, simulator):
        result = simulator.simulate(Scenario(sleep_score=95, consecutive_workout_days=0))
        # With high sleep + rest → should be peak or high
        assert result.energy_state in ("peak", "high")

    def test_recovery_needed(self, simulator):
        result = simulator.simulate(Scenario(
            sleep_score=30, workout_type="cycling", workout_intensity="high", consecutive_workout_days=8,
        ))
        assert result.energy_state in ("low", "recovery_needed")

    def test_energy_matches_config_thresholds(self):
        """Verify classification logic uses config thresholds correctly."""
        assert WhatIfSimulator._classify_energy(90, 90) == "peak"
        assert WhatIfSimulator._classify_energy(86, 82) == "high"
        assert WhatIfSimulator._classify_energy(75, 70) == "moderate"
        assert WhatIfSimulator._classify_energy(55, 40) == "low"
        assert WhatIfSimulator._classify_energy(40, 30) == "recovery_needed"


# ── Overtraining risk tests ─────────────────────────────────────────────

class TestOvertrainingRisk:
    def test_low_risk(self, simulator):
        result = simulator.simulate(Scenario(consecutive_workout_days=2))
        assert result.overtraining_risk == "low"

    def test_moderate_risk(self, simulator):
        result = simulator.simulate(Scenario(consecutive_workout_days=4))
        assert result.overtraining_risk == "moderate"

    def test_high_risk(self, simulator):
        result = simulator.simulate(Scenario(consecutive_workout_days=7))
        assert result.overtraining_risk == "high"

    def test_escalation(self, simulator):
        risks = []
        for days in [1, 3, 5, 7]:
            result = simulator.simulate(Scenario(consecutive_workout_days=days))
            risks.append(result.overtraining_risk)
        assert risks == ["low", "low", "moderate", "high"]


# ── Confidence range tests ──────────────────────────────────────────────

class TestConfidenceRange:
    def test_range_is_tuple(self, simulator):
        result = simulator.simulate(Scenario())
        assert isinstance(result.confidence_range, tuple)
        assert len(result.confidence_range) == 2

    def test_range_brackets_prediction(self, simulator):
        result = simulator.simulate(Scenario(sleep_score=80))
        lo, hi = result.confidence_range
        assert lo <= result.predicted_readiness <= hi

    def test_range_width_positive(self, simulator):
        result = simulator.simulate(Scenario(sleep_score=80))
        lo, hi = result.confidence_range
        assert hi - lo > 0


# ── Supporting data tests ───────────────────────────────────────────────

class TestSupportingData:
    def test_has_required_keys(self, simulator):
        result = simulator.simulate(Scenario())
        sd = result.supporting_data
        assert "regression_r" in sd
        assert "regression_n" in sd
        assert "sleep_bucket" in sd
        assert "bucket_n" in sd
        assert "baseline_7d_readiness" in sd
        assert "total_historical_days" in sd

    def test_comparison_to_baseline(self, simulator):
        result = simulator.simulate(Scenario(sleep_score=80))
        expected_delta = result.predicted_readiness - result.supporting_data["baseline_7d_readiness"]
        assert abs(result.comparison_to_baseline - expected_delta) < 0.2


# ── Recommendation tests ───────────────────────────────────────────────

class TestRecommendation:
    def test_returns_string(self, simulator):
        result = simulator.simulate(Scenario())
        assert isinstance(result.recommendation, str)
        assert len(result.recommendation) > 10

    def test_high_overtraining_warns(self, simulator):
        result = simulator.simulate(Scenario(consecutive_workout_days=8))
        assert "rest" in result.recommendation.lower() or "recovery" in result.recommendation.lower()
