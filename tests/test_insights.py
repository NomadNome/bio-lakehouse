"""Tests for insight analyzer components."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.insights.base import DateRange, InsightResult, InsightAnalyzer
from insights_engine.insights.sleep_readiness import SleepReadinessAnalyzer
from insights_engine.insights.workout_recovery import WorkoutRecoveryAnalyzer
from insights_engine.insights.readiness_trend import ReadinessTrendAnalyzer
from insights_engine.insights.anomaly_detection import AnomalyDetectionAnalyzer
from insights_engine.insights.timing_correlation import TimingCorrelationAnalyzer


# ── Dataclass tests ──────────────────────────────────────────────────────

class TestDateRange:
    def test_creation(self):
        dr = DateRange(start=date(2026, 2, 10), end=date(2026, 2, 16))
        assert dr.start == date(2026, 2, 10)
        assert (dr.end - dr.start).days == 6


class TestInsightResult:
    def test_defaults(self):
        r = InsightResult(insight_type="test", title="Test", narrative="Hello")
        assert r.statistics == {}
        assert r.chart is None
        assert r.caveats == []

    def test_with_all_fields(self):
        r = InsightResult(
            insight_type="sleep_readiness",
            title="Sleep → Readiness",
            narrative="Strong correlation",
            statistics={"r": 0.5, "n": 30},
            caveats=["Small sample"],
        )
        assert r.statistics["r"] == 0.5
        assert len(r.caveats) == 1


# ── Analyzer instantiation tests ────────────────────────────────────────

class TestAnalyzerInstantiation:
    def setup_method(self):
        self.mock_athena = MagicMock()

    def test_sleep_readiness_instantiates(self):
        a = SleepReadinessAnalyzer(self.mock_athena)
        assert isinstance(a, InsightAnalyzer)

    def test_workout_recovery_instantiates(self):
        a = WorkoutRecoveryAnalyzer(self.mock_athena)
        assert isinstance(a, InsightAnalyzer)

    def test_readiness_trend_instantiates(self):
        a = ReadinessTrendAnalyzer(self.mock_athena)
        assert isinstance(a, InsightAnalyzer)

    def test_anomaly_detection_instantiates(self):
        a = AnomalyDetectionAnalyzer(self.mock_athena)
        assert isinstance(a, InsightAnalyzer)

    def test_timing_correlation_instantiates(self):
        a = TimingCorrelationAnalyzer(self.mock_athena)
        assert isinstance(a, InsightAnalyzer)


# ── Narrate tests with known statistics ──────────────────────────────────

class TestSleepReadinessNarrate:
    def test_narrate_positive_correlation(self):
        a = SleepReadinessAnalyzer(MagicMock())
        result = InsightResult(
            insight_type="sleep_readiness",
            title="Sleep → Readiness Correlation",
            narrative="",
            statistics={
                "r": 0.536, "p_value": 0.001, "n": 40,
                "slope": 0.8, "intercept": 15.0, "std_err": 0.2,
                "strength": "moderate", "direction": "positive",
                "mean_sleep": 83.0, "mean_readiness": 81.0,
            },
        )
        narrative = a.narrate(result)
        assert "moderate" in narrative
        assert "positive" in narrative
        assert "0.54" in narrative or "0.536" in narrative
        assert "40" in narrative


class TestWorkoutRecoveryNarrate:
    def test_narrate_with_groups(self):
        a = WorkoutRecoveryAnalyzer(MagicMock())
        result = InsightResult(
            insight_type="workout_recovery",
            title="Workout Recovery",
            narrative="",
            statistics={
                "groups": {
                    "Cycling": {"mean": 80.1, "median": 82.0, "std": 9.1, "n": 45},
                    "Rest Day": {"mean": 81.8, "median": 86.0, "std": 9.1, "n": 27},
                },
                "comparisons": {
                    "Cycling_vs_Rest Day": {"U": 509.0, "p_value": 0.253, "significant": False},
                },
                "total_n": 72,
            },
        )
        narrative = a.narrate(result)
        assert "Cycling" in narrative
        assert "Rest Day" in narrative
        assert "not statistically significant" in narrative


class TestReadinessTrendNarrate:
    def test_narrate_declining(self):
        a = ReadinessTrendAnalyzer(MagicMock())
        result = InsightResult(
            insight_type="readiness_trend",
            title="Readiness Trend",
            narrative="",
            statistics={
                "n": 84, "mean": 80.7, "std": 9.2,
                "min": 51.0, "max": 95.0,
                "trend": "declining", "trend_icon": "↓",
                "slope_14d": -0.5, "notable_weeks_count": 1,
                "current_7d_avg": 81.6,
            },
        )
        narrative = a.narrate(result)
        assert "declining" in narrative
        assert "80.7" in narrative
        assert "81.6" in narrative


class TestAnomalyDetectionNarrate:
    def test_narrate_with_anomalies(self):
        a = AnomalyDetectionAnalyzer(MagicMock())
        result = InsightResult(
            insight_type="anomaly_detection",
            title="Anomaly Detection",
            narrative="",
            statistics={
                "mean_readiness": 81.0, "std_readiness": 9.2,
                "threshold": 67.2, "anomaly_count": 3, "total_days": 84,
                "anomaly_details": [
                    {"date": "2025-12-06", "readiness": 51.0, "sleep": 64.0, "deviation": -3.3},
                ],
                "missed_workout_breaks": [],
            },
        )
        narrative = a.narrate(result)
        assert "3 anomalies" in narrative
        assert "51" in narrative
        assert "2025-12-06" in narrative

    def test_narrate_no_anomalies(self):
        a = AnomalyDetectionAnalyzer(MagicMock())
        result = InsightResult(
            insight_type="anomaly_detection",
            title="Anomaly Detection",
            narrative="",
            statistics={
                "mean_readiness": 85.0, "std_readiness": 5.0,
                "threshold": 77.5, "anomaly_count": 0, "total_days": 30,
                "anomaly_details": [],
                "missed_workout_breaks": [],
            },
        )
        narrative = a.narrate(result)
        assert "0 anomalies" in narrative
