"""
Tests for Training Load periodization (TSS / CTL / ATL / TSB).
"""

import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from insights_engine.insights.training_load import (
    TrainingLoadAnalyzer,
    classify_form,
    compute_ema,
)


class TestComputeEMA(unittest.TestCase):
    """Test exponential moving average computation."""

    def test_constant_series_returns_constant(self):
        s = pd.Series([10.0] * 20)
        ema = compute_ema(s, span=7)
        np.testing.assert_allclose(ema.values, 10.0, atol=1e-10)

    def test_ema_length_matches_input(self):
        s = pd.Series(range(50))
        ema = compute_ema(s, span=42)
        self.assertEqual(len(ema), 50)

    def test_7day_ema_responds_faster_than_42day(self):
        """ATL (7-day) should react more quickly to a spike than CTL (42-day)."""
        data = [10.0] * 30 + [100.0] * 5
        s = pd.Series(data)
        ema_7 = compute_ema(s, span=7)
        ema_42 = compute_ema(s, span=42)
        self.assertGreater(ema_7.iloc[-1], ema_42.iloc[-1])

    def test_ema_smooths_noisy_data(self):
        np.random.seed(42)
        noisy = pd.Series(50 + np.random.randn(100) * 20)
        ema = compute_ema(noisy, span=7)
        self.assertLess(ema.std(), noisy.std())


class TestClassifyForm(unittest.TestCase):
    """Test TSB form classification."""

    def test_fresh(self):
        self.assertEqual(classify_form(20), "fresh")
        self.assertEqual(classify_form(15.1), "fresh")

    def test_neutral(self):
        self.assertEqual(classify_form(15), "neutral")
        self.assertEqual(classify_form(0), "neutral")
        self.assertEqual(classify_form(7.5), "neutral")

    def test_building(self):
        self.assertEqual(classify_form(-1), "building")
        self.assertEqual(classify_form(-15), "building")
        self.assertEqual(classify_form(-10), "building")

    def test_fatigued(self):
        self.assertEqual(classify_form(-15.1), "fatigued")
        self.assertEqual(classify_form(-30), "fatigued")

    def test_boundary_values(self):
        self.assertEqual(classify_form(15), "neutral")
        self.assertEqual(classify_form(0), "neutral")
        self.assertEqual(classify_form(-15), "building")


class TestTrainingLoadAnalyzer(unittest.TestCase):
    """Test TrainingLoadAnalyzer with mocked Athena.

    Chart creation is patched to avoid narwhals/PySpark mock conflicts
    in the test environment (Plotly's narwhals tries isinstance() against
    mocked pyspark.sql.DataFrame).
    """

    def _make_mock_athena(self, df):
        athena = MagicMock()
        athena.execute_query.return_value = df
        return athena

    def _make_sample_df(self, n_days=60, base_tss=40):
        dates = pd.date_range("2025-01-01", periods=n_days, freq="D")
        np.random.seed(42)
        tss_values = np.clip(base_tss + np.random.randn(n_days) * 15, 0, 300)
        for i in range(0, n_days, 7):
            tss_values[i] = 0
        return pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "tss": tss_values,
            "had_workout": [t > 0 for t in tss_values],
        })

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_analyze_returns_insight_result(self, _mock_viz):
        df = self._make_sample_df()
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertEqual(result.insight_type, "training_load")
        self.assertIn("form", result.statistics)
        self.assertIn(result.statistics["form"], ["fresh", "neutral", "building", "fatigued"])

    def test_insufficient_data(self):
        df = pd.DataFrame({"date": ["2025-01-01"], "tss": [50], "had_workout": [True]})
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertIn("Insufficient", result.narrative)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_ctl_atl_tsb_computed(self, _mock_viz):
        df = self._make_sample_df(n_days=60)
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertIn("latest_ctl", result.statistics)
        self.assertIn("latest_atl", result.statistics)
        self.assertIn("latest_tsb", result.statistics)
        expected_tsb = result.statistics["latest_ctl"] - result.statistics["latest_atl"]
        self.assertAlmostEqual(result.statistics["latest_tsb"], expected_tsb, places=0)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_narrative_contains_form(self, _mock_viz):
        df = self._make_sample_df()
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertIn(result.statistics["form"].title(), result.narrative)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_chart_is_set(self, _mock_viz):
        df = self._make_sample_df()
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertIsNotNone(result.chart)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_caveat_for_short_data(self, _mock_viz):
        df = self._make_sample_df(n_days=20)
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertTrue(any("42-day" in c for c in result.caveats))

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_all_rest_days(self, _mock_viz):
        dates = pd.date_range("2025-01-01", periods=14, freq="D")
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "tss": [0.0] * 14,
            "had_workout": [False] * 14,
        })
        athena = self._make_mock_athena(df)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertEqual(result.statistics["latest_ctl"], 0.0)
        self.assertEqual(result.statistics["latest_atl"], 0.0)
        self.assertEqual(result.statistics["form"], "neutral")


if __name__ == "__main__":
    unittest.main()
