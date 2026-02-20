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

    def _make_mock_athena(self, df, vitals_df=None):
        athena = MagicMock()
        if vitals_df is None:
            vitals_df = pd.DataFrame(columns=["date", "resting_heart_rate_bpm", "hrv_ms"])
        athena.execute_query.side_effect = [df, vitals_df]
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

    def _make_vitals_df(self, n_days=60, base_rhr=60, base_hrv=45):
        """Create vitals DataFrame with optional RHR/HRV spikes for testing."""
        dates = pd.date_range("2025-01-01", periods=n_days, freq="D")
        np.random.seed(42)
        rhr = base_rhr + np.random.randn(n_days) * 3
        hrv = base_hrv + np.random.randn(n_days) * 5
        return pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "resting_heart_rate_bpm": rhr,
            "hrv_ms": hrv,
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
        athena = MagicMock()
        athena.execute_query.return_value = df
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


    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_recovery_impaired_stats_present(self, _mock_viz):
        df = self._make_sample_df()
        vitals = self._make_vitals_df()
        athena = self._make_mock_athena(df, vitals)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertIn("recovery_impaired_days", result.statistics)
        self.assertIn("recovery_impaired_recent", result.statistics)
        self.assertIsInstance(result.statistics["recovery_impaired_days"], int)
        self.assertIsInstance(result.statistics["recovery_impaired_recent"], bool)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_recovery_impaired_with_no_vitals(self, _mock_viz):
        """Days without vitals data should never be flagged as recovery impaired."""
        df = self._make_sample_df()
        athena = self._make_mock_athena(df)  # no vitals_df
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertEqual(result.statistics["recovery_impaired_days"], 0)
        self.assertFalse(result.statistics["recovery_impaired_recent"])
        self.assertIn("no autonomic stress", result.narrative)

    @patch.object(TrainingLoadAnalyzer, "visualize", return_value=MagicMock())
    def test_recovery_impaired_flagged_when_rhr_elevated(self, _mock_viz):
        """RHR spike during fatigued TSB should trigger recovery impairment."""
        n = 60
        dates = pd.date_range("2025-01-01", periods=n, freq="D")
        # Low TSS early (CTL stays low), then spike hard (ATL >> CTL → TSB < -15)
        tss_values = [20.0] * 45 + [150.0] * 15
        df = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "tss": tss_values,
            "had_workout": [t > 0 for t in tss_values],
        })
        # Normal RHR baseline ~60, then spike to 75 on last days
        rhr = [60.0] * (n - 5) + [75.0] * 5
        hrv = [45.0] * n  # HRV stays normal
        vitals = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "resting_heart_rate_bpm": rhr,
            "hrv_ms": hrv,
        })
        athena = self._make_mock_athena(df, vitals)
        analyzer = TrainingLoadAnalyzer(athena)
        result = analyzer.analyze()

        self.assertGreater(result.statistics["recovery_impaired_days"], 0)
        self.assertIn("recovery impairment", result.narrative)


if __name__ == "__main__":
    unittest.main()
