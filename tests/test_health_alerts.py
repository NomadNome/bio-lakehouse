"""
Tests for Health Alerts Lambda handler.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add lambda directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "lambda" / "health_alerts"))

# Must mock boto3 before importing handler
mock_boto3 = MagicMock()
sys.modules["boto3"] = mock_boto3

import handler


class TestSafeFloat(unittest.TestCase):
    def test_valid_float(self):
        self.assertEqual(handler.safe_float("62.5"), 62.5)

    def test_integer_string(self):
        self.assertEqual(handler.safe_float("100"), 100.0)

    def test_none_returns_default(self):
        self.assertIsNone(handler.safe_float(None))
        self.assertEqual(handler.safe_float(None, 0.0), 0.0)

    def test_invalid_string_returns_default(self):
        self.assertIsNone(handler.safe_float("not_a_number"))

    def test_empty_string_returns_default(self):
        self.assertIsNone(handler.safe_float(""))


class TestCheckAlerts(unittest.TestCase):
    """Test alert condition evaluation with mocked Athena queries."""

    def _mock_run_athena(self, responses):
        """Create a side_effect function that returns responses in order."""
        call_count = [0]

        def mock_query(sql):
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(responses):
                return responses[idx]
            return []

        return mock_query

    @patch.object(handler, "run_athena_query")
    def test_elevated_resting_hr_triggers_alert(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "8"}],
            # latest — RHR 70 > 60 + 1.5*3 = 64.5
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "70", "hrv_ms": "45", "readiness_score": "75"}],
            # overtraining
            [{"date": "2025-01-15", "overtraining_risk": "low_risk"}],
            # readiness trend (no decline)
            [
                {"date": "2025-01-15", "readiness_score": "75"},
                {"date": "2025-01-14", "readiness_score": "73"},
                {"date": "2025-01-13", "readiness_score": "70"},
            ],
        ])

        alerts = handler.check_alerts()
        conditions = [a["condition"] for a in alerts]
        self.assertIn("Elevated Resting Heart Rate", conditions)

    @patch.object(handler, "run_athena_query")
    def test_depressed_hrv_triggers_alert(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "5"}],
            # latest — HRV 30 < 45 - 1.5*5 = 37.5
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "60", "hrv_ms": "30", "readiness_score": "75"}],
            # overtraining
            [{"date": "2025-01-15", "overtraining_risk": "low_risk"}],
            # readiness trend
            [
                {"date": "2025-01-15", "readiness_score": "75"},
                {"date": "2025-01-14", "readiness_score": "73"},
                {"date": "2025-01-13", "readiness_score": "70"},
            ],
        ])

        alerts = handler.check_alerts()
        conditions = [a["condition"] for a in alerts]
        self.assertIn("Depressed HRV", conditions)

    @patch.object(handler, "run_athena_query")
    def test_overtraining_high_risk_triggers_alert(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "5"}],
            # latest — normal values
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "60", "hrv_ms": "45", "readiness_score": "75"}],
            # overtraining = high_risk
            [{"date": "2025-01-15", "overtraining_risk": "high_risk"}],
            # readiness trend (no decline)
            [
                {"date": "2025-01-15", "readiness_score": "75"},
                {"date": "2025-01-14", "readiness_score": "73"},
                {"date": "2025-01-13", "readiness_score": "70"},
            ],
        ])

        alerts = handler.check_alerts()
        conditions = [a["condition"] for a in alerts]
        self.assertIn("High Overtraining Risk", conditions)

    @patch.object(handler, "run_athena_query")
    def test_readiness_declining_triggers_alert(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "5"}],
            # latest
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "60", "hrv_ms": "45", "readiness_score": "65"}],
            # overtraining
            [{"date": "2025-01-15", "overtraining_risk": "low_risk"}],
            # readiness declining: 80 -> 73 -> 65
            [
                {"date": "2025-01-15", "readiness_score": "65"},
                {"date": "2025-01-14", "readiness_score": "73"},
                {"date": "2025-01-13", "readiness_score": "80"},
            ],
        ])

        alerts = handler.check_alerts()
        conditions = [a["condition"] for a in alerts]
        self.assertIn("Readiness Declining", conditions)

    @patch.object(handler, "run_athena_query")
    def test_no_alerts_when_all_normal(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "5"}],
            # latest — all within range
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "61", "hrv_ms": "44", "readiness_score": "80"}],
            # overtraining = low
            [{"date": "2025-01-15", "overtraining_risk": "low_risk"}],
            # readiness improving
            [
                {"date": "2025-01-15", "readiness_score": "80"},
                {"date": "2025-01-14", "readiness_score": "78"},
                {"date": "2025-01-13", "readiness_score": "82"},
            ],
        ])

        alerts = handler.check_alerts()
        self.assertEqual(len(alerts), 0)

    @patch.object(handler, "run_athena_query")
    def test_multiple_alerts_can_trigger(self, mock_query):
        mock_query.side_effect = self._mock_run_athena([
            # baseline
            [{"rhr_mean": "60", "rhr_std": "3", "hrv_mean": "45", "hrv_std": "5"}],
            # latest — both HR elevated and HRV depressed
            [{"date": "2025-01-15", "resting_heart_rate_bpm": "70", "hrv_ms": "30", "readiness_score": "55"}],
            # overtraining = high
            [{"date": "2025-01-15", "overtraining_risk": "high_risk"}],
            # readiness declining
            [
                {"date": "2025-01-15", "readiness_score": "55"},
                {"date": "2025-01-14", "readiness_score": "65"},
                {"date": "2025-01-13", "readiness_score": "75"},
            ],
        ])

        alerts = handler.check_alerts()
        self.assertEqual(len(alerts), 4)


class TestLambdaHandler(unittest.TestCase):
    @patch.object(handler, "check_alerts")
    @patch.object(handler, "publish_alerts")
    def test_handler_returns_200(self, mock_publish, mock_check):
        mock_check.return_value = []
        result = handler.lambda_handler({}, None)
        self.assertEqual(result["statusCode"], 200)

    @patch.object(handler, "check_alerts")
    @patch.object(handler, "publish_alerts")
    def test_handler_publishes_when_alerts_exist(self, mock_publish, mock_check):
        mock_check.return_value = [{"condition": "Test", "message": "Test", "severity": "info"}]
        handler.lambda_handler({}, None)
        mock_publish.assert_called_once()

    @patch.object(handler, "check_alerts")
    @patch.object(handler, "publish_alerts")
    def test_handler_skips_publish_when_no_alerts(self, mock_publish, mock_check):
        mock_check.return_value = []
        handler.lambda_handler({}, None)
        mock_publish.assert_not_called()

    @patch.object(handler, "check_alerts")
    def test_handler_returns_500_on_error(self, mock_check):
        mock_check.side_effect = RuntimeError("Query failed")
        result = handler.lambda_handler({}, None)
        self.assertEqual(result["statusCode"], 500)


if __name__ == "__main__":
    unittest.main()
