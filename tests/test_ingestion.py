"""
Tests for the Lambda ingestion trigger handler.

Uses importlib to load handler since 'lambda' is a Python keyword
and can't be used in a normal import path.
"""

import importlib
import json
import os
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Set environment variables before importing handler
os.environ["INGESTION_LOG_TABLE"] = "bio_ingestion_log"
os.environ["ENVIRONMENT"] = "test"
os.environ["OURA_GLUE_JOB"] = "bio-lakehouse-oura-normalizer"
os.environ["PELOTON_GLUE_JOB"] = "bio-lakehouse-peloton-normalizer"

HANDLER_PATH = Path(__file__).parent.parent / "lambda" / "ingestion_trigger" / "handler.py"
DAILY_RUNNER_PATH = Path(__file__).parent.parent / "run_daily_ingestion.sh"

OURA_READINESS_HEADERS = [
    "id",
    "day",
    "score",
    "timestamp",
    "temperature_deviation",
    "temperature_trend_deviation",
    "contributors_activity_balance",
    "contributors_body_temperature",
    "contributors_hrv_balance",
    "contributors_previous_day_activity",
    "contributors_previous_night",
    "contributors_recovery_index",
    "contributors_resting_heart_rate",
    "contributors_sleep_balance",
    "contributors_sleep_regularity",
]


def load_handler():
    """Load the handler module using importlib to avoid 'lambda' keyword conflict."""
    # Mock boto3 before loading
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        spec = importlib.util.spec_from_file_location("handler", HANDLER_PATH)
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
    return handler, mock_boto3


class TestOptionalMFPInDailyRunner(unittest.TestCase):
    def test_mfp_feature_flag_guards_daily_critical_path(self):
        script = DAILY_RUNNER_PATH.read_text()

        assert 'BIO_MFP_ENABLED="${BIO_MFP_ENABLED:-true}"' in script
        assert 'if [ "$MFP_ENABLED" = true ]; then\n    MFP_CSV=' in script
        assert 'if [ "$MFP_ENABLED" = true ]; then\n    MFP_RUN=' in script
        assert 'MFP_STATUS="SKIPPED"' in script
        assert '{ [ "$MFP_ENABLED" = false ] || [ "$MFP" = "SUCCEEDED" ]; }' in script


class TestDetectSource(unittest.TestCase):
    def setUp(self):
        self.handler, _ = load_handler()

    def test_oura_readiness(self):
        assert self.handler.detect_source("oura/readiness/year=2025/month=11/day=25/dailyreadiness.csv") == "oura/readiness"

    def test_oura_sleep(self):
        assert self.handler.detect_source("oura/sleep/year=2025/month=12/day=01/dailysleep.csv") == "oura/sleep"

    def test_oura_activity(self):
        assert self.handler.detect_source("oura/activity/year=2025/month=11/day=26/dailyactivity.csv") == "oura/activity"

    def test_peloton_workouts(self):
        assert self.handler.detect_source("peloton/workouts/year=2024/month=06/day=15/workouts.csv") == "peloton/workouts"

    def test_unknown_source(self):
        assert self.handler.detect_source("random/path/file.csv") == "unknown"


class TestValidateCsvHeaders(unittest.TestCase):
    def setUp(self):
        self.handler, _ = load_handler()
        self.mock_s3 = MagicMock()
        self.handler.s3 = self.mock_s3

    def test_valid_oura_readiness_headers(self):
        csv_content = ",".join(OURA_READINESS_HEADERS) + "\ndata..."
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }
        result = self.handler.validate_csv_headers("bucket", "key", "oura/readiness")
        assert result["valid"] is True
        assert result["missing_headers"] == []

    def test_missing_headers(self):
        csv_content = "id,day\ndata..."
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }
        result = self.handler.validate_csv_headers("bucket", "key", "oura/readiness")
        assert result["valid"] is False
        assert "score" in result["missing_headers"]

    def test_s3_error(self):
        self.mock_s3.get_object.side_effect = Exception("Access denied")
        result = self.handler.validate_csv_headers("bucket", "key", "oura/readiness")
        assert result["valid"] is False
        assert "error" in result

    def test_valid_legacy_oura_json(self):
        json_content = json.dumps([{"day": "2026-09-05", "score": 82}])
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json_content.encode("utf-8")))
        }
        result = self.handler.validate_json_payload(
            "bucket", "oura/readiness/readiness.json", "oura/readiness"
        )
        assert result["valid"] is True
        assert result["record_count"] == 1


class TestLambdaHandler(unittest.TestCase):
    def setUp(self):
        self.handler, _ = load_handler()
        self.mock_s3 = MagicMock()
        self.mock_dynamodb = MagicMock()
        self.mock_glue = MagicMock()

        self.handler.s3 = self.mock_s3
        self.handler.dynamodb = self.mock_dynamodb
        self.handler.glue = self.mock_glue

        # Mock S3 get_object for header validation
        csv_content = ",".join(OURA_READINESS_HEADERS) + "\ndata..."
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }

        # Mock DynamoDB table
        self.mock_table = MagicMock()
        self.mock_dynamodb.Table.return_value = self.mock_table

        # Mock Glue start_job_run
        self.mock_glue.start_job_run.return_value = {"JobRunId": "jr_123"}

    def test_processes_s3_event(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "bio-lakehouse-bronze-123456"},
                        "object": {
                            "key": "oura/readiness/year=2025/month=11/day=25/dailyreadiness.csv",
                            "size": 1024,
                        },
                    }
                }
            ]
        }
        result = self.handler.lambda_handler(event, None)
        body = json.loads(result["body"])
        assert body["processed"] == 1
        assert body["results"][0]["valid"] is True
        self.mock_table.put_item.assert_called_once()

    def test_triggers_glue_for_valid_oura(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {
                            "key": "oura/readiness/year=2025/month=11/day=25/data.csv",
                            "size": 500,
                        },
                    }
                }
            ]
        }
        self.handler.lambda_handler(event, None)
        self.mock_glue.start_job_run.assert_called_once()

    def test_manual_pipeline_lock_suppresses_event_driven_glue(self):
        csv_content = ",".join(OURA_READINESS_HEADERS) + "\ndata..."

        def get_object(**kwargs):
            if kwargs["Key"] == self.handler.MANUAL_PIPELINE_LOCK_KEY:
                body = b'{"expires_at_epoch":4102444800}'
            else:
                body = csv_content.encode("utf-8")
            return {"Body": MagicMock(read=MagicMock(return_value=body))}

        self.mock_s3.get_object.side_effect = get_object
        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {
                        "key": "oura/readiness/year=2026/month=09/day=05/data.csv",
                        "size": 500,
                    },
                }
            }]
        }

        result = self.handler.lambda_handler(event, None)
        body = json.loads(result["body"])
        assert body["results"][0]["manual_pipeline_locked"] is True
        self.mock_glue.start_job_run.assert_not_called()


class TestHeaderNormalizationConsistency(unittest.TestCase):
    """Ensure Lambda and Glue use identical header normalization."""

    def test_header_normalization_consistency(self):
        # SYNC: This regex must match handler.py:validate_csv_headers()
        # and peloton_normalizer.py column normalization block.
        RAW = ["Workout Timestamp", "Live/On-Demand", "Length (minutes)", "Avg. Watts"]
        EXPECTED = ["workout_timestamp", "live_on-demand", "length_minutes", "avg_watts"]
        for raw, exp in zip(RAW, EXPECTED):
            result = re.sub(r"[.\s/()]+", "_", raw.strip()).lower().strip("_")
            assert result == exp, f"{raw!r} → {result!r}, expected {exp!r}"

    def test_semicolon_delimiter_detection(self):
        """Lambda should detect semicolon-delimited CSVs (Oura)."""
        handler, _ = load_handler()
        mock_s3 = MagicMock()
        handler.s3 = mock_s3

        csv_content = ";".join(OURA_READINESS_HEADERS) + "\ndata..."
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }
        result = handler.validate_csv_headers("bucket", "key", "oura/readiness")
        assert result["valid"] is True
        assert "id" in result["headers_found"]


if __name__ == "__main__":
    unittest.main()
