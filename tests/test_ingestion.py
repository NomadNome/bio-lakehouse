"""
Tests for the Lambda ingestion trigger handler.

Uses importlib to load handler since 'lambda' is a Python keyword
and can't be used in a normal import path.
"""

import importlib
import json
import os
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


def load_handler():
    """Load the handler module using importlib to avoid 'lambda' keyword conflict."""
    # Mock boto3 before loading
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        spec = importlib.util.spec_from_file_location("handler", HANDLER_PATH)
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
    return handler, mock_boto3


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
        csv_content = "id,day,score,timestamp,contributors_activity_balance\ndata..."
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
        csv_content = "id,day,score,timestamp\ndata..."
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


if __name__ == "__main__":
    unittest.main()
