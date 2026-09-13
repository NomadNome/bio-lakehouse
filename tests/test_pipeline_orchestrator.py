"""Unit tests for duplicate-safe pipeline orchestration."""

from __future__ import annotations

import importlib.util
import io
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch


HANDLER_PATH = (
    Path(__file__).parent.parent / "lambda" / "pipeline_orchestrator" / "handler.py"
)


def load_handler():
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        spec = importlib.util.spec_from_file_location("pipeline_orchestrator", HANDLER_PATH)
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
    handler.glue = MagicMock()
    handler.lam = MagicMock()
    handler.s3 = MagicMock()
    handler.BRONZE_BUCKET = "test-bronze"
    return handler


def test_active_manual_pipeline_lock():
    handler = load_handler()
    now = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    handler.s3.get_object.return_value = {
        "Body": io.BytesIO(b'{"expires_at_epoch":1788699600}')
    }

    assert handler.is_manual_pipeline_locked(now=now) is True


def test_expired_manual_pipeline_lock():
    handler = load_handler()
    now = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    handler.s3.get_object.return_value = {
        "Body": io.BytesIO(b'{"expires_at_epoch":0}')
    }

    assert handler.is_manual_pipeline_locked(now=now) is False


def test_locked_pipeline_skips_downstream_work():
    handler = load_handler()
    handler.is_manual_pipeline_locked = MagicMock(return_value=True)
    handler.is_job_active = MagicMock(return_value=False)
    handler.wait_for_crawler = MagicMock()
    handler.wait_for_job = MagicMock()

    result = handler.lambda_handler({}, None)

    assert result["statusCode"] == 200
    handler.wait_for_crawler.assert_not_called()
    handler.wait_for_job.assert_not_called()
    handler.lam.invoke.assert_not_called()


def test_automated_pipeline_invokes_briefing_asynchronously():
    handler = load_handler()
    handler.is_manual_pipeline_locked = MagicMock(return_value=False)
    handler.is_job_active = MagicMock(return_value=False)
    handler.wait_for_crawler = MagicMock(return_value=True)
    handler.wait_for_job = MagicMock(return_value=True)

    result = handler.lambda_handler({}, None)

    assert result["statusCode"] == 200
    handler.lam.invoke.assert_called_once_with(
        FunctionName=handler.MORNING_BRIEFING_FN,
        InvocationType="Event",
        Payload=b"{}",
    )
