"""Configuration-safety tests for the morning briefing Lambda."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


HANDLER_PATH = Path(__file__).parent.parent / "lambda" / "morning_briefing" / "handler.py"


def load_handler(database="bio_diego_gold"):
    mock_boto3 = MagicMock()
    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        patch.dict("os.environ", {"ATHENA_DATABASE": database}),
    ):
        spec = importlib.util.spec_from_file_location("morning_briefing", HANDLER_PATH)
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
    return handler


def test_all_briefing_queries_use_configured_gold_database():
    handler = load_handler()
    queries = []

    def capture(sql):
        queries.append(sql)
        return []

    handler.run_athena_query = capture
    handler._get_coach_session = MagicMock(return_value=None)
    handler._get_latest_discovery = MagicMock(return_value=None)

    handler.build_briefing()

    assert len(queries) == 4
    assert all("bio_diego_gold." in sql for sql in queries)
    assert all("bio_gold." not in sql for sql in queries)


def test_database_identifier_is_validated():
    handler = load_handler("bio_gold; DROP TABLE x")
    with pytest.raises(ValueError, match="Invalid ATHENA_DATABASE"):
        handler._gold_table("daily_readiness_performance")
