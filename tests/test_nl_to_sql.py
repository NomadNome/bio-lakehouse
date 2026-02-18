"""Tests for the NL-to-SQL engine components."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.core.nl_to_sql import NLToSQLResult, AnswerResult


# ── Dataclass tests ──────────────────────────────────────────────────────

class TestNLToSQLResult:
    def test_defaults(self):
        r = NLToSQLResult(sql="SELECT 1", explanation="test")
        assert r.sql == "SELECT 1"
        assert r.assumptions == []
        assert r.confidence == 0.0

    def test_with_all_fields(self):
        r = NLToSQLResult(
            sql="SELECT *",
            explanation="gets everything",
            assumptions=["all data is clean"],
            confidence=0.95,
        )
        assert r.confidence == 0.95
        assert len(r.assumptions) == 1


class TestAnswerResult:
    def test_defaults(self):
        r = AnswerResult(
            question="test?",
            sql="SELECT 1",
            explanation="",
            assumptions=[],
            confidence=0.9,
            data=pd.DataFrame(),
            answer="42",
            execution_time_ms=100,
            row_count=1,
        )
        assert r.error is None
        assert r.row_count == 1

    def test_with_error(self):
        r = AnswerResult(
            question="bad?",
            sql="",
            explanation="",
            assumptions=[],
            confidence=0.0,
            data=pd.DataFrame(),
            answer="",
            execution_time_ms=50,
            row_count=0,
            error="Translation error",
        )
        assert r.error == "Translation error"


# ── JSON parsing logic tests ─────────────────────────────────────────────

class TestJSONParsing:
    """Test the JSON extraction logic used in NLToSQLEngine.translate()."""

    def _parse(self, raw_text: str) -> dict:
        """Replicate the parsing logic from translate()."""
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            start = raw_text.find("{")
            end = raw_text.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(raw_text[start:end])
            raise ValueError("Could not parse")

    def test_plain_json(self):
        text = '{"sql": "SELECT 1", "explanation": "test", "confidence": 0.9}'
        result = self._parse(text)
        assert result["sql"] == "SELECT 1"

    def test_json_in_code_block(self):
        text = '```json\n{"sql": "SELECT 1", "explanation": "test"}\n```'
        result = self._parse(text)
        assert result["sql"] == "SELECT 1"

    def test_json_in_plain_code_block(self):
        text = '```\n{"sql": "SELECT 1", "explanation": "test"}\n```'
        result = self._parse(text)
        assert result["sql"] == "SELECT 1"

    def test_json_with_surrounding_text(self):
        text = 'Here is the query:\n{"sql": "SELECT 1", "explanation": "test"}\nDone.'
        result = self._parse(text)
        assert result["sql"] == "SELECT 1"

    def test_invalid_json_raises(self):
        with pytest.raises((json.JSONDecodeError, ValueError)):
            self._parse("not json at all")


# ── Prompt file tests ────────────────────────────────────────────────────

PROMPTS_DIR = PROJECT_ROOT / "insights_engine" / "prompts"


class TestPromptFiles:
    def test_system_prompt_exists_and_has_placeholder(self):
        path = PROMPTS_DIR / "nl_to_sql_system.txt"
        assert path.exists()
        content = path.read_text()
        assert "{schema_ddl}" in content
        assert "bio_gold" in content

    def test_examples_file_exists_and_has_questions(self):
        path = PROMPTS_DIR / "nl_to_sql_examples.txt"
        assert path.exists()
        content = path.read_text()
        assert "Q:" in content
        assert "A:" in content
        # Should have multiple examples
        assert content.count("Q:") >= 5

    def test_narrator_prompt_exists(self):
        path = PROMPTS_DIR / "insight_narrator.txt"
        assert path.exists()
        assert len(path.read_text()) > 100


# ── Engine instantiation test (mocked) ──────────────────────────────────

class TestNLToSQLEngineInit:
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_engine_creates_with_mock(self):
        from insights_engine.core.nl_to_sql import NLToSQLEngine

        mock_athena = MagicMock()
        engine = NLToSQLEngine(mock_athena, model="claude-sonnet-4-20250514")
        assert engine.model == "claude-sonnet-4-20250514"
        assert engine.athena is mock_athena

    def test_engine_raises_without_api_key(self):
        from insights_engine.core.nl_to_sql import NLToSQLEngine

        mock_athena = MagicMock()
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                NLToSQLEngine(mock_athena)
