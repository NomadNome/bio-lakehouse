"""Tests for AthenaClient utility methods."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.core.athena_client import AthenaClient, Column


class TestColumn:
    def test_creation(self):
        c = Column(name="readiness_score", type="integer")
        assert c.name == "readiness_score"
        assert c.comment == ""

    def test_with_comment(self):
        c = Column(name="date", type="varchar", comment="ISO date")
        assert c.comment == "ISO date"


class TestCoerceTypes:
    """Test _coerce_types static method without needing AWS credentials."""

    def test_numeric_columns(self):
        df = pd.DataFrame({
            "score": ["85", "90", "77", "82"],
            "output": ["123.4", "456.7", "789.0", "111.2"],
        })
        result = AthenaClient._coerce_types(df)
        assert pd.api.types.is_numeric_dtype(result["score"])
        assert pd.api.types.is_numeric_dtype(result["output"])
        assert result["score"].iloc[0] == 85

    def test_date_columns(self):
        df = pd.DataFrame({
            "date": ["2026-02-10", "2026-02-11", "2026-02-12"],
            "timestamp": ["2026-02-10 08:00:00", "2026-02-11 09:00:00", "2026-02-12 10:00:00"],
        })
        result = AthenaClient._coerce_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_string_columns_unchanged(self):
        df = pd.DataFrame({
            "name": ["cycling", "strength", "yoga"],
            "disciplines": ["Cycling", "Strength,Yoga", "Yoga"],
        })
        result = AthenaClient._coerce_types(df)
        assert result["name"].dtype == object
        assert result["disciplines"].iloc[0] == "Cycling"

    def test_mixed_numeric_with_nulls(self):
        df = pd.DataFrame({
            "score": ["85", None, "77", "82"],
        })
        result = AthenaClient._coerce_types(df)
        assert pd.api.types.is_numeric_dtype(result["score"])
        assert pd.isna(result["score"].iloc[1])

    def test_mostly_non_numeric_stays_string(self):
        df = pd.DataFrame({
            "notes": ["good day", "bad day", "123", "ok"],
        })
        result = AthenaClient._coerce_types(df)
        # Only 1 out of 4 is numeric (25%), below 50% threshold
        assert result["notes"].dtype == object

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = AthenaClient._coerce_types(df)
        assert result.empty


class TestCoerceTypesWeekStart:
    def test_week_start_column_parsed_as_date(self):
        df = pd.DataFrame({
            "week_start": ["2026-02-03", "2026-02-10"],
        })
        result = AthenaClient._coerce_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["week_start"])
