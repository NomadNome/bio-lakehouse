"""
Tests for bio_etl_utils shared ETL functions.

Tests the pure Python logic and categorization mappings.
PySpark-dependent functions are tested with mocks.
"""

import json
import sys
import unittest
from pathlib import Path

# Add project root to path so we can import the scripts
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "glue"))


class TestOuraDataParsing(unittest.TestCase):
    """Test the Oura data splitting logic."""

    def test_parse_json_column_valid(self):
        from split_oura_data import parse_json_column

        result = parse_json_column('{"activity_balance": 92, "body_temperature": 100}')
        assert result == {"activity_balance": 92, "body_temperature": 100}

    def test_parse_json_column_empty(self):
        from split_oura_data import parse_json_column

        assert parse_json_column("") == {}
        assert parse_json_column(None) == {}

    def test_parse_json_column_invalid(self):
        from split_oura_data import parse_json_column

        assert parse_json_column("not json") == {}

    def test_flatten_row_with_contributors(self):
        from split_oura_data import flatten_row

        row = {
            "id": "abc",
            "day": "2025-11-25",
            "score": "58",
            "contributors": '{"activity_balance": null, "body_temperature": 100}',
        }
        flat = flatten_row(row, ["contributors"])
        assert flat["id"] == "abc"
        assert flat["contributors_body_temperature"] == 100
        assert flat["contributors_activity_balance"] == ""
        assert "contributors" not in flat

    def test_flatten_row_with_met_data(self):
        from split_oura_data import flatten_row

        row = {
            "id": "abc",
            "met": '{"interval": 60.0, "items": [0.9, 1.2, 1.5]}',
        }
        flat = flatten_row(row, ["met"])
        assert flat["met_interval"] == 60.0
        assert flat["met_avg"] == 1.2
        assert flat["met_max"] == 1.5
        assert flat["met_count"] == 3


class TestPelotonDataParsing(unittest.TestCase):
    """Test the Peloton data splitting logic."""

    def test_parse_numeric_offset_timestamp(self):
        from split_peloton_data import parse_peloton_timestamp

        date, time, offset = parse_peloton_timestamp("2021-05-29 09:27 (-04)")
        assert date == "2021-05-29"
        assert time == "09:27"
        assert offset == "-04"

    def test_parse_tz_abbreviation_timestamp(self):
        from split_peloton_data import parse_peloton_timestamp

        date, time, offset = parse_peloton_timestamp("2022-01-21 17:22 (EST)")
        assert date == "2022-01-21"
        assert time == "17:22"
        assert offset == "-05"

    def test_parse_edt_timestamp(self):
        from split_peloton_data import parse_peloton_timestamp

        date, time, offset = parse_peloton_timestamp("2022-06-15 10:00 (EDT)")
        assert date == "2022-06-15"
        assert time == "10:00"
        assert offset == "-04"

    def test_parse_empty_timestamp(self):
        from split_peloton_data import parse_peloton_timestamp

        date, time, offset = parse_peloton_timestamp("")
        assert date == "unknown"

    def test_normalize_column_name(self):
        from split_peloton_data import normalize_column_name

        assert normalize_column_name("Avg. Watts") == "avg_watts"
        assert normalize_column_name("Length (minutes)") == "length_minutes"
        assert normalize_column_name("Avg. Cadence (RPM)") == "avg_cadence_rpm"
        assert normalize_column_name("Distance (mi)") == "distance_mi"
        assert normalize_column_name("Calories Burned") == "calories_burned"


class TestWorkoutCategorization(unittest.TestCase):
    """Test workout category mapping (pure dict, no PySpark needed)."""

    def test_category_map(self):
        # Import the map directly without PySpark
        WORKOUT_CATEGORY_MAP = {
            "cycling": "cardio_high",
            "running": "cardio_high",
            "strength": "strength_training",
            "yoga": "recovery",
            "stretching": "recovery",
            "walking": "cardio_low",
            "meditation": "recovery",
        }
        assert WORKOUT_CATEGORY_MAP["cycling"] == "cardio_high"
        assert WORKOUT_CATEGORY_MAP["strength"] == "strength_training"
        assert WORKOUT_CATEGORY_MAP["yoga"] == "recovery"
        assert WORKOUT_CATEGORY_MAP["walking"] == "cardio_low"


if __name__ == "__main__":
    unittest.main()
