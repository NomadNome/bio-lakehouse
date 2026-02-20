"""
Tests for HealthKit integration: parser, ingestion trigger, and schemas.
"""

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# -------------------------------------------------------
# Parser tests (pure Python, no Spark needed)
# -------------------------------------------------------

# Add scripts dir to path
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from parse_healthkit_export import (
    BODY_HEADERS,
    HealthKitAccumulator,
    normalize_workout_type,
    parse_date,
    parse_datetime_iso,
    safe_float,
    safe_int,
)


class TestHealthKitParser(unittest.TestCase):
    """Tests for parse_healthkit_export.py helper functions and accumulator."""

    def test_parse_date(self):
        assert parse_date("2025-11-25 08:30:00 -0500") == "2025-11-25"

    def test_parse_date_none(self):
        assert parse_date(None) is None
        assert parse_date("") is None

    def test_parse_datetime_iso(self):
        result = parse_datetime_iso("2025-11-25 08:30:00 -0500")
        assert result == "2025-11-25T08:30:00-05:00"

    def test_normalize_workout_type_hiking(self):
        assert normalize_workout_type("HKWorkoutActivityTypeHiking") == "hiking"

    def test_normalize_workout_type_functional_strength(self):
        result = normalize_workout_type("HKWorkoutActivityTypeFunctionalStrengthTraining")
        assert result == "functional_strength_training"

    def test_normalize_workout_type_swimming(self):
        assert normalize_workout_type("HKWorkoutActivityTypeSwimming") == "swimming"

    def test_normalize_workout_type_none(self):
        assert normalize_workout_type(None) == "unknown"

    def test_normalize_workout_type_hiit(self):
        result = normalize_workout_type("HKWorkoutActivityTypeHighIntensityIntervalTraining")
        assert result == "high_intensity_interval_training"

    def test_safe_float(self):
        assert safe_float("3.14") == 3.14
        assert safe_float(None) is None
        assert safe_float("") is None
        assert safe_float("abc") is None

    def test_safe_int(self):
        assert safe_int("42") == 42
        assert safe_int("42.7") == 43
        assert safe_int(None) is None

    def test_vital_aggregation_last_of_day(self):
        acc = HealthKitAccumulator()
        # Add multiple resting HR readings for same day — last should win
        acc.add_vital("HKQuantityTypeIdentifierRestingHeartRate", "2025-11-25", 62.0)
        acc.add_vital("HKQuantityTypeIdentifierRestingHeartRate", "2025-11-25", 58.0)
        rows = acc.aggregate_vitals()
        assert len(rows) == 1
        assert rows[0]["resting_heart_rate_bpm"] == 58.0  # last value

    def test_vital_aggregation_mean(self):
        acc = HealthKitAccumulator()
        # SpO2 uses mean aggregation
        acc.add_vital("HKQuantityTypeIdentifierOxygenSaturation", "2025-11-25", 0.97)
        acc.add_vital("HKQuantityTypeIdentifierOxygenSaturation", "2025-11-25", 0.98)
        rows = acc.aggregate_vitals()
        assert len(rows) == 1
        # Mean of 0.97 and 0.98 = 0.975, converted to % = 97.5
        assert rows[0]["blood_oxygen_pct"] == 97.5

    def test_spo2_decimal_to_pct(self):
        acc = HealthKitAccumulator()
        acc.add_vital("HKQuantityTypeIdentifierOxygenSaturation", "2025-11-25", 0.975)
        rows = acc.aggregate_vitals()
        assert rows[0]["blood_oxygen_pct"] == 97.5

    def test_body_kg_to_lbs(self):
        acc = HealthKitAccumulator()
        acc.add_body("HKQuantityTypeIdentifierBodyMass", "2025-11-25", 80.0, "kg")
        rows = acc.aggregate_body()
        assert len(rows) == 1
        assert rows[0]["weight_lbs"] == round(80.0 * 2.20462, 1)

    def test_body_fat_decimal_to_pct(self):
        acc = HealthKitAccumulator()
        acc.add_body("HKQuantityTypeIdentifierBodyFatPercentage", "2025-11-25", 0.18, "%")
        rows = acc.aggregate_body()
        assert rows[0]["body_fat_pct"] == 18.0

    def test_mindfulness_aggregation(self):
        acc = HealthKitAccumulator()
        acc.add_mindfulness("2025-11-25", 10.0)
        acc.add_mindfulness("2025-11-25", 15.0)
        acc.add_mindfulness("2025-11-26", 20.0)
        rows = acc.aggregate_mindfulness()
        assert len(rows) == 2
        # Day 1: 25 minutes, 2 sessions
        assert rows[0]["date"] == "2025-11-25"
        assert rows[0]["duration_minutes"] == 25.0
        assert rows[0]["session_count"] == 2
        # Day 2: 20 minutes, 1 session
        assert rows[1]["duration_minutes"] == 20.0
        assert rows[1]["session_count"] == 1

    def test_peloton_workout_filter(self):
        """Workouts from Peloton source should be excluded at parser level."""
        acc = HealthKitAccumulator()
        # This workout should NOT be added (parser filters before calling add_workout)
        # The parser checks sourceName.lower() contains "peloton" and skips
        # Here we test that the accumulator stores only what's given to it
        acc.add_workout({
            "date": "2025-11-25",
            "workout_type": "cycling",
            "source_app": "Apple Watch",
            "duration_minutes": 30,
            "calories_burned": 200,
        })
        assert len(acc.workouts) == 1

    def test_hrv_ms_stored(self):
        acc = HealthKitAccumulator()
        acc.add_vital("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", "2025-11-25", 42.0)
        rows = acc.aggregate_vitals()
        assert rows[0]["hrv_ms"] == 42.0

    def test_device_name_in_body_headers(self):
        assert "device_name" in BODY_HEADERS

    def test_device_name_flows_through_aggregate_body(self):
        acc = HealthKitAccumulator()
        acc.add_body(
            "HKQuantityTypeIdentifierBodyMass", "2026-02-19", 80.0, "kg",
            source_name="Hume Health"
        )
        acc.add_body(
            "HKQuantityTypeIdentifierBodyFatPercentage", "2026-02-19", 0.19, "%",
            source_name="Hume Health"
        )
        rows = acc.aggregate_body()
        assert len(rows) == 1
        assert rows[0]["device_name"] == "Hume Health"

    def test_device_name_defaults_to_empty(self):
        acc = HealthKitAccumulator()
        acc.add_body("HKQuantityTypeIdentifierBodyMass", "2025-11-25", 80.0, "kg")
        rows = acc.aggregate_body()
        assert rows[0]["device_name"] == ""


# -------------------------------------------------------
# Ingestion trigger tests
# -------------------------------------------------------

os.environ["INGESTION_LOG_TABLE"] = "bio_ingestion_log"
os.environ["ENVIRONMENT"] = "test"
os.environ["OURA_GLUE_JOB"] = "bio-lakehouse-oura-normalizer"
os.environ["PELOTON_GLUE_JOB"] = "bio-lakehouse-peloton-normalizer"
os.environ["HEALTHKIT_GLUE_JOB"] = "bio-lakehouse-healthkit-normalizer"

HANDLER_PATH = Path(__file__).parent.parent / "lambda" / "ingestion_trigger" / "handler.py"


def load_handler():
    """Load the handler module using importlib to avoid 'lambda' keyword conflict."""
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        spec = importlib.util.spec_from_file_location("handler_hk", HANDLER_PATH)
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
    return handler, mock_boto3


class TestHealthKitIngestion(unittest.TestCase):
    """Tests for HealthKit source detection, header validation, and Glue job routing."""

    def setUp(self):
        self.handler, _ = load_handler()

    def test_detect_healthkit_daily_vitals(self):
        key = "healthkit/daily_vitals/year=2026/month=02/day=19/daily_vitals.csv"
        assert self.handler.detect_source(key) == "healthkit/daily_vitals"

    def test_detect_healthkit_workouts(self):
        key = "healthkit/workouts/year=2026/month=02/day=19/workouts.csv"
        assert self.handler.detect_source(key) == "healthkit/workouts"

    def test_detect_healthkit_body(self):
        key = "healthkit/body/year=2026/month=02/day=19/body.csv"
        assert self.handler.detect_source(key) == "healthkit/body"

    def test_detect_healthkit_mindfulness(self):
        key = "healthkit/mindfulness/year=2026/month=02/day=19/mindfulness.csv"
        assert self.handler.detect_source(key) == "healthkit/mindfulness"

    def test_healthkit_headers_daily_vitals(self):
        expected = self.handler.EXPECTED_HEADERS["healthkit/daily_vitals"]
        assert "date" in expected
        assert "resting_heart_rate_bpm" in expected

    def test_healthkit_headers_workouts(self):
        expected = self.handler.EXPECTED_HEADERS["healthkit/workouts"]
        assert "date" in expected
        assert "workout_type" in expected
        assert "duration_minutes" in expected

    def test_healthkit_headers_body(self):
        expected = self.handler.EXPECTED_HEADERS["healthkit/body"]
        assert "date" in expected
        assert "weight_lbs" in expected

    def test_healthkit_headers_mindfulness(self):
        expected = self.handler.EXPECTED_HEADERS["healthkit/mindfulness"]
        assert "date" in expected
        assert "duration_minutes" in expected

    def test_glue_routing_healthkit(self):
        """HealthKit sources should route to the healthkit normalizer Glue job."""
        self.handler.glue = MagicMock()
        self.handler.glue.start_job_run.return_value = {"JobRunId": "jr_hk_123"}

        run_id = self.handler.trigger_glue_job(
            "healthkit/daily_vitals", "test-bucket", "healthkit/daily_vitals/test.csv"
        )
        self.handler.glue.start_job_run.assert_called_once()
        call_args = self.handler.glue.start_job_run.call_args
        assert call_args[1]["JobName"] == "bio-lakehouse-healthkit-normalizer"
        assert run_id == "jr_hk_123"


# -------------------------------------------------------
# Workout category map tests
# -------------------------------------------------------

# Add glue dir to path for direct import of bio_etl_utils
GLUE_DIR = Path(__file__).parent.parent / "glue"
sys.path.insert(0, str(GLUE_DIR))


class TestHealthKitWorkoutCategories(unittest.TestCase):
    """Tests for HEALTHKIT_WORKOUT_CATEGORY_MAP correctness."""

    def setUp(self):
        # Import without Spark by mocking pyspark
        self.mock_pyspark = MagicMock()
        self.mock_pyspark.sql.types.StructType = MagicMock
        self.mock_pyspark.sql.types.StructField = MagicMock
        self.mock_pyspark.sql.types.StringType = MagicMock
        self.mock_pyspark.sql.types.IntegerType = MagicMock
        self.mock_pyspark.sql.types.DoubleType = MagicMock
        self.mock_pyspark.sql.types.TimestampType = MagicMock

        with patch.dict("sys.modules", {
            "pyspark": self.mock_pyspark,
            "pyspark.sql": self.mock_pyspark.sql,
            "pyspark.sql.types": self.mock_pyspark.sql.types,
            "pyspark.sql.functions": self.mock_pyspark.sql.functions,
            "pyspark.sql": MagicMock(),
        }):
            spec = importlib.util.spec_from_file_location(
                "bio_etl_utils_test", GLUE_DIR / "bio_etl_utils.py"
            )
            self.utils = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.utils)

    def test_hiking_is_cardio_high(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["hiking"] == "cardio_high"

    def test_swimming_is_cardio_high(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["swimming"] == "cardio_high"

    def test_functional_strength_is_strength(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["functional_strength_training"] == "strength_training"

    def test_yoga_is_recovery(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["yoga"] == "recovery"

    def test_walking_is_cardio_low(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["walking"] == "cardio_low"

    def test_all_categories_valid(self):
        valid_categories = {"cardio_high", "cardio_low", "strength_training", "recovery"}
        for workout, category in self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP.items():
            assert category in valid_categories, f"{workout} mapped to invalid category {category}"

    def test_hiit_is_cardio_high(self):
        assert self.utils.HEALTHKIT_WORKOUT_CATEGORY_MAP["high_intensity_interval_training"] == "cardio_high"


if __name__ == "__main__":
    unittest.main()
