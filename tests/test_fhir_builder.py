"""
Tests for FHIR R4 Observation builder utilities and output structure.
"""

import json
import re
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock

# Mock PySpark modules before importing bio_etl_utils
for mod_name in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.context", "pyspark.sql.window",
]:
    sys.modules.setdefault(mod_name, MagicMock())

# Add glue directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "glue"))

from bio_etl_utils import (
    FHIR_CATEGORY_CODES,
    FHIR_LOINC_CODES,
    FHIR_LOINC_DISPLAY,
    FHIR_METRIC_CATEGORY,
    FHIR_NAMESPACE,
    FHIR_REQUIRED_FIELDS,
    FHIR_UCUM_UNITS,
    create_deterministic_fhir_id,
    validate_fhir_observation,
)


class TestFHIRConstants(unittest.TestCase):
    """Test FHIR constant definitions."""

    def test_loinc_codes_present(self):
        self.assertEqual(FHIR_LOINC_CODES["heart_rate"], "8867-4")
        self.assertEqual(FHIR_LOINC_CODES["steps"], "55423-8")

    def test_ucum_units_present(self):
        self.assertEqual(FHIR_UCUM_UNITS["heart_rate"], "/min")
        self.assertEqual(FHIR_UCUM_UNITS["steps"], "/d")

    def test_loinc_display_names(self):
        self.assertIn("heart_rate", FHIR_LOINC_DISPLAY)
        self.assertIn("steps", FHIR_LOINC_DISPLAY)
        self.assertIsInstance(FHIR_LOINC_DISPLAY["heart_rate"], str)

    def test_category_codes_structure(self):
        for key in ["vital-signs", "activity"]:
            cat = FHIR_CATEGORY_CODES[key]
            self.assertIn("coding", cat)
            self.assertIsInstance(cat["coding"], list)
            self.assertEqual(len(cat["coding"]), 1)
            coding = cat["coding"][0]
            self.assertEqual(
                coding["system"],
                "http://terminology.hl7.org/CodeSystem/observation-category",
            )
            self.assertEqual(coding["code"], key)

    def test_metric_to_category_mapping(self):
        self.assertEqual(FHIR_METRIC_CATEGORY["heart_rate"], "vital-signs")
        self.assertEqual(FHIR_METRIC_CATEGORY["steps"], "activity")

    def test_all_metrics_have_complete_mappings(self):
        """Every metric key should appear in all constant dicts."""
        metrics = FHIR_LOINC_CODES.keys()
        for metric in metrics:
            self.assertIn(metric, FHIR_UCUM_UNITS)
            self.assertIn(metric, FHIR_LOINC_DISPLAY)
            self.assertIn(metric, FHIR_METRIC_CATEGORY)


class TestDeterministicFHIRID(unittest.TestCase):
    """Test deterministic FHIR ID generation."""

    def test_same_input_same_output(self):
        id1 = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15")
        id2 = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15")
        self.assertEqual(id1, id2)

    def test_different_source_different_id(self):
        id1 = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15")
        id2 = create_deterministic_fhir_id("oura", "heart_rate", "2025-01-15")
        self.assertNotEqual(id1, id2)

    def test_different_metric_different_id(self):
        id1 = create_deterministic_fhir_id("oura", "heart_rate", "2025-01-15")
        id2 = create_deterministic_fhir_id("oura", "steps", "2025-01-15")
        self.assertNotEqual(id1, id2)

    def test_different_date_different_id(self):
        id1 = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15")
        id2 = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-16")
        self.assertNotEqual(id1, id2)

    def test_valid_uuid_format(self):
        fhir_id = create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15")
        # Should be a valid UUID string
        parsed = uuid.UUID(fhir_id)
        self.assertEqual(str(parsed), fhir_id)
        # UUID v5
        self.assertEqual(parsed.version, 5)

    def test_uses_expected_namespace(self):
        fhir_id = create_deterministic_fhir_id("test", "heart_rate", "2025-01-01")
        expected = str(uuid.uuid5(FHIR_NAMESPACE, "test:heart_rate:2025-01-01"))
        self.assertEqual(fhir_id, expected)


class TestFHIRObservationStructure(unittest.TestCase):
    """Test FHIR Observation resource structure."""

    def _build_sample_observation(self, metric_type="heart_rate", value=62.5):
        """Build a sample FHIR Observation for testing."""
        source = "healthkit" if metric_type == "heart_rate" else "oura"
        category_key = FHIR_METRIC_CATEGORY[metric_type]
        return {
            "resourceType": "Observation",
            "id": create_deterministic_fhir_id(source, metric_type, "2025-01-15"),
            "status": "final",
            "category": [FHIR_CATEGORY_CODES[category_key]],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": FHIR_LOINC_CODES[metric_type],
                        "display": FHIR_LOINC_DISPLAY[metric_type],
                    }
                ],
                "text": FHIR_LOINC_DISPLAY[metric_type],
            },
            "subject": {"reference": "Patient/bio-lakehouse-user-1"},
            "effectiveDateTime": "2025-01-15T00:00:00Z",
            "valueQuantity": {
                "value": value,
                "unit": FHIR_UCUM_UNITS[metric_type],
                "system": "http://unitsofmeasure.org",
                "code": FHIR_UCUM_UNITS[metric_type],
            },
        }

    def test_required_fields_present(self):
        obs = self._build_sample_observation()
        for field in FHIR_REQUIRED_FIELDS:
            self.assertIn(field, obs, f"Missing required field: {field}")

    def test_resource_type_is_observation(self):
        obs = self._build_sample_observation()
        self.assertEqual(obs["resourceType"], "Observation")

    def test_status_is_final(self):
        obs = self._build_sample_observation()
        self.assertEqual(obs["status"], "final")

    def test_loinc_system_url(self):
        obs = self._build_sample_observation()
        coding = obs["code"]["coding"][0]
        self.assertEqual(coding["system"], "http://loinc.org")

    def test_ucum_system_url(self):
        obs = self._build_sample_observation()
        vq = obs["valueQuantity"]
        self.assertEqual(vq["system"], "http://unitsofmeasure.org")

    def test_heart_rate_observation(self):
        obs = self._build_sample_observation("heart_rate", 62.5)
        self.assertEqual(obs["code"]["coding"][0]["code"], "8867-4")
        self.assertEqual(obs["valueQuantity"]["value"], 62.5)
        self.assertEqual(obs["valueQuantity"]["unit"], "/min")
        cat_code = obs["category"][0]["coding"][0]["code"]
        self.assertEqual(cat_code, "vital-signs")

    def test_steps_observation(self):
        obs = self._build_sample_observation("steps", 8500)
        self.assertEqual(obs["code"]["coding"][0]["code"], "55423-8")
        self.assertEqual(obs["valueQuantity"]["value"], 8500)
        self.assertEqual(obs["valueQuantity"]["unit"], "/d")
        cat_code = obs["category"][0]["coding"][0]["code"]
        self.assertEqual(cat_code, "activity")

    def test_iso8601_datetime_format(self):
        obs = self._build_sample_observation()
        dt = obs["effectiveDateTime"]
        # Should match ISO 8601 with UTC timezone
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
        self.assertRegex(dt, pattern)

    def test_serializes_to_valid_json(self):
        obs = self._build_sample_observation()
        json_str = json.dumps(obs)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["resourceType"], "Observation")

    def test_patient_reference_format(self):
        obs = self._build_sample_observation()
        ref = obs["subject"]["reference"]
        self.assertTrue(ref.startswith("Patient/"))


class TestFHIRValidation(unittest.TestCase):
    """Test validate_fhir_observation function."""

    def _build_valid_observation(self):
        return {
            "resourceType": "Observation",
            "id": create_deterministic_fhir_id("healthkit", "heart_rate", "2025-01-15"),
            "status": "final",
            "category": [FHIR_CATEGORY_CODES["vital-signs"]],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "8867-4",
                        "display": "Heart rate",
                    }
                ],
                "text": "Heart rate",
            },
            "subject": {"reference": "Patient/bio-lakehouse-user-1"},
            "effectiveDateTime": "2025-01-15T00:00:00Z",
            "valueQuantity": {
                "value": 62.5,
                "unit": "/min",
                "system": "http://unitsofmeasure.org",
                "code": "/min",
            },
        }

    def test_valid_observation_passes(self):
        obs = self._build_valid_observation()
        self.assertTrue(validate_fhir_observation(obs))

    def test_missing_resource_type_fails(self):
        obs = self._build_valid_observation()
        del obs["resourceType"]
        with self.assertRaises(ValueError) as ctx:
            validate_fhir_observation(obs)
        self.assertIn("resourceType", str(ctx.exception))

    def test_missing_id_fails(self):
        obs = self._build_valid_observation()
        del obs["id"]
        with self.assertRaises(ValueError):
            validate_fhir_observation(obs)

    def test_missing_status_fails(self):
        obs = self._build_valid_observation()
        del obs["status"]
        with self.assertRaises(ValueError):
            validate_fhir_observation(obs)

    def test_missing_value_quantity_fails(self):
        obs = self._build_valid_observation()
        del obs["valueQuantity"]
        with self.assertRaises(ValueError):
            validate_fhir_observation(obs)

    def test_none_field_value_fails(self):
        obs = self._build_valid_observation()
        obs["code"] = None
        with self.assertRaises(ValueError):
            validate_fhir_observation(obs)

    def test_empty_dict_fails(self):
        with self.assertRaises(ValueError):
            validate_fhir_observation({})

    def test_multiple_missing_fields_reported(self):
        obs = {"resourceType": "Observation", "id": "test-id"}
        with self.assertRaises(ValueError) as ctx:
            validate_fhir_observation(obs)
        error_msg = str(ctx.exception)
        self.assertIn("status", error_msg)
        self.assertIn("category", error_msg)


if __name__ == "__main__":
    unittest.main()
