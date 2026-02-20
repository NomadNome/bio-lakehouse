"""
Tests for FHIR R4 Bundle builder (Streamlit export).
"""

import json
import unittest
from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from insights_engine.fhir.bundle_builder import (
    FHIR_LOINC_CODES,
    FHIR_UCUM_UNITS,
    FHIRBundleBuilder,
    _build_observation,
    _build_patient_resource,
    _create_fhir_id,
)


class TestFHIRIDGeneration(unittest.TestCase):
    def test_deterministic(self):
        id1 = _create_fhir_id("healthkit", "heart_rate", "2025-01-15")
        id2 = _create_fhir_id("healthkit", "heart_rate", "2025-01-15")
        self.assertEqual(id1, id2)

    def test_different_inputs_different_ids(self):
        id1 = _create_fhir_id("healthkit", "heart_rate", "2025-01-15")
        id2 = _create_fhir_id("healthkit", "hrv", "2025-01-15")
        self.assertNotEqual(id1, id2)


class TestBuildObservation(unittest.TestCase):
    def test_heart_rate_observation(self):
        obs = _build_observation("healthkit", "heart_rate", "2025-01-15", 62.5)
        self.assertEqual(obs["resourceType"], "Observation")
        self.assertEqual(obs["code"]["coding"][0]["code"], "8867-4")
        self.assertEqual(obs["valueQuantity"]["value"], 62.5)
        self.assertEqual(obs["valueQuantity"]["unit"], "/min")
        self.assertEqual(obs["status"], "final")

    def test_hrv_observation(self):
        obs = _build_observation("healthkit", "hrv", "2025-01-15", 45.0)
        self.assertEqual(obs["code"]["coding"][0]["code"], "80404-7")
        self.assertEqual(obs["valueQuantity"]["unit"], "ms")

    def test_vo2_max_observation(self):
        obs = _build_observation("healthkit", "vo2_max", "2025-01-15", 42.3)
        self.assertEqual(obs["code"]["coding"][0]["code"], "60842-2")
        self.assertEqual(obs["valueQuantity"]["unit"], "mL/kg/min")

    def test_body_weight_observation(self):
        obs = _build_observation("healthkit", "body_weight", "2025-01-15", 175.0)
        self.assertEqual(obs["code"]["coding"][0]["code"], "29463-7")
        self.assertEqual(obs["valueQuantity"]["unit"], "[lb_av]")

    def test_blood_oxygen_observation(self):
        obs = _build_observation("healthkit", "blood_oxygen", "2025-01-15", 98.0)
        self.assertEqual(obs["code"]["coding"][0]["code"], "2708-6")
        self.assertEqual(obs["valueQuantity"]["unit"], "%")

    def test_observation_serializes_to_json(self):
        obs = _build_observation("healthkit", "heart_rate", "2025-01-15", 62.5)
        json_str = json.dumps(obs)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["resourceType"], "Observation")

    def test_effective_datetime_format(self):
        obs = _build_observation("healthkit", "heart_rate", "2025-01-15", 62.5)
        self.assertEqual(obs["effectiveDateTime"], "2025-01-15T00:00:00Z")


class TestBuildPatientResource(unittest.TestCase):
    def test_patient_structure(self):
        patient = _build_patient_resource()
        self.assertEqual(patient["resourceType"], "Patient")
        self.assertEqual(patient["id"], "bio-lakehouse-user-1")
        self.assertTrue(patient["active"])


class TestFHIRBundleBuilder(unittest.TestCase):
    def _make_mock_athena(self, df):
        athena = MagicMock()
        athena.execute_query.return_value = df
        return athena

    def _sample_gold_df(self, n_days=7):
        dates = pd.date_range("2025-01-10", periods=n_days, freq="D")
        return pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "resting_heart_rate_bpm": [62.0 + i for i in range(n_days)],
            "steps": [8000 + i * 100 for i in range(n_days)],
            "hrv_ms": [45.0 + i for i in range(n_days)],
            "vo2_max": [42.0] * n_days,
            "weight_lbs": [175.0] * n_days,
            "blood_oxygen_pct": [98.0] * n_days,
        })

    def test_bundle_structure(self):
        df = self._sample_gold_df()
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 16))

        self.assertEqual(bundle["resourceType"], "Bundle")
        self.assertEqual(bundle["type"], "collection")
        self.assertIn("timestamp", bundle)
        self.assertIn("entry", bundle)
        self.assertGreater(len(bundle["entry"]), 0)

    def test_bundle_contains_patient(self):
        df = self._sample_gold_df()
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 16))

        patient_entries = [
            e for e in bundle["entry"]
            if e["resource"]["resourceType"] == "Patient"
        ]
        self.assertEqual(len(patient_entries), 1)

    def test_bundle_contains_observations(self):
        df = self._sample_gold_df(n_days=3)
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 12))

        obs_entries = [
            e for e in bundle["entry"]
            if e["resource"]["resourceType"] == "Observation"
        ]
        # 3 days * 6 metrics = 18 observations
        self.assertEqual(len(obs_entries), 18)

    def test_bundle_total_matches_entries(self):
        df = self._sample_gold_df()
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 16))

        self.assertEqual(bundle["total"], len(bundle["entry"]))

    def test_empty_data_returns_patient_only(self):
        df = pd.DataFrame(columns=[
            "date", "resting_heart_rate_bpm", "steps", "hrv_ms",
            "vo2_max", "weight_lbs", "blood_oxygen_pct",
        ])
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 16))

        self.assertEqual(len(bundle["entry"]), 1)  # Patient only
        self.assertEqual(bundle["entry"][0]["resource"]["resourceType"], "Patient")

    def test_null_values_excluded(self):
        df = pd.DataFrame({
            "date": ["2025-01-10"],
            "resting_heart_rate_bpm": [62.0],
            "steps": [None],
            "hrv_ms": [None],
            "vo2_max": [None],
            "weight_lbs": [None],
            "blood_oxygen_pct": [None],
        })
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 10))

        obs_entries = [
            e for e in bundle["entry"]
            if e["resource"]["resourceType"] == "Observation"
        ]
        self.assertEqual(len(obs_entries), 1)  # Only heart rate

    def test_build_json_returns_valid_json(self):
        df = self._sample_gold_df(n_days=2)
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        json_str = builder.build_json(date(2025, 1, 10), date(2025, 1, 11))
        parsed = json.loads(json_str)
        self.assertEqual(parsed["resourceType"], "Bundle")

    def test_fullurl_format(self):
        df = self._sample_gold_df(n_days=1)
        athena = self._make_mock_athena(df)
        builder = FHIRBundleBuilder(athena)

        bundle = builder.build(date(2025, 1, 10), date(2025, 1, 10))

        for entry in bundle["entry"]:
            self.assertTrue(entry["fullUrl"].startswith("urn:uuid:"))


if __name__ == "__main__":
    unittest.main()
