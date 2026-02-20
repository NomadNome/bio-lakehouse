"""
Bio Insights Engine - FHIR R4 Bundle Builder

Builds a FHIR R4 Bundle (type: collection) from Gold layer data for a given
date range. Runs in pure Python (no Spark) for on-demand Streamlit export.

FHIR constants are duplicated here to avoid PySpark import dependency.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime

import pandas as pd

from insights_engine.core.athena_client import AthenaClient


# -------------------------------------------------------
# FHIR Constants (duplicated from bio_etl_utils to avoid
# PySpark dependency in the Streamlit runtime)
# -------------------------------------------------------

FHIR_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

FHIR_LOINC_CODES = {
    "heart_rate": "8867-4",
    "steps": "55423-8",
    "hrv": "80404-7",
    "vo2_max": "60842-2",
    "body_weight": "29463-7",
    "blood_oxygen": "2708-6",
}

FHIR_LOINC_DISPLAY = {
    "heart_rate": "Heart rate",
    "steps": "Number of steps in 24 hour Measured",
    "hrv": "R-R interval.standard deviation (Heart rate variability)",
    "vo2_max": "Oxygen consumption (VO2 max)",
    "body_weight": "Body weight",
    "blood_oxygen": "Oxygen saturation in Arterial blood by Pulse oximetry",
}

FHIR_UCUM_UNITS = {
    "heart_rate": "/min",
    "steps": "/d",
    "hrv": "ms",
    "vo2_max": "mL/kg/min",
    "body_weight": "[lb_av]",
    "blood_oxygen": "%",
}

FHIR_METRIC_CATEGORY = {
    "heart_rate": "vital-signs",
    "steps": "activity",
    "hrv": "vital-signs",
    "vo2_max": "vital-signs",
    "body_weight": "vital-signs",
    "blood_oxygen": "vital-signs",
}

FHIR_CATEGORY_CODES = {
    "vital-signs": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs",
            }
        ]
    },
    "activity": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "activity",
                "display": "Activity",
            }
        ]
    },
}

# Column name → FHIR metric key mapping for Gold table
GOLD_COLUMN_METRIC_MAP = {
    "resting_heart_rate_bpm": ("healthkit", "heart_rate"),
    "steps": ("oura", "steps"),
    "hrv_ms": ("healthkit", "hrv"),
    "vo2_max": ("healthkit", "vo2_max"),
    "weight_lbs": ("healthkit", "body_weight"),
    "blood_oxygen_pct": ("healthkit", "blood_oxygen"),
}

PATIENT_REFERENCE = "Patient/bio-lakehouse-user-1"


def _create_fhir_id(source: str, metric_type: str, date_str: str) -> str:
    composite_key = f"{source}:{metric_type}:{date_str}"
    return str(uuid.uuid5(FHIR_NAMESPACE, composite_key))


def _build_observation(
    source: str, metric_type: str, date_str: str, value: float
) -> dict:
    """Build a single FHIR R4 Observation resource dict."""
    category_key = FHIR_METRIC_CATEGORY[metric_type]
    return {
        "resourceType": "Observation",
        "id": _create_fhir_id(source, metric_type, date_str),
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
        "subject": {"reference": PATIENT_REFERENCE},
        "effectiveDateTime": f"{date_str}T00:00:00Z",
        "valueQuantity": {
            "value": float(value),
            "unit": FHIR_UCUM_UNITS[metric_type],
            "system": "http://unitsofmeasure.org",
            "code": FHIR_UCUM_UNITS[metric_type],
        },
    }


def _build_patient_resource() -> dict:
    """Build a minimal FHIR Patient resource."""
    patient_id = PATIENT_REFERENCE.split("/")[1]
    return {
        "resourceType": "Patient",
        "id": patient_id,
        "active": True,
        "name": [{"text": "Bio Lakehouse User"}],
    }


class FHIRBundleBuilder:
    """Builds a FHIR R4 Bundle from Gold layer Athena data."""

    def __init__(self, athena: AthenaClient):
        self.athena = athena

    def build(self, start_date: date, end_date: date) -> dict:
        """Query Gold data and build a FHIR R4 Bundle (collection).

        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            FHIR R4 Bundle dict
        """
        sql = f"""
        SELECT
            date,
            resting_heart_rate_bpm,
            steps,
            hrv_ms,
            vo2_max,
            weight_lbs,
            blood_oxygen_pct
        FROM bio_gold.daily_readiness_performance
        WHERE COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) >= DATE '{start_date.isoformat()}'
          AND COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) <= DATE '{end_date.isoformat()}'
        ORDER BY date
        """
        df = self.athena.execute_query(sql)

        entries = []

        # Patient resource
        patient = _build_patient_resource()
        entries.append({
            "fullUrl": f"urn:uuid:{_create_fhir_id('system', 'patient', 'bio-lakehouse-user-1')}",
            "resource": patient,
        })

        # Observation resources
        for _, row in df.iterrows():
            date_str = str(row["date"])[:10]  # Ensure YYYY-MM-DD
            for col, (source, metric_type) in GOLD_COLUMN_METRIC_MAP.items():
                value = row.get(col)
                if value is not None and pd.notna(value):
                    try:
                        float_val = float(value)
                        if float_val > 0:
                            obs = _build_observation(source, metric_type, date_str, float_val)
                            entries.append({
                                "fullUrl": f"urn:uuid:{obs['id']}",
                                "resource": obs,
                            })
                    except (ValueError, TypeError):
                        continue

        bundle = {
            "resourceType": "Bundle",
            "id": str(uuid.uuid4()),
            "type": "collection",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "total": len(entries),
            "entry": entries,
        }
        return bundle

    def build_json(self, start_date: date, end_date: date) -> str:
        """Build bundle and return as formatted JSON string."""
        bundle = self.build(start_date, end_date)
        return json.dumps(bundle, indent=2)
