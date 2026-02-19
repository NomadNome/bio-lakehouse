# FHIR R4 Observation Mapping Specification

## Overview

The Bio Lakehouse FHIR module transforms Silver-layer health metrics into HL7 FHIR R4 Observation resources as NDJSON, enabling interoperability with EHR systems.

## Input Schemas

### Heart Rate (HealthKit Daily Vitals)

| Column | Type | Example |
|--------|------|---------|
| `date` | String | `"2025-01-15"` |
| `resting_heart_rate_bpm` | Double | `62.5` |

**Source path:** `s3://{SILVER}/healthkit_daily_vitals/`

### Steps (Oura Daily Activity)

| Column | Type | Example |
|--------|------|---------|
| `day` | String | `"2025-01-15"` |
| `steps` | Integer | `8500` |

**Source path:** `s3://{SILVER}/oura_daily_activity/`

## FHIR R4 Observation Output

Each daily data point produces one FHIR Observation resource.

### Heart Rate Example

```json
{
  "resourceType": "Observation",
  "id": "a1b2c3d4-e5f6-5a7b-8c9d-0e1f2a3b4c5d",
  "status": "final",
  "category": [
    {
      "coding": [
        {
          "system": "http://terminology.hl7.org/CodeSystem/observation-category",
          "code": "vital-signs",
          "display": "Vital Signs"
        }
      ]
    }
  ],
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "8867-4",
        "display": "Heart rate"
      }
    ],
    "text": "Heart rate"
  },
  "subject": {
    "reference": "Patient/bio-lakehouse-user-1"
  },
  "effectiveDateTime": "2025-01-15T00:00:00Z",
  "valueQuantity": {
    "value": 62.5,
    "unit": "/min",
    "system": "http://unitsofmeasure.org",
    "code": "/min"
  }
}
```

### Steps Example

```json
{
  "resourceType": "Observation",
  "id": "f6e5d4c3-b2a1-5098-7654-3210fedcba98",
  "status": "final",
  "category": [
    {
      "coding": [
        {
          "system": "http://terminology.hl7.org/CodeSystem/observation-category",
          "code": "activity",
          "display": "Activity"
        }
      ]
    }
  ],
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "55423-8",
        "display": "Number of steps in 24 hour Measured"
      }
    ],
    "text": "Number of steps in 24 hour Measured"
  },
  "subject": {
    "reference": "Patient/bio-lakehouse-user-1"
  },
  "effectiveDateTime": "2025-01-15T00:00:00Z",
  "valueQuantity": {
    "value": 8500,
    "unit": "/d",
    "system": "http://unitsofmeasure.org",
    "code": "/d"
  }
}
```

## LOINC / UCUM Reference

| Metric | LOINC Code | LOINC Display | UCUM Unit | UCUM Code |
|--------|-----------|---------------|-----------|-----------|
| Heart Rate | `8867-4` | Heart rate | beats per minute | `/min` |
| Steps | `55423-8` | Number of steps in 24 hour Measured | steps per day | `/d` |

## ID Generation Strategy

Resource IDs are deterministic UUID v5 values generated from a composite key:

```
namespace: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 (DNS namespace)
key:       {source}:{metric_type}:{date}
```

**Examples:**
- `healthkit:heart_rate:2025-01-15` → always produces the same UUID
- `oura:steps:2025-01-15` → always produces a different, but consistent UUID

This ensures idempotent reruns overwrite with identical IDs rather than creating duplicates.

## Output Format

- **Format:** NDJSON (Newline-Delimited JSON) — one JSON object per line
- **S3 Path:** `s3://{GOLD}/fhir_observations/year=YYYY/month=MM/`
- **Partitioning:** By year and month, derived from `effectiveDateTime`
- **Write mode:** Overwrite (full refresh each run)

## Patient Reference

The `subject.reference` field is configurable via the `--patient_reference` Glue job argument. Default: `Patient/bio-lakehouse-user-1`.
