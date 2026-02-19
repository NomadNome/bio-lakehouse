"""
Bio Lakehouse - FHIR R4 Observation Builder (Glue ETL Job)

Transforms Silver heart rate and steps data into HL7 FHIR R4 Observation
resources as NDJSON, enabling export to EHR systems.

Data sources:
  - Heart rate: Silver healthkit_daily_vitals (resting_heart_rate_bpm, date)
  - Steps: Silver oura_daily_activity (steps, day)

Output:
  - NDJSON at s3://{GOLD}/fhir_observations/ partitioned by year/month

Glue job arguments:
  --silver_bucket: Silver S3 bucket name
  --gold_bucket: Gold S3 bucket name
  --patient_reference: FHIR Patient reference (default: Patient/bio-lakehouse-user-1)
"""

import json
import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from bio_etl_utils import (
    FHIR_CATEGORY_CODES,
    FHIR_LOINC_CODES,
    FHIR_LOINC_DISPLAY,
    FHIR_METRIC_CATEGORY,
    FHIR_UCUM_UNITS,
    create_deterministic_fhir_id,
)

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "silver_bucket", "gold_bucket", "patient_reference"],
)
job.init(args["JOB_NAME"], args)

SILVER_BUCKET = args["silver_bucket"]
GOLD_BUCKET = args["gold_bucket"]
PATIENT_REFERENCE = args.get("patient_reference", "Patient/bio-lakehouse-user-1")

# Validate patient reference format to prevent malformed FHIR output
if not re.match(r"^Patient/[A-Za-z0-9._-]+$", PATIENT_REFERENCE):
    raise ValueError(
        f"Invalid patient_reference format. "
        f"Expected 'Patient/<alphanumeric-id>', got a non-conformant value."
    )

print("Running FHIR Observation builder...")


def read_silver_safe(path):
    """Read Silver parquet, returning None if path doesn't exist yet."""
    try:
        df = spark.read.parquet(path)
        if df.rdd.isEmpty():
            return None
        return df
    except Exception as e:
        print(f"Could not read {path}: {e}")
        return None


# -------------------------------------------------------
# Build FHIR Observation UDF
# -------------------------------------------------------

def build_fhir_observation(source, metric_type, date_str, value, patient_ref):
    """Build a FHIR R4 Observation JSON string.

    Args:
        source: Data source (e.g., "healthkit", "oura")
        metric_type: "heart_rate" or "steps"
        date_str: Date in YYYY-MM-DD format
        value: Numeric measurement value
        patient_ref: FHIR Patient reference string

    Returns:
        JSON string of the FHIR Observation, or None if value is null
    """
    if value is None or date_str is None:
        return None

    category_key = FHIR_METRIC_CATEGORY[metric_type]
    observation = {
        "resourceType": "Observation",
        "id": create_deterministic_fhir_id(source, metric_type, date_str),
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
        "subject": {"reference": patient_ref},
        "effectiveDateTime": f"{date_str}T00:00:00Z",
        "valueQuantity": {
            "value": float(value),
            "unit": FHIR_UCUM_UNITS[metric_type],
            "system": "http://unitsofmeasure.org",
            "code": FHIR_UCUM_UNITS[metric_type],
        },
    }
    return json.dumps(observation)


# Register UDFs
build_hr_observation_udf = F.udf(
    lambda date_str, value: build_fhir_observation(
        "healthkit", "heart_rate", date_str, value, PATIENT_REFERENCE
    ),
    StringType(),
)

build_steps_observation_udf = F.udf(
    lambda date_str, value: build_fhir_observation(
        "oura", "steps", date_str, value, PATIENT_REFERENCE
    ),
    StringType(),
)


# -------------------------------------------------------
# Read Silver data
# -------------------------------------------------------

hr_df = read_silver_safe(f"s3://{SILVER_BUCKET}/healthkit_daily_vitals/")
steps_df = read_silver_safe(f"s3://{SILVER_BUCKET}/oura_daily_activity/")

observations = []

if hr_df is not None:
    hr_obs = (
        hr_df.filter(F.col("resting_heart_rate_bpm").isNotNull())
        .withColumn(
            "value",
            build_hr_observation_udf(F.col("date"), F.col("resting_heart_rate_bpm")),
        )
        .filter(F.col("value").isNotNull())
        .select(F.col("value"), F.col("date"))
    )
    observations.append(hr_obs)
    print(f"Heart rate observations: {hr_obs.count()}")
else:
    print("No HealthKit daily vitals data available")

if steps_df is not None:
    steps_obs = (
        steps_df.filter(F.col("steps").isNotNull())
        .withColumn(
            "value",
            build_steps_observation_udf(F.col("day"), F.col("steps")),
        )
        .filter(F.col("value").isNotNull())
        .select(F.col("value"), F.col("day").alias("date"))
    )
    observations.append(steps_obs)
    print(f"Steps observations: {steps_obs.count()}")
else:
    print("No Oura daily activity data available")


# -------------------------------------------------------
# Union and write NDJSON
# -------------------------------------------------------

if observations:
    all_obs = observations[0]
    for obs_df in observations[1:]:
        all_obs = all_obs.unionByName(obs_df)

    # Add partition columns from date
    all_obs = (
        all_obs.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    # Write as NDJSON (one JSON object per line)
    (
        all_obs.select("value", "year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .text(f"s3://{GOLD_BUCKET}/fhir_observations/")
    )

    total_count = all_obs.count()
    print(f"Wrote {total_count} FHIR Observations to Gold layer")
else:
    print("No observations to write - no source data available")

job.commit()
print("FHIR Observation builder job complete")
