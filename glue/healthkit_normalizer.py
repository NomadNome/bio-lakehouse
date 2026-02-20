"""
Bio Lakehouse - Apple HealthKit Data Normalizer (Glue ETL Job)

Reads Bronze-layer HealthKit CSVs (daily_vitals, workouts, body, mindfulness),
normalizes them, and writes partitioned Parquet to the Silver layer.

Glue job arguments:
  --bronze_bucket: Bronze S3 bucket name
  --silver_bucket: Silver S3 bucket name
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from bio_etl_utils import (
    HEALTHKIT_WORKOUT_CATEGORY_MAP,
    forward_fill,
    validate_schema,
)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Disable automatic partition inference — Bronze paths use year=/month=/day=
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

# Parse job arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_bucket",
        "silver_bucket",
    ],
)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["bronze_bucket"]
SILVER_BUCKET = args["silver_bucket"]


def read_bronze_csv(path):
    """Read CSV with partition discovery disabled to avoid column shadowing."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("basePath", path)
        .option("recursiveFileLookup", "true")
        .csv(path)
    )


def process_daily_vitals():
    """Normalize HealthKit daily vitals data."""
    print("Processing HealthKit daily vitals...")

    path = f"s3://{BRONZE_BUCKET}/healthkit/daily_vitals/"
    try:
        df = read_bronze_csv(path)
    except Exception as e:
        print(f"No daily vitals data found ({e}), skipping")
        return

    if df.rdd.isEmpty():
        print("No daily vitals data found, skipping")
        return

    validate_schema(df, ["date", "resting_heart_rate_bpm"], "healthkit_daily_vitals")

    # Cast numeric columns
    for col_name in ["resting_heart_rate_bpm", "hrv_ms", "vo2_max",
                     "blood_oxygen_pct", "respiratory_rate"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

    # Forward-fill VO2 Max (sparse — Apple Watch only records occasionally)
    if "vo2_max" in df.columns:
        df = forward_fill(df, partition_col=None, order_col="date", fill_cols=["vo2_max"])

    # Partitioning columns
    df = (
        df.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/healthkit_daily_vitals/"
    )
    print(f"Wrote {df.count()} daily vitals records to Silver")


def process_workouts():
    """Normalize HealthKit workouts data."""
    print("Processing HealthKit workouts...")

    path = f"s3://{BRONZE_BUCKET}/healthkit/workouts/"
    try:
        df = read_bronze_csv(path)
    except Exception as e:
        print(f"No workouts data found ({e}), skipping")
        return

    if df.rdd.isEmpty():
        print("No workouts data found, skipping")
        return

    validate_schema(df, ["date", "workout_type", "duration_minutes"], "healthkit_workouts")

    # Cast numeric columns
    for col_name in ["duration_minutes", "distance_mi"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
    for col_name in ["calories_burned", "avg_heart_rate"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("integer"))

    # Belt-and-suspenders: filter out any Peloton workouts that slipped through
    if "source_app" in df.columns:
        df = df.filter(~F.lower(F.col("source_app")).contains("peloton"))

    # Apply workout category mapping
    mapping_expr = F.create_map(
        *[item for pair in HEALTHKIT_WORKOUT_CATEGORY_MAP.items()
          for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    df = df.withColumn(
        "workout_category",
        F.coalesce(
            mapping_expr[F.lower(F.col("workout_type"))],
            F.lit("other"),
        ),
    )

    # Partitioning columns
    df = (
        df.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/healthkit_workouts/"
    )
    print(f"Wrote {df.count()} workout records to Silver")


def process_body():
    """Normalize HealthKit body measurement data."""
    print("Processing HealthKit body measurements...")

    path = f"s3://{BRONZE_BUCKET}/healthkit/body/"
    try:
        df = read_bronze_csv(path)
    except Exception as e:
        print(f"No body data found ({e}), skipping")
        return

    if df.rdd.isEmpty():
        print("No body data found, skipping")
        return

    validate_schema(df, ["date", "weight_lbs"], "healthkit_body")

    # Cast numeric columns
    for col_name in ["weight_lbs", "body_fat_pct", "bmi", "lean_body_mass_lbs", "bmr", "height_in"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

    # Forward-fill weight (sporadic measurements)
    fill_cols = ["weight_lbs"]
    # Also forward-fill body composition metrics for days when only weight is recorded
    if "body_fat_pct" in df.columns:
        fill_cols.append("body_fat_pct")
    if "lean_body_mass_lbs" in df.columns:
        fill_cols.append("lean_body_mass_lbs")
    if "height_in" in df.columns:
        fill_cols.append("height_in")  # Height rarely changes
    
    df = forward_fill(df, partition_col=None, order_col="date", fill_cols=fill_cols)

    # Partitioning columns
    df = (
        df.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/healthkit_body/"
    )
    print(f"Wrote {df.count()} body records to Silver")


def process_mindfulness():
    """Normalize HealthKit mindfulness session data."""
    print("Processing HealthKit mindfulness sessions...")

    path = f"s3://{BRONZE_BUCKET}/healthkit/mindfulness/"
    try:
        df = read_bronze_csv(path)
    except Exception as e:
        print(f"No mindfulness data found ({e}), skipping")
        return

    if df.rdd.isEmpty():
        print("No mindfulness data found, skipping")
        return

    validate_schema(df, ["date", "duration_minutes"], "healthkit_mindfulness")

    # Cast numeric columns
    df = df.withColumn("duration_minutes", F.col("duration_minutes").cast("double"))
    if "session_count" in df.columns:
        df = df.withColumn("session_count", F.col("session_count").cast("integer"))

    # Partitioning columns
    df = (
        df.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/healthkit_mindfulness/"
    )
    print(f"Wrote {df.count()} mindfulness records to Silver")


# Run all processors
process_daily_vitals()
process_workouts()
process_body()
process_mindfulness()

job.commit()
print("HealthKit normalizer job complete")
