"""
Bio Lakehouse - Peloton Workout Data Normalizer (Glue ETL Job)

Reads Bronze-layer Peloton CSVs, normalizes timestamps and types,
calculates derived metrics, and writes partitioned Parquet to Silver.

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
    calculate_hr_zones,
    calculate_output_per_minute,
    categorize_workout_type,
    validate_schema,
)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

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

print("Processing Peloton workout data...")

# Read Peloton workout CSVs from Bronze.
# The Bronze bucket may contain both raw full-export CSVs (top-level, title-case
# headers like "Workout Timestamp") and old Hive-partitioned CSVs (year=/month=
# paths with pre-processed snake_case headers). These have incompatible schemas,
# so we read only the top-level raw CSVs (KnownasNoma_workouts_*.csv) which are
# complete exports containing all historical workouts.
# List top-level Peloton CSVs and read only the latest (full export = superset)
import boto3 as _boto3
_s3 = _boto3.client("s3")
_resp = _s3.list_objects_v2(
    Bucket=BRONZE_BUCKET, Prefix="peloton/workouts/KnownasNoma_"
)
_csv_keys = sorted(
    [obj["Key"] for obj in _resp.get("Contents", []) if obj["Key"].endswith(".csv")],
    key=lambda k: _resp["Contents"][[o["Key"] for o in _resp["Contents"]].index(k)]["LastModified"],
)
_latest_key = _csv_keys[-1] if _csv_keys else None
print(f"Reading latest Peloton CSV: {_latest_key}")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"s3://{BRONZE_BUCKET}/{_latest_key}")
)

if df.rdd.isEmpty():
    print("No Peloton data found, exiting")
    job.commit()
    sys.exit(0)

# Normalize column names: "Workout Timestamp" → "workout_timestamp"
# Raw Peloton CSVs use title case; previously processed ones are already snake_case
import re
# SYNC: This normalization regex must match in both:
#   - lambda/ingestion_trigger/handler.py:validate_csv_headers()
#   - glue/peloton_normalizer.py (column normalization block)
for col in df.columns:
    snake = re.sub(r"[.\s/()]+", "_", col.strip()).lower().strip("_")
    if snake != col:
        df = df.withColumnRenamed(col, snake)
print(f"Columns after normalization: {df.columns}")

validate_schema(
    df,
    ["workout_timestamp", "fitness_discipline", "calories_burned"],
    "peloton_workouts",
)

# Parse workout_timestamp into workout_date and workout_time.
# Raw format: "2026-02-21 07:25 (-05)" or "2026-02-21 07:25 (EST)"
# Use regex extraction to handle variable whitespace and optional seconds.
df = df.withColumn(
    "workout_date",
    F.to_timestamp(
        F.regexp_extract("workout_timestamp", r"^(\d{4}-\d{2}-\d{2})", 1),
        "yyyy-MM-dd",
    ),
)
df = df.withColumn(
    "workout_time",
    F.regexp_extract("workout_timestamp", r"\d{4}-\d{2}-\d{2}\s+(\d{2}:\d{2}(?::\d{2})?)", 1),
)
print(f"Sample workout_date: {df.select('workout_date').first()[0]}")

# Cast numeric columns
int_cols = ["total_output", "avg_watts", "avg_cadence_rpm", "calories_burned",
            "avg_heartrate", "length_minutes"]
double_cols = ["avg_speed_mph", "distance_mi"]

for col_name in int_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("integer"))

for col_name in double_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))

# Parse resistance percentage string (e.g., "42%") to numeric
if "avg_resistance" in df.columns:
    df = df.withColumn(
        "avg_resistance_pct",
        F.regexp_extract(F.col("avg_resistance"), r"(\d+)", 1).cast("integer"),
    )

# Build UTC timestamp from workout_date + workout_time
# Use date_format on the timestamp workout_date to get "yyyy-MM-dd" string,
# then concatenate with workout_time for proper parsing
df = df.withColumn(
    "workout_timestamp_utc",
    F.when(
        F.col("workout_time").isNotNull() & (F.col("workout_time") != ""),
        F.to_timestamp(
            F.concat_ws(" ", F.date_format("workout_date", "yyyy-MM-dd"), F.col("workout_time")),
            "yyyy-MM-dd HH:mm",
        ),
    ).otherwise(F.col("workout_date")),
)

# Categorize workout types
df = categorize_workout_type(df)

# Calculate derived metrics
df = calculate_output_per_minute(df)
df = calculate_hr_zones(df)

# Calculate total output in kJ (Peloton output is in kJ already)
df = df.withColumn("total_output_kj", F.col("total_output").cast("double"))

# Partitioning columns (extract from timestamp)
df = (
    df.withColumn("year", F.date_format("workout_date", "yyyy"))
    .withColumn("month", F.date_format("workout_date", "MM"))
)

# Write to Silver
df.write.mode("overwrite").partitionBy("year", "month").parquet(
    f"s3://{SILVER_BUCKET}/peloton_workouts/"
)

print(f"Wrote {df.count()} Peloton workout records to Silver")

job.commit()
print("Peloton normalizer job complete")
