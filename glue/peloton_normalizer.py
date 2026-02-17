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

# Read all Peloton workout CSVs from Bronze
df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    f"s3://{BRONZE_BUCKET}/peloton/workouts/"
)

if df.rdd.isEmpty():
    print("No Peloton data found, exiting")
    job.commit()
    sys.exit(0)

validate_schema(
    df,
    ["workout_date", "fitness_discipline", "calories_burned"],
    "peloton_workouts",
)

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

# Build UTC timestamp from workout_date + workout_time + utc_offset
df = df.withColumn(
    "workout_timestamp_utc",
    F.when(
        F.col("workout_time").isNotNull() & (F.col("workout_time") != ""),
        F.to_timestamp(
            F.concat_ws(" ", F.col("workout_date"), F.col("workout_time")),
            "yyyy-MM-dd HH:mm",
        ),
    ).otherwise(F.to_date(F.col("workout_date"), "yyyy-MM-dd").cast("timestamp")),
)

# Categorize workout types
df = categorize_workout_type(df)

# Calculate derived metrics
df = calculate_output_per_minute(df)
df = calculate_hr_zones(df)

# Calculate total output in kJ (Peloton output is in kJ already)
df = df.withColumn("total_output_kj", F.col("total_output").cast("double"))

# Partitioning columns
df = (
    df.withColumn("year", F.substring("workout_date", 1, 4))
    .withColumn("month", F.substring("workout_date", 6, 2))
)

# Write to Silver
df.write.mode("overwrite").partitionBy("year", "month").parquet(
    f"s3://{SILVER_BUCKET}/peloton_workouts/"
)

print(f"Wrote {df.count()} Peloton workout records to Silver")

job.commit()
print("Peloton normalizer job complete")
