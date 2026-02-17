"""
Bio Lakehouse - Oura Ring Data Normalizer (Glue ETL Job)

Reads Bronze-layer Oura CSVs (readiness, sleep, activity), normalizes them,
and writes partitioned Parquet to the Silver layer.

Glue job arguments:
  --source_bucket: Bronze S3 bucket name
  --source_key: S3 key that triggered this job (optional, for incremental)
  --source_type: oura/readiness, oura/sleep, etc.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from bio_etl_utils import forward_fill, validate_schema

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

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


def process_readiness():
    """Normalize Oura daily readiness data."""
    print("Processing Oura readiness data...")

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(
        f"s3://{BRONZE_BUCKET}/oura/readiness/"
    )

    if df.rdd.isEmpty():
        print("No readiness data found, skipping")
        return

    validate_schema(df, ["id", "day", "score", "timestamp"], "oura_readiness")

    # Cast score to integer, handle nulls
    df = df.withColumn("score", F.col("score").cast("integer"))

    # Forward-fill missing readiness scores (Oura sometimes has gaps)
    df = forward_fill(df, partition_col=None, order_col="day", fill_cols=["score"])

    # Parse day column for partitioning
    df = (
        df.withColumn("year", F.substring("day", 1, 4))
        .withColumn("month", F.substring("day", 6, 2))
        .withColumn("day_of_month", F.substring("day", 9, 2))
    )

    # Write partitioned Parquet to Silver
    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/oura_daily_readiness/"
    )
    print(f"Wrote {df.count()} readiness records to Silver")


def process_sleep():
    """Normalize Oura daily sleep data."""
    print("Processing Oura sleep data...")

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(
        f"s3://{BRONZE_BUCKET}/oura/sleep/"
    )

    if df.rdd.isEmpty():
        print("No sleep data found, skipping")
        return

    validate_schema(df, ["id", "day", "score", "timestamp"], "oura_sleep")

    df = df.withColumn("score", F.col("score").cast("integer"))

    # Parse partitioning columns
    df = (
        df.withColumn("year", F.substring("day", 1, 4))
        .withColumn("month", F.substring("day", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/oura_daily_sleep/"
    )
    print(f"Wrote {df.count()} sleep records to Silver")


def process_activity():
    """Normalize Oura daily activity data."""
    print("Processing Oura activity data...")

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(
        f"s3://{BRONZE_BUCKET}/oura/activity/"
    )

    if df.rdd.isEmpty():
        print("No activity data found, skipping")
        return

    validate_schema(df, ["id", "day", "score", "active_calories", "steps"], "oura_activity")

    # Cast numeric columns
    for col_name in ["score", "active_calories", "steps", "high_activity_time",
                     "medium_activity_time", "low_activity_time", "sedentary_time",
                     "total_calories"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("integer"))

    for col_name in ["met_avg", "met_max"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

    # Parse partitioning columns
    df = (
        df.withColumn("year", F.substring("day", 1, 4))
        .withColumn("month", F.substring("day", 6, 2))
    )

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/oura_daily_activity/"
    )
    print(f"Wrote {df.count()} activity records to Silver")


# Run all processors
process_readiness()
process_sleep()
process_activity()

job.commit()
print("Oura normalizer job complete")
