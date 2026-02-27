"""
Bio Lakehouse - Oura Ring Data Normalizer (Glue ETL Job)

Reads Bronze-layer Oura data (CSV and JSON) for readiness, sleep, and activity,
normalizes them, and writes partitioned Parquet to the Silver layer.

Glue job arguments:
  --bronze_bucket: Bronze S3 bucket name
  --silver_bucket: Silver S3 bucket name
"""

import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from bio_etl_utils import forward_fill, validate_schema

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Disable automatic partition inference — our Bronze paths use year=/month=/day=
# which Spark interprets as Hive partitions, shadowing the CSV 'day' column
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

# Column lists per data type (must match csv_transformer.py output)
READINESS_COLUMNS = [
    "id", "day", "score", "temperature_deviation",
    "temperature_trend_deviation", "timestamp",
    "contributors_activity_balance", "contributors_body_temperature",
    "contributors_hrv_balance", "contributors_previous_day_activity",
    "contributors_previous_night", "contributors_recovery_index",
    "contributors_resting_heart_rate", "contributors_sleep_balance",
    "contributors_sleep_regularity",
]

SLEEP_COLUMNS = [
    "id", "day", "score", "timestamp",
    "contributors_deep_sleep", "contributors_efficiency",
    "contributors_latency", "contributors_rem_sleep",
    "contributors_restfulness", "contributors_timing",
    "contributors_total_sleep",
]

ACTIVITY_COLUMNS = [
    "id", "day", "score", "timestamp",
    "active_calories", "steps",
    "high_activity_time", "medium_activity_time",
    "low_activity_time", "sedentary_time", "total_calories",
    "met_interval", "met_avg", "met_max", "met_count",
]

DATA_TYPE_COLUMNS = {
    "readiness": READINESS_COLUMNS,
    "sleep": SLEEP_COLUMNS,
    "activity": ACTIVITY_COLUMNS,
}


def _detect_csv_delimiter(s3_path):
    """Sample first line of first CSV under path; return ';' or ','."""
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    s3_client = boto3.client("s3")
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20)
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            head = s3_client.get_object(Bucket=bucket, Key=obj["Key"], Range="bytes=0-1024")
            first_line = head["Body"].read().decode("utf-8").split("\n")[0]
            return ";" if first_line.count(";") > first_line.count(",") else ","
    return ","


def read_bronze_csv(path):
    """Read CSV files, grouping by header signature to handle mixed column orders.

    Bronze CSVs have two column orders: bulk-uploaded (alphabetical) and
    Lambda-produced (id,day,score,...). Spark CSV reader maps columns by
    position, not name, so reading mixed-order files together misaligns
    values. We group files by header, read each group in one Spark pass,
    and unionByName to merge correctly.
    """
    delimiter = _detect_csv_delimiter(path)
    print(f"Detected CSV delimiter for {path}: {delimiter!r}")

    parts = path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    # Group CSV files by their header line
    header_groups = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv") or obj["Size"] == 0:
                continue
            head = s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-1024")
            header_line = head["Body"].read().decode("utf-8").split("\n")[0].strip()
            header_groups.setdefault(header_line, []).append(f"s3://{bucket}/{key}")

    if not header_groups:
        return spark.createDataFrame([], StructType([]))

    print(f"Found {len(header_groups)} distinct CSV header layouts, "
          f"{sum(len(v) for v in header_groups.values())} files total")

    # Read each group (same column order) in one Spark pass, then union by name
    dfs = []
    for file_paths in header_groups.values():
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("sep", delimiter)
            .csv(file_paths)
        )
        dfs.append(df)

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)
    return result


def _flatten_json_record(record, data_type):
    """Flatten a single JSON record to match CSV column schema."""
    flat = {}
    target_cols = DATA_TYPE_COLUMNS[data_type]

    # Copy top-level scalar fields
    for col in target_cols:
        if col in record:
            val = record[col]
            flat[col] = str(val) if val is not None else ""

    # Flatten contributors struct
    contributors = record.get("contributors") or {}
    for key, value in contributors.items():
        col_name = f"contributors_{key}"
        if col_name in target_cols:
            flat[col_name] = str(value) if value is not None else ""

    # Compute MET stats for activity
    if data_type == "activity":
        met = record.get("met") or {}
        items = met.get("items") if isinstance(met, dict) else None
        if items and len(items) > 0:
            flat["met_interval"] = str(met.get("interval", ""))
            flat["met_avg"] = str(round(sum(items) / len(items), 2))
            flat["met_max"] = str(max(items))
            flat["met_count"] = str(len(items))
        else:
            for k in ("met_interval", "met_avg", "met_max", "met_count"):
                flat[k] = ""

    # Fill missing columns with empty string
    return {col: flat.get(col, "") for col in target_cols}


def read_bronze_json(path, data_type):
    """Read JSON files from Bronze using boto3, flatten to match CSV schema."""
    # Parse s3://bucket/prefix/ into bucket and prefix
    parts = path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    s3 = boto3.client("s3")
    all_rows = []

    # List all .json files under the prefix
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))
                records = data if isinstance(data, list) else [data]
                for rec in records:
                    all_rows.append(_flatten_json_record(rec, data_type))
            except Exception as e:
                print(f"Warning: failed to read {key}: {e}")
                continue

    if not all_rows:
        return spark.createDataFrame([], StructType([]))

    target_cols = DATA_TYPE_COLUMNS[data_type]
    schema = StructType([StructField(c, StringType(), True) for c in target_cols])
    # Convert dicts to tuples in schema order to avoid PySpark alphabetical key sorting
    tuples = [tuple(row[c] for c in target_cols) for row in all_rows]
    return spark.createDataFrame(tuples, schema=schema)


def read_bronze(path, data_type):
    """Read both CSV and JSON from Bronze, union and deduplicate on id."""
    csv_df = None
    json_df = None

    try:
        csv_df = read_bronze_csv(path)
        if csv_df.rdd.isEmpty():
            csv_df = None
    except Exception as e:
        print(f"CSV read failed for {path}: {e}")
        csv_df = None

    try:
        json_df = read_bronze_json(path, data_type)
        if json_df.rdd.isEmpty():
            json_df = None
    except Exception:
        json_df = None

    # Filter CSV rows with invalid day values (caused by test files or corrupt records)
    if csv_df is not None and "day" in csv_df.columns:
        csv_df = csv_df.filter(F.col("day").rlike("^\\d{4}-\\d{2}-\\d{2}"))
        if csv_df.rdd.isEmpty():
            csv_df = None

    if csv_df is None and json_df is None:
        return spark.createDataFrame([], StructType([]))

    if csv_df is not None and json_df is not None:
        # Align CSV columns to match JSON selection
        target_cols = DATA_TYPE_COLUMNS[data_type]
        for col_name in target_cols:
            if col_name not in csv_df.columns:
                csv_df = csv_df.withColumn(col_name, F.lit(None).cast("string"))
        csv_df = csv_df.select(target_cols)
        combined = csv_df.unionByName(json_df)
    elif csv_df is not None:
        combined = csv_df
    else:
        combined = json_df

    # Deduplicate — prefer first occurrence (CSV over JSON due to union order)
    combined = combined.dropDuplicates(["id"])
    return combined


def process_readiness():
    """Normalize Oura daily readiness data."""
    print("Processing Oura readiness data...")

    df = read_bronze(f"s3://{BRONZE_BUCKET}/oura/readiness/", "readiness")

    if df.rdd.isEmpty():
        print("No readiness data found, skipping")
        return

    validate_schema(df, ["id", "day", "score", "timestamp"], "oura_readiness")

    # Cast score to integer, handle nulls
    df = df.withColumn("score", F.col("score").cast("integer"))

    # Forward-fill missing readiness scores (Oura sometimes has gaps)
    df = forward_fill(df, partition_col=None, order_col="day", fill_cols=["score"])

    # Extract partitioning columns from the CSV 'day' column (string: "2025-11-25")
    df = (
        df.withColumn("year", F.substring("day", 1, 4))
        .withColumn("month", F.substring("day", 6, 2))
    )

    # Write partitioned Parquet to Silver
    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/oura_daily_readiness/"
    )
    print(f"Wrote {df.count()} readiness records to Silver")


def process_sleep():
    """Normalize Oura daily sleep data."""
    print("Processing Oura sleep data...")

    df = read_bronze(f"s3://{BRONZE_BUCKET}/oura/sleep/", "sleep")

    if df.rdd.isEmpty():
        print("No sleep data found, skipping")
        return

    validate_schema(df, ["id", "day", "score", "timestamp"], "oura_sleep")

    df = df.withColumn("score", F.col("score").cast("integer"))

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

    df = read_bronze(f"s3://{BRONZE_BUCKET}/oura/activity/", "activity")

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
