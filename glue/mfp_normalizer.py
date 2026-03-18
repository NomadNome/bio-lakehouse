"""
Bio Lakehouse - MyFitnessPal Nutrition Data Normalizer (Glue ETL Job)

Reads Bronze-layer MFP Nutrition Summary CSVs (meal-level rows),
aggregates to daily totals, computes macro ratios, and writes
partitioned Parquet to the Silver layer.

Glue job arguments:
  --bronze_bucket: Bronze S3 bucket name
  --silver_bucket: Silver S3 bucket name
"""

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Disable automatic partition inference
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

# After snake_case normalization, MFP headers like "Saturated Fat" become
# "saturated_fat" (no unit suffix). Rename to add units for Silver consistency.
COLUMN_RENAMES = {
    "saturated_fat": "saturated_fat_g",
    "polyunsaturated_fat": "polyunsaturated_fat_g",
    "monounsaturated_fat": "monounsaturated_fat_g",
    "trans_fat": "trans_fat_g",
    "cholesterol": "cholesterol_mg",
    "potassium": "potassium_mg",
    "fiber": "fiber_g",
    "sugar": "sugar_g",
}

# Numeric columns to aggregate via SUM (using final/renamed names)
NUMERIC_COLS = [
    "calories", "fat_g", "saturated_fat_g", "polyunsaturated_fat_g",
    "monounsaturated_fat_g", "trans_fat_g", "cholesterol_mg", "sodium_mg",
    "potassium_mg", "carbohydrates_g", "fiber_g", "sugar_g", "protein_g",
]


def normalize_column_name(name):
    """Normalize column name to snake_case (same regex as ingestion trigger)."""
    return re.sub(r"[.\s/()]+", "_", name.strip()).lower().strip("_")


def process_nutrition():
    """Read MFP CSVs, aggregate meals to daily totals, compute macro ratios."""
    print("Processing MFP nutrition data...")

    path = f"s3://{BRONZE_BUCKET}/mfp/nutrition/"
    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("recursiveFileLookup", "true")
            .csv(path)
        )
    except Exception as e:
        print(f"No MFP nutrition data found ({e}), skipping")
        return

    if df.rdd.isEmpty():
        print("No MFP nutrition data found, skipping")
        return

    # Normalize column names to snake_case
    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Rename columns to add unit suffixes for Silver consistency
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    print(f"Columns after normalization + rename: {df.columns}")

    # Cast numeric columns to double
    for col_name in NUMERIC_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Deduplicate overlapping CSV exports — same (date, meal) can appear in
    # multiple Bronze files when MFP exports cover overlapping date ranges.
    dedup_cols = ["date", "meal"] if "meal" in df.columns else ["date"]
    before_count = df.count()
    df = df.dropDuplicates(dedup_cols)
    after_count = df.count()
    if before_count != after_count:
        print(f"Deduplicated: {before_count} → {after_count} rows (removed {before_count - after_count} duplicates)")

    # Aggregate meal-level rows to daily totals
    agg_exprs = []
    for col_name in NUMERIC_COLS:
        if col_name in df.columns:
            agg_exprs.append(F.sum(col_name).alias(col_name))

    agg_exprs.append(F.count("*").alias("meal_count"))

    if "meal" in df.columns:
        agg_exprs.append(F.collect_set("meal").alias("meals_logged"))

    daily = df.groupBy("date").agg(*agg_exprs)

    # Compute derived macro ratios (% of calories from each macro)
    # Protein: 4 cal/g, Carbs: 4 cal/g, Fat: 9 cal/g
    if "protein_g" in daily.columns and "calories" in daily.columns:
        daily = daily.withColumn(
            "protein_pct",
            F.when(
                (F.col("calories").isNotNull()) & (F.col("calories") > 0),
                F.round(F.col("protein_g") * 4.0 / F.col("calories") * 100, 1),
            ),
        )

    if "carbohydrates_g" in daily.columns and "calories" in daily.columns:
        daily = daily.withColumn(
            "carb_pct",
            F.when(
                (F.col("calories").isNotNull()) & (F.col("calories") > 0),
                F.round(F.col("carbohydrates_g") * 4.0 / F.col("calories") * 100, 1),
            ),
        )

    if "fat_g" in daily.columns and "calories" in daily.columns:
        daily = daily.withColumn(
            "fat_pct",
            F.when(
                (F.col("calories").isNotNull()) & (F.col("calories") > 0),
                F.round(F.col("fat_g") * 9.0 / F.col("calories") * 100, 1),
            ),
        )

    # Convert meals_logged array to comma-separated string for Parquet compatibility
    if "meals_logged" in daily.columns:
        daily = daily.withColumn(
            "meals_logged", F.concat_ws(",", F.col("meals_logged"))
        )

    # Extract partition columns from date
    daily = (
        daily.withColumn("year", F.substring("date", 1, 4))
        .withColumn("month", F.substring("date", 6, 2))
    )

    daily.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{SILVER_BUCKET}/mfp_daily_nutrition/"
    )
    print(f"Wrote {daily.count()} daily nutrition records to Silver")


# Run processor
process_nutrition()

job.commit()
print("MFP normalizer job complete")
