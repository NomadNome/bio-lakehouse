"""
Bio Lakehouse - Readiness-Performance Aggregator (Glue ETL Job)

Joins Silver Oura readiness data with Silver Peloton workout data on date.
Calculates readiness_to_output_ratio and daily aggregated metrics.
Writes Gold-layer Parquet for dashboard consumption.

Glue job arguments:
  --silver_bucket: Silver S3 bucket name
  --gold_bucket: Gold S3 bucket name
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "silver_bucket", "gold_bucket"],
)
job.init(args["JOB_NAME"], args)

SILVER_BUCKET = args["silver_bucket"]
GOLD_BUCKET = args["gold_bucket"]

print("Running readiness-performance aggregator...")

# -------------------------------------------------------
# Read Silver data
# -------------------------------------------------------

# Oura readiness
readiness_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/oura_daily_readiness/")
readiness_daily = readiness_df.select(
    F.col("day").alias("date"),
    F.col("score").alias("readiness_score"),
    F.col("contributors_hrv_balance"),
    F.col("contributors_resting_heart_rate").alias("resting_hr_score"),
    F.col("contributors_previous_night").alias("previous_night_score"),
    F.col("contributors_recovery_index").alias("recovery_index_score"),
    F.col("temperature_deviation"),
)

# Oura sleep
sleep_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/oura_daily_sleep/")
sleep_daily = sleep_df.select(
    F.col("day").alias("date"),
    F.col("score").alias("sleep_score"),
    F.col("contributors_deep_sleep").alias("deep_sleep_score"),
    F.col("contributors_rem_sleep").alias("rem_sleep_score"),
    F.col("contributors_efficiency").alias("sleep_efficiency_score"),
    F.col("contributors_total_sleep").alias("total_sleep_score"),
)

# Oura activity
activity_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/oura_daily_activity/")
activity_daily = activity_df.select(
    F.col("day").alias("date"),
    F.col("score").alias("activity_score"),
    F.col("active_calories"),
    F.col("steps"),
    F.col("total_calories"),
)

# Peloton workouts — aggregate to daily
peloton_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/peloton_workouts/")
peloton_daily = peloton_df.groupBy("workout_date").agg(
    F.count("*").alias("workout_count"),
    F.sum("total_output_kj").alias("total_output_kj"),
    F.sum("calories_burned").alias("peloton_calories"),
    F.avg("avg_watts").alias("avg_watts"),
    F.max("avg_heartrate").alias("max_avg_hr"),
    F.avg("output_per_minute").alias("avg_output_per_minute"),
    F.collect_set("workout_category").alias("workout_categories"),
    F.sum("length_minutes").alias("total_workout_minutes"),
    F.collect_set("fitness_discipline").alias("disciplines"),
).withColumnRenamed("workout_date", "date")

# Flatten array columns to comma-separated strings
peloton_daily = peloton_daily.withColumn(
    "workout_categories", F.array_join(F.col("workout_categories"), ",")
).withColumn("disciplines", F.array_join(F.col("disciplines"), ","))

# -------------------------------------------------------
# Join all datasets on date
# -------------------------------------------------------

gold_df = (
    readiness_daily
    .join(sleep_daily, on="date", how="full_outer")
    .join(activity_daily, on="date", how="full_outer")
    .join(peloton_daily, on="date", how="full_outer")
)

# -------------------------------------------------------
# Calculate derived metrics
# -------------------------------------------------------

# Readiness-to-output ratio: how much output per point of readiness
gold_df = gold_df.withColumn(
    "readiness_to_output_ratio",
    F.when(
        (F.col("readiness_score").isNotNull())
        & (F.col("readiness_score") > 0)
        & (F.col("total_output_kj").isNotNull()),
        F.round(F.col("total_output_kj") / F.col("readiness_score"), 2),
    ).otherwise(None),
)

# Combined wellness score (weighted average of readiness + sleep)
gold_df = gold_df.withColumn(
    "combined_wellness_score",
    F.when(
        F.col("readiness_score").isNotNull() & F.col("sleep_score").isNotNull(),
        F.round((F.col("readiness_score") * 0.6 + F.col("sleep_score") * 0.4), 1),
    ).otherwise(F.coalesce(F.col("readiness_score"), F.col("sleep_score"))),
)

# Flag: had workout that day
gold_df = gold_df.withColumn(
    "had_workout",
    F.when(F.col("workout_count").isNotNull() & (F.col("workout_count") > 0), True)
    .otherwise(False),
)

# Partitioning columns
gold_df = (
    gold_df.withColumn("year", F.substring("date", 1, 4))
    .withColumn("month", F.substring("date", 6, 2))
)

# -------------------------------------------------------
# Write to Gold
# -------------------------------------------------------

gold_df.write.mode("overwrite").partitionBy("year", "month").parquet(
    f"s3://{GOLD_BUCKET}/daily_readiness_performance/"
)

print(f"Wrote {gold_df.count()} daily records to Gold layer")

job.commit()
print("Readiness aggregator job complete")
