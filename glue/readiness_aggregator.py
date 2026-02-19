"""
Bio Lakehouse - Readiness-Performance Aggregator (Glue ETL Job)

Joins Silver Oura readiness, sleep, and activity data with Silver Peloton
workout data and Silver HealthKit data on date.
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
# Read Silver data
# -------------------------------------------------------

# Oura readiness
readiness_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/oura_daily_readiness/")
readiness_daily = readiness_df.select(
    F.col("day").alias("date"),
    F.col("score").alias("readiness_score"),
    F.col("contributors_hrv_balance").alias("hrv_balance_score"),
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
# Read HealthKit Silver data (may not exist on first run)
# -------------------------------------------------------

# HealthKit daily vitals
hk_vitals_df = read_silver_safe(f"s3://{SILVER_BUCKET}/healthkit_daily_vitals/")
if hk_vitals_df is not None:
    hk_vitals_daily = hk_vitals_df.select(
        F.col("date"),
        F.col("resting_heart_rate_bpm"),
        F.col("hrv_ms"),
        F.col("vo2_max"),
        F.col("blood_oxygen_pct"),
        F.col("respiratory_rate"),
    )
else:
    hk_vitals_daily = None
    print("No HealthKit daily vitals data available yet")

# HealthKit workouts — aggregate to daily
hk_workouts_df = read_silver_safe(f"s3://{SILVER_BUCKET}/healthkit_workouts/")
if hk_workouts_df is not None:
    hk_workouts_daily = hk_workouts_df.groupBy("date").agg(
        F.count("*").alias("hk_workout_count"),
        F.sum("calories_burned").alias("hk_calories"),
        F.sum("duration_minutes").alias("hk_workout_minutes"),
        F.collect_set("workout_category").alias("hk_workout_categories"),
        F.collect_set("workout_type").alias("hk_workout_types"),
    )
    hk_workouts_daily = hk_workouts_daily.withColumn(
        "hk_workout_categories", F.array_join(F.col("hk_workout_categories"), ",")
    ).withColumn("hk_workout_types", F.array_join(F.col("hk_workout_types"), ","))
else:
    hk_workouts_daily = None
    print("No HealthKit workouts data available yet")

# HealthKit body measurements
hk_body_df = read_silver_safe(f"s3://{SILVER_BUCKET}/healthkit_body/")
if hk_body_df is not None:
    hk_body_daily = hk_body_df.select(
        F.col("date"),
        F.col("weight_lbs"),
        F.col("body_fat_pct"),
        F.col("bmi"),
        F.col("lean_body_mass_lbs"),
    )
else:
    hk_body_daily = None
    print("No HealthKit body data available yet")

# HealthKit mindfulness — already daily aggregated
hk_mindfulness_df = read_silver_safe(f"s3://{SILVER_BUCKET}/healthkit_mindfulness/")
if hk_mindfulness_df is not None:
    hk_mindfulness_daily = hk_mindfulness_df.select(
        F.col("date"),
        F.col("duration_minutes").alias("mindfulness_minutes"),
        F.col("session_count").alias("mindfulness_session_count"),
    )
else:
    hk_mindfulness_daily = None
    print("No HealthKit mindfulness data available yet")

# -------------------------------------------------------
# Join all datasets on date
# -------------------------------------------------------

gold_df = (
    readiness_daily
    .join(sleep_daily, on="date", how="full_outer")
    .join(activity_daily, on="date", how="full_outer")
    .join(peloton_daily, on="date", how="full_outer")
)

# Join HealthKit datasets (only if they exist)
if hk_vitals_daily is not None:
    gold_df = gold_df.join(hk_vitals_daily, on="date", how="full_outer")
if hk_workouts_daily is not None:
    gold_df = gold_df.join(hk_workouts_daily, on="date", how="full_outer")
if hk_body_daily is not None:
    gold_df = gold_df.join(hk_body_daily, on="date", how="full_outer")
if hk_mindfulness_daily is not None:
    gold_df = gold_df.join(hk_mindfulness_daily, on="date", how="full_outer")

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

# Total workout count: Peloton + HealthKit
peloton_count = F.coalesce(F.col("workout_count"), F.lit(0))
hk_count = (
    F.coalesce(F.col("hk_workout_count"), F.lit(0))
    if hk_workouts_daily is not None else F.lit(0)
)
gold_df = gold_df.withColumn("total_workout_count", peloton_count + hk_count)

# Total calories from all workout sources
peloton_cal = F.coalesce(F.col("peloton_calories"), F.lit(0))
hk_cal = (
    F.coalesce(F.col("hk_calories"), F.lit(0))
    if hk_workouts_daily is not None else F.lit(0)
)
gold_df = gold_df.withColumn("total_calories_all_sources", peloton_cal + hk_cal)

# Total workout minutes from all sources
peloton_min = F.coalesce(F.col("total_workout_minutes"), F.lit(0))
hk_min = (
    F.coalesce(F.col("hk_workout_minutes"), F.lit(0))
    if hk_workouts_daily is not None else F.lit(0)
)
gold_df = gold_df.withColumn("total_workout_minutes_all", peloton_min + hk_min)

# Flag: had workout that day (Peloton OR HealthKit)
gold_df = gold_df.withColumn(
    "had_workout",
    F.when(F.col("total_workout_count") > 0, True).otherwise(False),
)

# Mindfulness-adjusted wellness score: small bonus for mindfulness practice
if hk_mindfulness_daily is not None:
    gold_df = gold_df.withColumn(
        "mindfulness_adjusted_wellness",
        F.when(
            F.col("combined_wellness_score").isNotNull()
            & F.col("mindfulness_minutes").isNotNull()
            & (F.col("mindfulness_minutes") > 0),
            F.least(
                F.round(F.col("combined_wellness_score") + F.least(F.col("mindfulness_minutes") / 10.0, F.lit(3.0)), 1),
                F.lit(100.0),
            ),
        ).otherwise(F.col("combined_wellness_score")),
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
