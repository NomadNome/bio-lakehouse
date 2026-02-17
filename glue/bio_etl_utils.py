"""
Bio Lakehouse - Shared ETL Utilities for Glue Jobs

Common functions used across Oura and Peloton normalizer jobs.
"""

from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# -------------------------------------------------------
# Schema Definitions
# -------------------------------------------------------

OURA_READINESS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("day", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("temperature_deviation", DoubleType(), True),
        StructField("temperature_trend_deviation", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("contributors_activity_balance", IntegerType(), True),
        StructField("contributors_body_temperature", IntegerType(), True),
        StructField("contributors_hrv_balance", IntegerType(), True),
        StructField("contributors_previous_day_activity", IntegerType(), True),
        StructField("contributors_previous_night", IntegerType(), True),
        StructField("contributors_recovery_index", IntegerType(), True),
        StructField("contributors_resting_heart_rate", IntegerType(), True),
        StructField("contributors_sleep_balance", IntegerType(), True),
        StructField("contributors_sleep_regularity", IntegerType(), True),
    ]
)

OURA_SLEEP_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("day", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("contributors_deep_sleep", IntegerType(), True),
        StructField("contributors_efficiency", IntegerType(), True),
        StructField("contributors_latency", IntegerType(), True),
        StructField("contributors_rem_sleep", IntegerType(), True),
        StructField("contributors_restfulness", IntegerType(), True),
        StructField("contributors_timing", IntegerType(), True),
        StructField("contributors_total_sleep", IntegerType(), True),
    ]
)

PELOTON_SCHEMA = StructType(
    [
        StructField("workout_timestamp", StringType(), True),
        StructField("live_on_demand", StringType(), True),
        StructField("instructor_name", StringType(), True),
        StructField("length_minutes", IntegerType(), True),
        StructField("fitness_discipline", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("total_output", IntegerType(), True),
        StructField("avg_watts", IntegerType(), True),
        StructField("avg_resistance", StringType(), True),
        StructField("avg_cadence_rpm", IntegerType(), True),
        StructField("avg_speed_mph", DoubleType(), True),
        StructField("distance_mi", DoubleType(), True),
        StructField("calories_burned", IntegerType(), True),
        StructField("avg_heartrate", IntegerType(), True),
        StructField("workout_date", StringType(), True),
        StructField("workout_time", StringType(), True),
        StructField("utc_offset", StringType(), True),
    ]
)


# -------------------------------------------------------
# Timestamp Normalization
# -------------------------------------------------------


def normalize_timestamp(df: DataFrame, ts_col: str, offset_col: str = None) -> DataFrame:
    """Convert timestamps to UTC.

    Args:
        df: Input DataFrame
        ts_col: Column containing the timestamp
        offset_col: Optional column with UTC offset (e.g., "-04")

    Returns:
        DataFrame with added `timestamp_utc` column
    """
    if offset_col and offset_col in df.columns:
        # Parse offset and adjust
        df = df.withColumn(
            "timestamp_utc",
            F.to_utc_timestamp(F.col(ts_col), F.concat(F.lit("GMT"), F.col(offset_col))),
        )
    else:
        # Assume already UTC or has timezone info in the string
        df = df.withColumn("timestamp_utc", F.to_timestamp(F.col(ts_col)))
    return df


# -------------------------------------------------------
# Forward Fill
# -------------------------------------------------------


def forward_fill(df: DataFrame, partition_col: str, order_col: str, fill_cols: list) -> DataFrame:
    """PySpark forward-fill for missing values.

    Uses window functions to carry forward the last non-null value.

    Args:
        df: Input DataFrame
        partition_col: Column to partition by (or None for global)
        order_col: Column to order by
        fill_cols: List of column names to forward-fill

    Returns:
        DataFrame with forward-filled columns
    """
    if partition_col:
        window = Window.partitionBy(partition_col).orderBy(order_col).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
    else:
        window = Window.orderBy(order_col).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

    for col_name in fill_cols:
        df = df.withColumn(col_name, F.last(F.col(col_name), ignorenulls=True).over(window))

    return df


# -------------------------------------------------------
# Schema Validation
# -------------------------------------------------------


def validate_schema(df: DataFrame, required_columns: list, dataset_name: str) -> bool:
    """Validate that a DataFrame contains all required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        dataset_name: Name for error messages

    Returns:
        True if valid, raises ValueError if not
    """
    actual_cols = set(df.columns)
    missing = [c for c in required_columns if c not in actual_cols]

    if missing:
        raise ValueError(
            f"Schema validation failed for {dataset_name}. "
            f"Missing columns: {missing}. "
            f"Available columns: {sorted(actual_cols)}"
        )
    return True


# -------------------------------------------------------
# Workout Type Categorization
# -------------------------------------------------------

WORKOUT_CATEGORY_MAP = {
    "cycling": "cardio_high",
    "running": "cardio_high",
    "bootcamp": "cardio_high",
    "rowing": "cardio_high",
    "bike_bootcamp": "cardio_high",
    "circuit": "cardio_high",
    "strength": "strength_training",
    "stretching": "recovery",
    "yoga": "recovery",
    "meditation": "recovery",
    "walking": "cardio_low",
    "cardio": "cardio_high",
    "outdoor": "cardio_high",
}


def categorize_workout_type(df: DataFrame, discipline_col: str = "fitness_discipline") -> DataFrame:
    """Add a normalized workout_category column based on fitness discipline.

    Categories: cardio_high, cardio_low, strength_training, recovery, other
    """
    mapping_expr = F.create_map(
        *[item for pair in WORKOUT_CATEGORY_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    df = df.withColumn(
        "workout_category",
        F.coalesce(
            mapping_expr[F.lower(F.col(discipline_col))],
            F.lit("other"),
        ),
    )
    return df


# -------------------------------------------------------
# Derived Metrics
# -------------------------------------------------------


def calculate_output_per_minute(df: DataFrame) -> DataFrame:
    """Calculate output_per_minute = total_output / length_minutes."""
    return df.withColumn(
        "output_per_minute",
        F.when(
            (F.col("length_minutes").isNotNull()) & (F.col("length_minutes") > 0),
            F.round(F.col("total_output") / F.col("length_minutes"), 2),
        ).otherwise(None),
    )


def calculate_hr_zones(df: DataFrame, max_hr: int = 200) -> DataFrame:
    """Estimate HR zone from average heart rate.

    Zones (% of max HR):
        1: <60%, 2: 60-70%, 3: 70-80%, 4: 80-90%, 5: >90%
    """
    return df.withColumn(
        "hr_zone",
        F.when(F.col("avg_heartrate").isNull(), None)
        .when(F.col("avg_heartrate") < max_hr * 0.6, 1)
        .when(F.col("avg_heartrate") < max_hr * 0.7, 2)
        .when(F.col("avg_heartrate") < max_hr * 0.8, 3)
        .when(F.col("avg_heartrate") < max_hr * 0.9, 4)
        .otherwise(5),
    )
