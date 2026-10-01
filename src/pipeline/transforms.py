# src/pipeline/transforms.py — the pure trip transformations and rule definitions used by silver.py.
# No Spark session, pipeline decorator or configuration is read here, so the same code runs in the
# Lakeflow pipeline and under pytest on a local PySpark session (tests/test_transforms.py).
from pyspark.sql import DataFrame, functions as F

RAW_DROP_RULES = {
    "ride_id_present": "ride_id IS NOT NULL",
    "ended_after_started": "ended_at > started_at",
    "duration_1min_to_24h": "duration_min BETWEEN 1 AND 1440",
    "member_type_valid": "member_type IN ('member', 'casual')",
}
DROP_RULES = {k: f"coalesce({v}, false)" for k, v in RAW_DROP_RULES.items()}
# a NULL fails the rule

WARN_RULES = {
    "period_matches_file": "period = file_period",
    "coords_in_area": "start_lat BETWEEN 40.4 AND 41.0 AND start_lng BETWEEN -74.3 AND -73.6",
    "stations_present": "start_station_id IS NOT NULL AND end_station_id IS NOT NULL",
}

ALL_DROP = " AND ".join(DROP_RULES.values())
COORDS = ["start_lat", "start_lng", "end_lat", "end_lng"]


def conform(df: DataFrame) -> DataFrame:
    return df.select(
        F.when(F.col("_source_file").startswith("JC_"), "JC").otherwise("NYC").alias("system"),
        F.col("ride_id").alias("ride_id"),
        F.col("rideable_type").alias("rideable_type"),
        F.try_to_timestamp(F.col("started_at")).alias("started_at"),
        # ANSI-safe: bad values become NULL
        F.try_to_timestamp(F.col("ended_at")).alias("ended_at"),
        F.col("start_station_id").cast("string").alias("start_station_id"),
        F.col("start_station_name").alias("start_station_name"),
        F.col("end_station_id").cast("string").alias("end_station_id"),
        F.col("end_station_name").alias("end_station_name"),
        *[F.col(c).cast("double").alias(c) for c in COORDS],
        F.col("member_casual").alias("member_type"),
        F.col("_source_file"),
    )


def add_derived(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("duration_min", (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60.0)
            .withColumn("start_date", F.to_date("started_at"))
            .withColumn("start_hour", F.hour("started_at"))
            .withColumn("start_dow", F.date_format("started_at", "E"))
            .withColumn("period", F.date_format("started_at", "yyyy-MM"))
            .withColumn("file_period", F.regexp_extract("_source_file", r"_(\d{4}-\d{2})_", 1))
            .withColumn("is_round_trip", F.col("start_station_id") == F.col("end_station_id"))
            .withColumn("_row_key", F.col("ride_id")))
