# src/pipeline/station_history.py — stretch S3: station attributes over time (SCD Type 2)
# Each station_information snapshot (one a day, loaded by COPY INTO into ref_station_information) is a
# full picture of the stations. AUTO CDC from snapshots compares each snapshot with the previous one
# and keeps one row per station per distinct set of attributes, with __START_AT / __END_AT.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LAKEHOUSE = spark.conf.get("a2.lakehouse")

dp.create_streaming_table("dim_station_history",
                          comment="Station attributes over time, SCD Type 2 from daily GBFS snapshots")


def next_snapshot(latest_version):
    """Return (DataFrame, version) for the oldest snapshot not yet processed, or None when caught up."""
    ref = spark.read.table(f"{LAKEHOUSE}.ref_station_information")
    upcoming = ref.where(F.col("last_updated") > F.lit(latest_version if latest_version is not None else 0))
    nxt = upcoming.agg(F.min("last_updated")).first()[0]
    if nxt is None:
        return None
    stations = (ref.where(F.col("last_updated") == nxt)
                .select(F.explode("data.stations").alias("s"))
                .select(F.col("s.station_id").cast("string").alias("station_id"),
                        F.col("s.short_name").cast("string").alias("short_name"),
                        F.col("s.name").alias("name"),
                        F.col("s.lat").cast("double").alias("lat"),
                        F.col("s.lon").cast("double").alias("lon"),
                        F.col("s.capacity").cast("int").alias("capacity"),
                        F.col("s.region_id").cast("string").alias("region_id")))
    return stations, nxt


dp.create_auto_cdc_from_snapshot_flow(
    target="dim_station_history",
    source=next_snapshot,
    keys=["station_id"],
    stored_as_scd_type=2,
)
