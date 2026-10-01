# src/pipeline/silver.py — conform, validate, quarantine, de-duplicate
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transforms import ALL_DROP, DROP_RULES, WARN_RULES, add_derived, conform

LAKEHOUSE = spark.conf.get("a2.lakehouse")

WEATHER_RESULTS = "array<struct<date string, datatype string, station string, attributes string, value double>>"


@dp.temporary_view()
def v_trips_conformed():
    return add_derived(conform(spark.readStream.table("bronze_trips")))


@dp.table(name="silver_trips_clean", cluster_by=["start_date"], comment="Trips that pass every drop rule")
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")
@dp.expect_all(WARN_RULES)
@dp.expect_all_or_drop(DROP_RULES)
def silver_trips_clean():
    return spark.readStream.table("v_trips_conformed")


@dp.table(name="silver_trips_quarantine", comment="Trips that failed at least one drop rule")
def silver_trips_quarantine():
    return spark.readStream.table("v_trips_conformed").where(f"NOT ({ALL_DROP})")


@dp.materialized_view(name="silver_trips", cluster_by=["start_date", "start_station_id"],
                       comment="Conformed trips from both systems, de-duplicated on ride_id")
def silver_trips():
    return spark.read.table("silver_trips_clean").dropDuplicates(["_row_key"])


@dp.table(name="silver_station_status", comment="One row per station per GBFS snapshot")
def silver_station_status():
    return (spark.readStream.table("bronze_station_status")
            .select(F.col("last_updated").cast("long").alias("snapshot_epoch"),
                    F.explode("data.stations").alias("s"), "_source_file")
            .select(F.timestamp_seconds("snapshot_epoch").alias("snapshot_ts"),
                    F.col("s.station_id").cast("string").alias("station_id"),
                    F.col("s.num_bikes_available").cast("int").alias("bikes_available"),
                    F.col("s.num_ebikes_available").cast("int").alias("ebikes_available"),
                    F.col("s.num_docks_available").cast("int").alias("docks_available"),
                    F.col("s.is_renting").cast("int").alias("is_renting"),
                    # check how your feed encodes these
                    F.col("s.is_returning").cast("int").alias("is_returning"),
                    "_source_file"))


@dp.materialized_view(name="dim_station", comment="Latest GBFS station_information snapshot, one row per station")
def dim_station():
    ref = spark.read.table(f"{LAKEHOUSE}.ref_station_information")
    latest = ref.agg(F.max("last_updated").alias("last_updated"))
    return (ref.join(latest, "last_updated")
            .select(F.explode("data.stations").alias("s"))
            .select(F.col("s.station_id").cast("string").alias("station_id"),
                    F.col("s.short_name").cast("string").alias("short_name"),
                    F.col("s.name").alias("name"),
                    F.col("s.lat").cast("double").alias("lat"),
                    F.col("s.lon").cast("double").alias("lon"),
                    F.col("s.capacity").cast("int").alias("capacity"),
                    F.col("s.region_id").cast("string").alias("region_id")))


@dp.materialized_view(name="silver_weather_daily", comment="One row per station and day; the latest fetch wins")
def silver_weather_daily():
    obs = (spark.read.table("bronze_weather")
           # Bronze keeps the CDO response as delivered: without type inference "results" is a JSON string
           .select("fetched_at", F.explode(F.from_json("results", WEATHER_RESULTS)).alias("r"))
           .select("fetched_at",
                   F.to_date(F.substring("r.date", 1, 10)).alias("obs_date"),
                   F.regexp_replace("r.station", "^GHCND:", "").alias("station_id"),
                   F.col("r.datatype").alias("datatype"),
                   F.col("r.value").cast("double").alias("value")))
    # Each scheduled fetch lands a new file: keep the most recently fetched value per station, day and element.
    latest = obs.groupBy("obs_date", "station_id", "datatype").agg(F.max_by("value", "fetched_at").alias("value"))
    # Pipelines do not support .pivot(); conditional aggregation does the same job.
    return (latest.groupBy("obs_date", "station_id")
            .agg(*[F.max(F.when(F.col("datatype") == t, F.col("value"))).alias(t.lower())
                   for t in ["PRCP", "SNOW", "TMAX", "TMIN"]]))
