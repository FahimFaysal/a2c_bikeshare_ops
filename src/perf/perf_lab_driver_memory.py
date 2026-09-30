# Databricks notebook source
# src/perf/perf_lab_driver_memory.py — lab part 5, kept apart so a driver failure cannot take the
# other measurements with it. One attempt only: record what happens, then explain the fix.
import time
from pyspark.sql import functions as F

ONE_PERIOD = spark.table("workspace.prd_lakehouse.silver_trips").where(F.col("period") == "2025-02")
t0 = time.time()
try:
    rows = ONE_PERIOD.collect()
    outcome = f"collected {len(rows):,} rows to the driver in {time.time() - t0:.1f}s"
except Exception as exc:
    outcome = f"failed after {time.time() - t0:.1f}s: {type(exc).__name__}: {str(exc)[:300]}"
print("collect():", outcome)

# The fix: aggregate on the cluster and bring back the small result.
t0 = time.time()
small = ONE_PERIOD.groupBy("start_date").agg(F.count("*").alias("trips")).collect()
print(f"aggregate first: {len(small)} rows to the driver in {time.time() - t0:.1f}s")
spark.createDataFrame([("driver_memory", "collect() of one period", outcome)],
                      "part string, setting string, detail string") \
     .write.mode("append").saveAsTable("workspace.prd_ops.perf_driver_memory")
