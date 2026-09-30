# Databricks notebook source
# src/perf/perf_lab.py — run on the prd tables after the backfill. One-off lab: it names prd explicitly.
# Every measurement runs twice; the second run (warm) is recorded in prd_ops.perf_results together
# with the physical plan, so the numbers in the README come from a table, not from memory.
import contextlib
import io
import time
from pyspark.sql import functions as F

SILVER = "workspace.prd_lakehouse.silver_trips"
DIM = "workspace.prd_lakehouse.dim_station"
RESULTS = "workspace.prd_ops.perf_results"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESULTS} (
    part STRING, setting STRING, duration_s DOUBLE, join_strategy STRING, plan STRING, recorded_at TIMESTAMP)""")

def plan_of(df) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()

def strategy_of(plan: str) -> str:
    for name in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin"):
        if name in plan:
            return name
    return "none"

def timed(part, setting, df, runs=2):
    """Run the whole plan `runs` times without storing anything; record the last duration."""
    for _ in range(runs):
        t0 = time.time()
        df.write.format("noop").mode("overwrite").save()
        duration = time.time() - t0
    plan = plan_of(df)
    print(f"{part:<10} {setting:<34} {duration:7.1f}s  {strategy_of(plan)}")
    spark.createDataFrame([(part, setting, duration, strategy_of(plan), plan)],
                          "part string, setting string, duration_s double, join_strategy string, plan string") \
         .withColumn("recorded_at", F.current_timestamp()).write.mode("append").saveAsTable(RESULTS)

ONE_PERIOD = spark.table(SILVER).where(F.col("period") == "2025-02")

# COMMAND ----------
# 1) Shuffle partitions: one of the few Spark confs you may set on serverless
for setting in ["auto", "8", "4000"]:
    spark.conf.set("spark.sql.shuffle.partitions", setting)
    timed("shuffle", f"shuffle.partitions={setting}",
          spark.table(SILVER).groupBy("start_station_id", "start_date").count())
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------
# 2) Input split size: compare the number of scan tasks in the query profile
for size in ["128MB", "16MB"]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", size)
    timed("split", f"maxPartitionBytes={size}", ONE_PERIOD.agg(F.sum("duration_min")))
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")  # back to the default

# COMMAND ----------
# 3) Join strategy: autoBroadcastJoinThreshold is not settable on serverless, so use hints
dim = spark.table(DIM)
cond = ONE_PERIOD["start_station_id"] == dim["short_name"]
timed("join", "BROADCAST hint (dim_station)", ONE_PERIOD.join(F.broadcast(dim), cond).agg(F.count("*")))
timed("join", "MERGE hint (sort-merge)", ONE_PERIOD.join(dim.hint("merge"), cond).agg(F.count("*")))
timed("join", "no hint (optimizer's choice)", ONE_PERIOD.join(dim, cond).agg(F.count("*")))

# COMMAND ----------
# 4) Skew and spill on purpose: a window over a low-cardinality key, one period only.
# Fix: add a high-cardinality key so the work spreads out. Compare the profiles.
timed("skew", "window PARTITION BY member_type", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY member_type ORDER BY started_at) AS rn"))
timed("skew", "window PARTITION BY member_type, start_date", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY member_type, start_date ORDER BY started_at) AS rn"))
