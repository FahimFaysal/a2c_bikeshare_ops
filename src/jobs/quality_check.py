# Databricks notebook source
# src/jobs/quality_check.py — decides the if/else branch through a task value
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("max_quarantine_pct", "2.0")
CAT, ENV, RUN_ID = dbutils.widgets.get("catalog"), dbutils.widgets.get("env"), dbutils.widgets.get("run_id")
MAX_Q = float(dbutils.widgets.get("max_quarantine_pct"))
OPS = f"{CAT}.{ENV}_ops"

rec = spark.table(f"{OPS}.reconciliation").where(F.col("run_id") == RUN_ID)
r = rec.agg(F.count("*").alias("checks"),
            F.sum(F.when(F.col("status") != "OK", 1).otherwise(0)).alias("mismatches"),
            F.sum("bronze_rows").alias("bronze"),
            F.sum("quarantined_rows").alias("quarantined")).first()

quarantine_pct = 100.0 * (r.quarantined or 0) / max(1, r.bronze or 0)
# Own check: the de-duplicated Silver table must hold each ride_id once, or every Gold count is inflated
d = spark.table(f"{CAT}.{ENV}_lakehouse.silver_trips").agg(
    (F.count("*") - F.countDistinct("ride_id")).alias("duplicate_keys")).first()
duplicate_keys = d.duplicate_keys or 0
passed = ((r.checks or 0) > 0 and (r.mismatches or 0) == 0 and quarantine_pct <= MAX_Q
          and duplicate_keys == 0)

print(f"checks={r.checks} mismatches={r.mismatches} quarantine={quarantine_pct:.3f}% (max {MAX_Q}%) "
      f"duplicate_keys={duplicate_keys} -> {'PASS' if passed else 'FAIL'}")

dbutils.jobs.taskValues.set(key="gate", value="pass" if passed else "fail")
dbutils.jobs.taskValues.set(key="quarantine_pct", value=round(quarantine_pct, 3))
