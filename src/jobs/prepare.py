# Databricks notebook source
# src/jobs/prepare.py — unzip newly landed trip files and register their expected row counts
import os
import re
import shutil
import zipfile

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
RAW = f"/Volumes/{CAT}/{ENV}_landing/raw"
ZIP_DIR, CSV_DIR = f"{RAW}/tripdata_zip", f"{RAW}/tripdata_csv"
MANIFEST = f"{CAT}.{ENV}_ops.file_manifest"
PATTERN = re.compile(r"^(JC-)?(\d{4})(\d{2})-citibike-tripdata(\.csv)?\.zip$")

dbutils.fs.mkdirs(CSV_DIR)
known = {r.landed_file for r in spark.table(MANIFEST).select("landed_file").collect()}
periods = set()

for f in sorted(dbutils.fs.ls(ZIP_DIR), key=lambda f: f.name):
    m = PATTERN.match(f.name)
    if not m or f.name in known:
        continue
    system = "jc" if m.group(1) else "nyc"
    period = f"{m.group(2)}-{m.group(3)}"
    local_zip = f"/tmp/{f.name}"
    shutil.copyfile(f"{ZIP_DIR}/{f.name}", local_zip)
    # Volumes are readable as local paths
    registered = []
    with zipfile.ZipFile(local_zip) as zf:
        members = sorted(n for n in zf.namelist()
                          if n.lower().endswith(".csv") and not n.startswith("__MACOSX"))
        for i, member in enumerate(members, 1):
            out_name = f"{system.upper()}_{period}_{i}.csv"
            local_csv = f"/tmp/{out_name}"
            with zf.open(member) as src, open(local_csv, "wb") as dst:
                shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            with open(local_csv, "rb") as fh:
                expected = sum(1 for _ in fh) - 1
                # data rows = lines minus the header
            shutil.copyfile(local_csv, f"{CSV_DIR}/{out_name}")
            # overwrites: a retry after a crash re-lands the same name with the same content
            os.remove(local_csv)
            registered.append((system, f.name, out_name, period, expected))
            print(f"{f.name} -> {out_name}: {expected:,} rows")
    # Crash safety: a zip is skipped once it is in the manifest, so all its CSVs are registered in ONE
    # commit after every copy succeeded. A run that dies earlier leaves no manifest row and is redone.
    if registered:
        (spark.createDataFrame(registered, "dataset string, landed_file string, unpacked_file string, "
                                           "period string, expected_rows bigint")
         .withColumn("registered_at", F.current_timestamp())
         .write.mode("append").saveAsTable(MANIFEST))
    os.remove(local_zip)
    periods.add(period)

dbutils.jobs.taskValues.set(key="periods", value=sorted(periods))
dbutils.jobs.taskValues.set(key="n_new", value=len(periods))
