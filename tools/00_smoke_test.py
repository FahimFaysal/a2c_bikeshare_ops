# Databricks notebook source
# 00_smoke_test.py — run once on Day 1 in a serverless notebook; paste the output into README.md
import requests

HOSTS = {
    "Citi Bike trip files (S3)": "https://s3.amazonaws.com/tripdata/index.html",
    "GBFS feed": "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json",
    "NOAA CDO API v2": "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets",
    "NOAA Access Data Service (fallback)": "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations=USW00094728&startDate=2025-01-01&endDate=2025-01-02&format=json",
    "PyPI (control: usually allowed)": "https://pypi.org/simple/requests/",
}

results = {"hosts": {}, "who_am_i": None, "secret_scopes": None, "secret_scopes_error": None,
           "row_filter": None, "row_filter_error": None}

print("1) Outbound internet from serverless compute")
for label, url in HOSTS.items():
    try:
        r = requests.get(url, timeout=15, stream=True)
        print(f"   {label:<38} reachable (HTTP {r.status_code})")  # any HTTP status = reachable
        results["hosts"][label] = f"reachable (HTTP {r.status_code})"
    except Exception as exc:
        print(f"   {label:<38} BLOCKED   ({type(exc).__name__})")
        results["hosts"][label] = f"BLOCKED ({type(exc).__name__})"

print("2) Who am I")
who = spark.sql("SELECT session_user()").first()[0]
print("   session_user():", who)
results["who_am_i"] = who

print("3) Secret scopes I can see")
try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    print("  ", scopes)
    results["secret_scopes"] = scopes
except Exception as exc:
    print("   secrets not available:", exc)
    results["secret_scopes_error"] = str(exc)

# COMMAND ----------
# 4) Row filters work on this compute? (creates and drops a throw-away schema)
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.a2_smoke")
    spark.sql("CREATE OR REPLACE TABLE workspace.a2_smoke.t AS SELECT * FROM VALUES ('A', 1), ('B', 2) AS t(k, v)")
    spark.sql("CREATE OR REPLACE FUNCTION workspace.a2_smoke.only_a(k STRING) RETURNS BOOLEAN RETURN k = 'A'")
    spark.sql("ALTER TABLE workspace.a2_smoke.t SET ROW FILTER workspace.a2_smoke.only_a ON (k)")
    rf_rows = [tuple(r) for r in spark.table("workspace.a2_smoke.t").collect()]
    print("4) Row filter result (expect only A):", rf_rows)
    results["row_filter"] = str(rf_rows)
    spark.sql("DROP SCHEMA workspace.a2_smoke CASCADE")
except Exception as exc:
    print("4) Row filter test failed:", exc)
    results["row_filter_error"] = str(exc)
    try:
        spark.sql("DROP SCHEMA IF EXISTS workspace.a2_smoke CASCADE")
    except Exception:
        pass


# COMMAND ----------
import json

print("RESULTS_JSON=" + json.dumps(results))
dbutils.notebook.exit(json.dumps(results))
