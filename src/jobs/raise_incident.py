# Databricks notebook source
# src/jobs/raise_incident.py — the "false" branch: record why, then fail the run on purpose
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("quarantine_pct", "")
OPS = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('env')}_ops"

reason = f"quality gate failed (quarantine {dbutils.widgets.get('quarantine_pct')}%, see reconciliation)"
# MERGE, not INSERT: a retried task must not record the same incident twice
spark.sql("""MERGE INTO IDENTIFIER(:t) t USING (SELECT :run_id AS run_id, :reason AS reason) s
             ON t.run_id = s.run_id
             WHEN NOT MATCHED THEN INSERT (run_id, reason, raised_at) VALUES (s.run_id, s.reason, current_timestamp())""",
          args={"t": f"{OPS}.incidents", "run_id": dbutils.widgets.get("run_id"), "reason": reason})
raise RuntimeError(reason)
# a failed run triggers the on_failure e-mail
