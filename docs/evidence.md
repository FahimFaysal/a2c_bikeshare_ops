# Evidence pack — Assignment 2C

Real, verified evidence only. Each entry below is filled in as its task is completed, with the
actual command/output captured — nothing is written ahead of verification.

## Day 1

### Task 1.1 — Environment & smoke test
See `README.md` → "Environment findings (Task 1.1)" for the full table and raw JSON.

### Task 1.2 — Repository and bundle skeleton
- Repo: https://github.com/FahimFaysal/a2c_bikeshare_ops (public, standalone)
- Environment issue hit + fixed: CLI v0.244.0's `bundle validate` failed with
  `error downloading Terraform: unable to verify checksums signature: openpgp: key expired`
  (HashiCorp GPG-key-expiry bug in the CLI's internal Terraform download). Fixed by upgrading the
  Databricks CLI to v1.18.0.
- `databricks bundle validate -t dev` → `Validation OK!`
- `databricks bundle deploy -t dev` → `Resources: 6 created, 0 changed, 0 deleted, 0 unchanged`
  (jobs.weather_job, jobs.release_job, jobs.setup_job, jobs.gbfs_job, jobs.build_job,
  pipelines.lakehouse); `Files: 28 uploaded, 0 deleted`. No bootstrap-trigger error on first deploy.
- `databricks bundle run -t dev setup_job` → `TERMINATED SUCCESS`
- `SHOW SCHEMAS IN workspace LIKE 'dev_*'` → `dev_gold, dev_lakehouse, dev_landing, dev_ops`
- `SHOW VOLUMES IN workspace.dev_landing` → `raw`
- `SELECT * FROM workspace.dev_ops.entitlements` → `fahim.faysal@bjitgroup.com | * | true`

### Task 1.3 — Land first period + reference data
-

### Task 1.4 — REST ingestion with secrets
-

## Day 2

### Task 2.1 — Bronze
-

### Task 2.2 — Silver
-

### Task 2.3 — Gold
-

## Day 3

### Task 3.1 — Build job DAG
-

### Task 3.2 — Triggers
-

### Task 3.3 — CI/CD
-

## Day 4

### Task 4.1 — Backfill
-

### Task 4.2 — Tuning
-

### Task 4.3 — Skew/spill/failure
-

### Task 4.4 — Layout
-

### Task 4.5 — Monitoring
-

## Day 5

### Task 5.1 — Access control
-

### Task 5.2 — Row filter / column mask
-

### Task 5.3 — ABAC
-

### Task 5.4 — Dashboard
-

### Task 5.5 — Client pack
-
