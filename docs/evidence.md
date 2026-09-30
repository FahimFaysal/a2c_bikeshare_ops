# Evidence pack — Assignment 2C

Real, verified evidence only. Each entry below is filled in as its task is completed, with the
actual command/output captured — nothing is written ahead of verification.

## Evidence index (assignment Section 8)

| # | Evidence | Task | Status |
|---|---|---|---|
| E1 | Smoke-test results | 1.1 | done — README "Task 1.1" |
| E2 | Reconciliation for every period | 4.1 | |
| E3 | Schema-evolution event | 2.1 | done — below; screenshot to add |
| E4 | Expectation metrics | 2.2 | done — below; screenshot to add |
| E5 | COPY INTO second run = 0 rows | 1.3 | done — below |
| E6 | Secret redaction | 1.4 | run done; screenshot to add |
| E7 | Build job DAG and green run | 3.1 | done — below; screenshot to add |
| E8 | Incident branch run | 3.1 | done — below; screenshot and e-mail to add |
| E9 | File-arrival and table-update triggered runs | 3.3 | |
| E10 | GitHub Actions runs | 3.3 | |
| E11 | Tuning results | 4.2 | |
| E12 | Skew before/after | 4.3 | |
| E13 | Repair run | 4.3 | |
| E14 | Layout comparison | 4.4 | |
| E15 | Governance in three entitlement states, grants before/after REVOKE | 5.1–5.3 | |
| E16 | Lineage graph | 5.4 | |

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

### Task 1.3 — Land first period + reference data (E5)
Run 2026-09-30, dev. Full tables in `README.md` → "Task 1.3".
- `drop_files.py --env dev --match 202502` → 2 zips in `raw/tripdata_zip/` (396 MB + 2 MB).
- `bundle run -t dev gbfs_job` → `TERMINATED SUCCESS`; one `station_information` and one
  `station_status` snapshot (`20260930T080117Z`).
- **E5** `bundle run -t dev setup_job` twice: `SELECT count(*) FROM dev_lakehouse.ref_station_information`
  → 1 after run 1, 1 after run 2. `DESCRIBE HISTORY` shows versions 0 (CREATE TABLE) and 1
  (COPY INTO, `numFiles 1, numOutputRows 1`) only: the second run wrote nothing.
- `prepare.py` one-off run `479485360658836` (task run `212031765015647`, SUCCESS, 32 s) →
  manifest: `JC_2025-02_1.csv` 45,255; `NYC_2025-02_1.csv` 1,000,000; `NYC_2025-02_2.csv` 1,000,000;
  `NYC_2025-02_3.csv` 31,257 (NYC total 2,031,257).
- Screenshot still to add: the second COPY INTO result cell in the workspace UI (0 rows).

### Task 1.4 — REST ingestion with secrets (E6)
Run 2026-09-30, dev.
- `databricks secrets list-secrets a2` → key `noaa_token`.
- `weather_rest.py` one-off run `1026475686969055` (task run `714733142701311`) → SUCCESS; two JSON
  files in `raw/weather/` (2024-05-01..2024-12-31 and 2025-01-01..2025-06-30).
- One GBFS snapshot in `raw/gbfs_status/` (`station_status_20260930T080117Z.json`).
- Pattern search for a PAT (`dapi…`) or a 32-letter key across the repository: 0 hits.
- **E6** screenshot still to add: run output showing `token: [REDACTED]`.

## Day 2

### Task 2.1 — Bronze (E3)
Run 2026-09-30, dev. Tables in `README.md` → "Task 2.1".
- Pipeline update `df5c4215` COMPLETED. Bronze rows per file = manifest expected rows for all four
  files (45,255 / 1,000,000 / 1,000,000 / 31,257); clean + quarantine = Bronze for each.
- **E3** drift drill run `661980989091322`: update `6e8469` cancelled on
  `UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE [feed_version]`; update `89a88c` started by
  SCHEMA_CHANGE and COMPLETED; `feed_version` non-NULL in 1,000 rows, NULL elsewhere;
  `_rescued_data` non-NULL in 0 rows; de-duplication removed 1,000 rows (2,077,189 → 2,076,189).
- Screenshot still to add: the pipeline event log showing the schema-change stop and restart.

### Task 2.2 — Silver (E4)
Run 2026-09-30, dev. Profile and rules table in `README.md` → "Task 2.2".
- **E4** expectation metrics, update `df5c42`, flow `silver_trips_clean` (passed / failed):
  has_source_file 2,076,512 / 0; ride_id_present 2,076,512 / 0; ended_after_started 2,076,512 / 0;
  duration_1min_to_24h 2,076,189 / 323; member_type_valid 2,076,512 / 0;
  period_matches_file 2,076,237 / 275; coords_in_area 2,075,824 / 688; stations_present 2,071,090 / 5,422.
- Balance: clean 2,076,189 + quarantine 323 = Bronze 2,076,512 (real files); true for each file.
- Duplicates: 0 in the real files; 1,000 drill rows removed by `silver_trips`.
- Fail demo: update `e68ced` FAILED on `DEMO_every_trip_has_start_station`; rule removed; update
  `6ba4ad` COMPLETED and restored 2,077,189 clean rows.
- Screenshots still to add: data-quality tab of `silver_trips_clean`; failed update `e68ced` and
  its event-log entry.

### Task 2.3 — Gold
Run 2026-09-30, dev.
- All six Gold flows COMPLETED in one update (09:48 UTC).
- `sum(trips)` = 2,076,189 in `gold_demand_hourly`, `gold_weather_demand`, `gold_ride_behaviour`
  = `count(*)` of `silver_trips`. `gold_station_flow` 4,658 rows, `gold_station_health` 2,104 rows,
  0 days without weather.
- Station match rate (trip id = GBFS `short_name`): NYC 2,096 / 2,240 stations, JC 80 / 83.
- Draft queries and results: `docs/business-questions.md`.

## Day 3

### Task 3.1 — Build job DAG (E7, E8)
Run 2026-09-30, dev. Task-by-task table in `README.md` → "Task 3.1".
- **E7** green run `163219437441288` (SUCCESS, 343 s): prepare, has_new=true, build, reconcile_each,
  quality_check, gate=true, certify; raise_incident EXCLUDED. Reconciliation 2025-03 jc OK
  (73,293 = 73,280 + 13). One row in `ops.release`.
- **E8** incident run `555298946603170` (`max_quarantine_pct=-1`, FAILED): gate=false, certify
  EXCLUDED, raise_incident FAILED; `ops.incidents` row "quality gate failed (quarantine 0.028%, ...)".
- Screenshots still to add: DAG of both runs (with the for-each iteration opened); the failure e-mail.

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
