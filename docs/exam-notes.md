# Exam notes — Assignment 2C

Answers to the 27 checkpoint questions. Each one is tied to something that happened in this build,
so it can be explained from experience in the walkthrough.

> Draft status: written with an AI assistant from the project's own runs. Before submitting, each
> answer is to be re-read and rephrased in my own words.

## Day 1 — Platform and ingestion

**1. The same COPY INTO runs twice against the same folder. What does the second run load, and why?**
Nothing. COPY INTO remembers, per target table, which files it has loaded and skips them. Here the
second `setup_job` run left `ref_station_information` at 1 row and wrote no new table version
(history shows one COPY INTO commit). `COPY_OPTIONS ('force' = 'true')` would reload everything.

**2. When do you pick Auto Loader over COPY INTO?**
Auto Loader when files keep arriving and there are many of them, or when the schema can change:
it tracks discovered files incrementally, infers and evolves the schema and rescues bad values.
That is the trip CSVs and the GBFS status snapshots. COPY INTO for a small, re-runnable SQL batch
load, such as the one-a-day station reference file.

**3. A SaaS application has a managed connector; an internal system only has a REST API. How do you ingest each?**
The SaaS source with the Lakeflow Connect managed connector: Databricks runs and maintains the
incremental pipeline. The REST source with a notebook that calls the API, lands the raw responses
in a Volume, reads its credential from a secret scope and is scheduled by a job. That is how NOAA
and GBFS are ingested here.

**4. Where does an API token belong, and what do you see if you print it?**
In a secret scope (`a2` / `noaa_token`), read with `dbutils.secrets.get`. Printing it shows
`[REDACTED]`. Redaction only hides the literal value in output; the real protection is who has
access to the scope.

**5. Which compute fits ad-hoc exploration, a nightly ETL job and a BI dashboard? Why can't `spark.executor.memory` be set here?**
Exploration: interactive compute (all-purpose cluster or a serverless notebook). Nightly ETL: job
compute, which exists only for the run and costs less. Dashboard: a SQL warehouse. On serverless
Databricks sizes the machines, so only a short list of Spark settings (six) can be changed.

## Day 2 — Pipeline

**6. `expect`, `expect_or_drop`, `expect_or_fail`?**
`expect` keeps the row and counts the violation (the 5,422 trips without a station id).
`expect_or_drop` removes the row before it is written (the 323 trips over 24 hours, which go to
quarantine instead). `expect_or_fail` stops the update and writes nothing (the temporary demo rule
failed update `e68ced`).

**7. For a BI consumer: streaming table, materialized view or view?**
Streaming table when data is append-only and each row is processed once (Bronze, row-level
Silver). Materialized view when the result must reflect everything in the sources: aggregates,
joins, de-duplication (all Gold objects here). View when nothing needs storing and the logic is
cheap enough to compute per query.

**8. Auto Loader in `addNewColumns` mode meets a file with a new column. What happens?**
The stream stops with an unknown-field error after adding the column to the stored schema. The
next update continues with the new column; old rows have NULL in it and existing types do not
change. In the drill the update was cancelled on `feed_version` and a new update, "started by
SCHEMA_CHANGE", completed by itself.

**9. What lands in `_rescued_data`?**
Values that do not fit the schema: a type mismatch, a column-name case mismatch, or a field that
is not in the schema when evolution is off. It holds them as JSON with the source file path. In
the drill it stayed empty, because the new field became a real column.

**10. Why de-duplicate in a materialized view, not in the streaming table?**
Removing duplicates across all history in a stream needs either state that grows without limit or
a watermark, and a watermark silently discards late rows. A materialized view is computed over the
whole table, so the result is always right. Here it removed exactly the 1,000 drill rows.

## Day 3 — Jobs and CI/CD

**11. A partner drops files at unpredictable times. Which trigger, and what goes wrong with cron?**
A file-arrival trigger. With cron the job runs either before the files are there (empty or partial
run) or long after (stale data), and burns compute when nothing is new. The prod build started
itself about two minutes after the February files landed.

**12. A downstream job must run whenever a certified table changes. Which trigger?**
A table-update trigger on that table. The release job started 14 seconds after `certify` wrote to
`ops.release`.

**13. What does `mode: development` change?**
Resources get a `[dev user]` name prefix, all triggers and schedules are paused, pipelines run in
development mode, concurrent runs are allowed and the deployment lock is off.

**14. Bundle variable versus target override?**
A variable is a value substituted into definitions that all targets share (`env` gives `dev_*` or
`prd_*` schemas). A target override merges settings over the shared definition for one target
only (the release job's `on_success` e-mail exists only in prod).

**15. A run failed in task 4 of 7. What does repair run re-run?**
Only the failed task and the tasks that depend on it. Tasks that succeeded keep their results. The
repair uses the job's current settings, so a fix deployed in between is picked up.

**16. You work in a Git folder. Where is the pull request opened?**
In the Git provider (GitHub). The Git folder covers clone, branch, commit, push, pull, merge and
conflict resolution; review and merge to `main` happen on GitHub.

## Day 4 — Performance and troubleshooting

**17. How does skew show up, and how is it fixed?**
A few tasks run far longer and handle far more rows than the rest, often with spill to disk.
Fixes: a key with more distinct values, salting the hot key, broadcasting the small side of a
join, letting adaptive execution split skewed partitions, or aggregating first.

**18. `spark.sql.shuffle.partitions` versus `spark.default.parallelism`?**
The first is the number of partitions after a shuffle in DataFrame and SQL work (joins,
aggregations); on serverless it defaults to `auto`. The second is the default partition count for
RDD operations and cannot be set on serverless.

**19. What does `spark.sql.autoBroadcastJoinThreshold` control?**
The largest table size Spark will broadcast automatically in a join (10 MB by default, −1 turns it
off). It cannot be set on serverless, so a `BROADCAST` hint is used instead.

**20. Liquid Clustering, partitioning, Z-ORDER: when each?**
Liquid Clustering by default: it copes with columns that have many distinct values, clusters
incrementally and the keys can be changed later. Partitioning only for columns with few values and
large partitions (about 1 GB or more). Z-ORDER is the older method and needs repeated full
OPTIMIZE runs. Liquid Clustering cannot be combined with the other two.

**21. What does predictive optimization do?**
It runs OPTIMIZE, VACUUM and ANALYZE on Unity Catalog managed tables by itself, so no maintenance
jobs are scheduled. With `CLUSTER BY AUTO` it also picks the clustering keys from query patterns.

**22. A classic cluster will not start, or a job fails with a library conflict. Where do you look first?**
The cluster event log for start-up problems (init scripts, quotas, instance availability). The
task error and driver log for a library problem, then pin or remove the dependency. On serverless
the place to check is the environment's dependency list.

## Day 5 — Governance

**23. What happens to the data on DROP of a managed table, and of an external table?**
Managed: Unity Catalog removes the files, but the table can be restored with UNDROP within the
retention period (7 days by default). External: only the metadata goes; the files stay in the
storage location.

**24. Minimum privileges to query a table?**
`USE CATALOG` on the catalog, `USE SCHEMA` on the schema and `SELECT` on the table, granted
directly or inherited from a higher level.

**25. Row filter, column mask, ABAC policy?**
A row filter is a function that decides which rows a reader gets. A column mask is a function that
changes a column's value for a reader. An ABAC policy is written once on a catalog or schema and
applies a filter or mask to every table whose columns carry a matching governed tag, so new tables
are covered without new rules.

**26. How does a DENY interact with a GRANT?**
A DENY always wins, over direct, inherited and group grants and over ownership; metastore admins
are exempt. Today only `MANAGE ACCESS CONTROL` can be denied, it needs classic compute on DBR 18
LTS or later (not available on Free Edition), and `SHOW GRANTS` does not list it.

**27. A schema-level SELECT is revoked but the user can still read the table. Why?**
They still hold it another way: a grant on the catalog or on the table itself, membership of a
group that has it, or ownership. `SHOW GRANTS` has to be checked at each level and for each group.
