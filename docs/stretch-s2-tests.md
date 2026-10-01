# Stretch goal S2 — unit tests in CI

The pure transformation logic of the Silver layer (`conform`, `add_derived` and the drop and warn
rule definitions) lives in `src/pipeline/transforms.py`; `silver.py` imports it. Nothing in that
module touches a Spark session, a pipeline decorator or pipeline configuration, so the same code runs
in the Lakeflow pipeline and under pytest on a local PySpark 4.0 session (`tests/`).

Run locally:

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q          # Java 17 is required by PySpark
```

15 tests, all passing locally (14 s). What they pin down, each one a behaviour the build depends on:

- station ids stay text (`5329.03` is not turned into a double);
- the system is taken from the unpacked file name prefix (`JC_`), everything else is NYC;
- an unparseable timestamp becomes NULL instead of raising under ANSI mode (the session runs with ANSI on, as serverless does);
- derived columns (duration, date, hour, weekday, period, file period, round-trip flag, row key);
- every drop rule at its boundary (1 minute and 24 hours in, one second outside; end before start; unknown rider type);
- NULLs fail the drop rules, so a bad row is quarantined and never slips through;
- clean + quarantine equals the input row for row (the reconciliation identity, on a five-row frame);
- warn rules flag a row (missing station, coordinates outside the area, month mismatch) without dropping it;
- a chispa frame comparison of the conformed output.

CI: `.github/workflows/deploy.yml` has a `test` job (Java 17, Python 3.12, `pytest -q`) and
`validate` now `needs: test`, so a failing test stops a pull request before `bundle validate`.

Checked in the workspace before merging: the refactored `silver.py` was deployed to `dev` and the dev
pipeline ran to COMPLETED (update `4b84a2`), so the `from transforms import ...` works inside a pipeline.
The prod target gets the same file on the merge.
