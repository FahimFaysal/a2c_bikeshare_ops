
# Demo script — Assignment 2C (workspace version)
# Demo script — Assignment 2C (GitHub version)


Follows the assignment's outline. Have open before starting: the published dashboard, the prod
build job's run list, the SQL editor with `src/governance/11_entitlement_states.sql`, the GitHub
repository's Actions tab, and a terminal in the repo.

## 1. The problem and the answers — 2 minutes (dashboard)
- One sentence: "The transport department needs to know where bikes pile up and where stations run
  dry, and to trust the numbers behind it."
- Release tile: the data on screen is the latest certified release.
- Heatmap: weekday commuter peaks at 08:00 and 17:00–18:00.
- E-bike share by month; rain table compared within each month, with the number of days.
- Draining and filling stations, table and map.
- Busy stations that run empty: say that the snapshots cover hours, not months.

## 2. How the data gets there — 3 minutes (job and pipeline)
- Landing Volume → `prepare` unzips and counts rows → pipeline Bronze, Silver, Gold.
- Open one prod build run: the DAG, the for-each over periods, the gate.
- `SELECT * FROM workspace.prd_ops.reconciliation`: every period and system, expected = Bronze,
  Silver + quarantine = Bronze, status OK.
- Quarantine: what the rules are and how many rows each one caught; nothing is dropped silently.
- The incident branch: the run that failed the gate and the row in `ops.incidents`.

## 3. Live proof — 2 minutes (terminal)
- Drop one file that has not been delivered yet (keep one back for this):
  `python tools/drop_files.py --env prd --match <file>`
- Show the build job starting by itself with trigger type "File arrival"; while it runs, move on.
  Come back at the end for the release run with trigger type "Table update".

## 4. Trust — 2 minutes (SQL editor)
- Run the entitlement query as full access, then as the Jersey City partner: New York rows are
  gone, ride ids are pseudonymised, coordinates are rounded. Switch back.
- `SHOW POLICIES ON SCHEMA workspace.prd_gold`: two policies cover every tagged Gold column.
- Lineage graph of one Gold object back to Bronze and forward to the dashboard.

## 5. How it ships — 1 minute (GitHub)
- A pull request runs `bundle validate`; the merge deploys dev, then prod.
- One bundle, one variable, two environments; to deploy for a client, add a target and run
  `databricks bundle deploy`.

## If asked
- What would change for a real client: service principal with OIDC instead of a token; a paid
  workspace; alerting to a channel instead of one mailbox; GBFS collected continuously.
- Limits: availability snapshots cover hours; weather is one station (Central Park).
