-- src/perf/layout_lab.sql — clustering-key comparison, run on the prd Gold materialized views
-- Baseline: current CLUSTER BY (defined in gold.sql). Compare a query that filters on a different column.

-- 1) Query profile of a filter that matches the existing clustering key
SELECT count(*) FROM workspace.prd_lakehouse.gold_demand_hourly WHERE start_date = '2025-02-14';
-- capture: bytes/files pruned, from the query profile "Data Filters" / "Files pruned" line

-- 2) Same shape of query, filtered on a column that is NOT the clustering key
SELECT count(*) FROM workspace.prd_lakehouse.gold_demand_hourly WHERE member_type = 'member';
-- capture: same metrics, expect little or no pruning

-- 3) Re-cluster on the alternate column and re-run query (2)
ALTER MATERIALIZED VIEW workspace.prd_lakehouse.gold_demand_hourly CLUSTER BY (member_type);
OPTIMIZE workspace.prd_lakehouse.gold_demand_hourly;
SELECT count(*) FROM workspace.prd_lakehouse.gold_demand_hourly WHERE member_type = 'member';
-- capture: pruning improves for (2)'s query shape, but query (1)'s shape would now prune worse

-- 4) Put the clustering key back to what the pipeline defines, then let the next pipeline run reconcile it
ALTER MATERIALIZED VIEW workspace.prd_lakehouse.gold_demand_hourly CLUSTER BY (start_date);
OPTIMIZE workspace.prd_lakehouse.gold_demand_hourly;
