-- src/governance/11_entitlement_states.sql — Task 5.2: the same query in three entitlement states.
-- State 1: full access (scope '*', sensitive = true). Screenshot each result.
SELECT system, count(*) AS trips, min(ride_id) AS sample_ride, avg(start_lat) AS avg_lat, max(start_lat) AS max_lat
FROM workspace.prd_gold.gold_trip_detail_recent GROUP BY ALL ORDER BY system;

-- State 2: the Jersey City partner without access to identifiers or precise coordinates
UPDATE workspace.prd_ops.entitlements SET scope_value = 'JC', can_see_sensitive = false
WHERE user_email = session_user();
-- re-run the query: only JC rows, ride ids pseudonymised, coordinates rounded to 3 decimals

-- State 3: back to full access. Always end here: the pipeline refresh runs as this user.
UPDATE workspace.prd_ops.entitlements SET scope_value = '*', can_see_sensitive = true
WHERE user_email = session_user();
