-- src/governance/10_access.sql — Task 5.1: grant a reader the gold schema only, prove a REVOKE, table lifecycle.
-- No teammate is invited to this workspace, so the reader is `account users` (assignment fallback).
GRANT USE CATALOG ON CATALOG workspace TO `account users`;
GRANT USE SCHEMA ON SCHEMA workspace.prd_gold TO `account users`;
GRANT SELECT ON SCHEMA workspace.prd_gold TO `account users`;   -- inherited by current and future Gold objects
-- The row filter and masks call functions in prd_ops and read ops.entitlements with the definer's rights,
-- so readers need no grant on prd_ops.
SHOW GRANTS ON SCHEMA workspace.prd_gold;
REVOKE SELECT ON SCHEMA workspace.prd_gold FROM `account users`;
SHOW GRANTS ON SCHEMA workspace.prd_gold;                       -- the SELECT row is gone
GRANT SELECT ON SCHEMA workspace.prd_gold TO `account users`;   -- restore it for the demo

-- Managed-table lifecycle
CREATE TABLE workspace.prd_ops.scratch_undrop AS SELECT * FROM workspace.prd_gold.gold_ride_behaviour;
DROP TABLE workspace.prd_ops.scratch_undrop;
SHOW TABLES DROPPED IN workspace.prd_ops;
UNDROP TABLE workspace.prd_ops.scratch_undrop;
