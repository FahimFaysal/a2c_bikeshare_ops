-- src/governance/12_abac.sql — Task 5.3: two policies cover every tagged Gold column.
-- 1. Governed tags (no IF NOT EXISTS on this workspace; run once)
CREATE GOVERNED TAG sensitivity VALUES ('financial', 'restricted', 'location');
CREATE GOVERNED TAG access_scope VALUES ('partner');

-- 2. Tag every Gold object EXCEPT gold_trip_detail_recent: it already has a manual row filter and masks,
--    and a column cannot carry both a manual rule and an ABAC rule.
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN lat SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN lng SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN lat SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN lng SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_demand_hourly  ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_weather_demand ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_ride_behaviour ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN system SET TAGS ('access_scope' = 'partner');

-- 3. One mask policy and one row-filter policy cover every tagged column in the schema
CREATE POLICY generalise_locations
ON SCHEMA workspace.prd_gold
COMMENT 'Coordinates are rounded to about 100 m unless the reader holds the sensitive entitlement'
COLUMN MASK workspace.prd_ops.mask_coord
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity', 'location') AS c
ON COLUMN c;

CREATE POLICY partner_rows
ON SCHEMA workspace.prd_gold
COMMENT 'Partner analysts only see rows for the scope they are entitled to'
ROW FILTER workspace.prd_ops.rf_scope
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('access_scope', 'partner') AS s
USING COLUMNS (s);

SHOW POLICIES ON SCHEMA workspace.prd_gold;
-- 4. Keep your own entitlement at '*' whenever the pipeline refreshes.
-- 5. Switch to state 2 from Task 5.2 and query the tagged tables: New York rows disappear
--    and coordinates come back rounded.
