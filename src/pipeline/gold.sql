-- src/pipeline/gold.sql — consumer-facing objects, written to the gold schema by fully qualified name
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_demand_hourly
CLUSTER BY (start_date)
COMMENT 'Trips per hour by system, rider type and bike type'
AS SELECT
  system, start_date, start_hour, start_dow, member_type, rideable_type,
  count(*)                                 AS trips,
  percentile_approx(duration_min, 0.5)     AS median_duration_min
FROM silver_trips
GROUP BY ALL;

-- The governed object: row filter and masks are part of the definition, so every refresh keeps them.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_trip_detail_recent (
  system            STRING,
  ride_id           STRING MASK ${a2.ops}.mask_text,
  started_at        TIMESTAMP,
  ended_at          TIMESTAMP,
  rideable_type     STRING,
  member_type       STRING,
  start_station_id  STRING,
  end_station_id    STRING,
  start_lat         DOUBLE MASK ${a2.ops}.mask_coord,
  start_lng         DOUBLE MASK ${a2.ops}.mask_coord,
  end_lat           DOUBLE MASK ${a2.ops}.mask_coord,
  end_lng           DOUBLE MASK ${a2.ops}.mask_coord,
  duration_min      DOUBLE
)
WITH ROW FILTER ${a2.ops}.rf_scope ON (system)
CLUSTER BY (started_at)
COMMENT 'Last 30 days of trips in the data, for investigations. Filtered by system, ids and coordinates masked.'
AS SELECT
  system, ride_id, started_at, ended_at, rideable_type, member_type,
  start_station_id, end_station_id, start_lat, start_lng, end_lat, end_lng, duration_min
FROM silver_trips
WHERE started_at >= (SELECT max(started_at) - INTERVAL 30 DAYS FROM silver_trips);

-- TODO (Task 2.3) gold_weather_demand (start_date x system): trips joined to silver_weather_daily (USW00094728)
-- TODO (Task 2.3) gold_station_flow   (system x station x peak window): departures, arrivals, net flow per weekday, lat, lng
-- TODO (Task 2.3) gold_ride_behaviour (system x period x rideable_type x member_type): p50 / p90 duration, round-trip share, share > 45 min
-- TODO (Task 2.3) gold_station_health (system x station): share of snapshots empty / full while renting and returning, demand, lat, lng
--      (take system from the trips that start at the station)
