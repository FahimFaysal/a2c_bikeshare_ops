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

-- BQ1: one row per day and system, with Central Park weather. Rain day = prcp >= 5 mm (assignment definition).
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_weather_demand
COMMENT 'Daily trips by system joined to NOAA Central Park (USW00094728) daily weather'
AS SELECT
  t.system,
  t.start_date,
  date_format(t.start_date, 'yyyy-MM')                        AS period,
  count(*)                                                    AS trips,
  count_if(t.member_type = 'member')                          AS member_trips,
  count_if(t.member_type = 'casual')                          AS casual_trips,
  count_if(t.rideable_type = 'electric_bike')                 AS ebike_trips,
  any_value(w.prcp)                                           AS prcp_mm,
  any_value(w.snow)                                           AS snow_mm,
  any_value(w.tmax)                                           AS tmax_c,
  any_value(w.tmin)                                           AS tmin_c,
  any_value(w.prcp) >= 5                                      AS is_rain_day,
  CASE WHEN any_value(w.tmax) IS NULL THEN NULL
       WHEN any_value(w.tmax) < 5  THEN '1: below 5 C'
       WHEN any_value(w.tmax) < 15 THEN '2: 5 to 15 C'
       WHEN any_value(w.tmax) < 25 THEN '3: 15 to 25 C'
       ELSE '4: 25 C and above' END                           AS temp_band
FROM silver_trips t
LEFT JOIN silver_weather_daily w
  ON w.obs_date = t.start_date AND w.station_id = 'USW00094728'
GROUP BY t.system, t.start_date;

-- BQ2: departures, arrivals and net flow per station in the weekday peaks (Mon-Fri).
-- AM = 07:00-10:00, PM = 16:00-19:00. A departure is counted at its start time, an arrival at its end time.
-- Trips without a station id cannot be placed at a station and are left out (counted in the README).
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_station_flow
COMMENT 'Weekday peak-window departures, arrivals and net flow per station, averaged per weekday'
AS WITH events AS (
  SELECT system, start_station_id AS station_id, start_station_name AS station_name,
         start_date AS event_date, start_hour AS event_hour, 1 AS departure, 0 AS arrival,
         start_lat AS trip_lat, start_lng AS trip_lng
  FROM silver_trips WHERE start_station_id IS NOT NULL
  UNION ALL
  SELECT system, end_station_id, end_station_name,
         to_date(ended_at), hour(ended_at), 0, 1, end_lat, end_lng
  FROM silver_trips WHERE end_station_id IS NOT NULL
),
peaks AS (
  SELECT *, CASE WHEN event_hour BETWEEN 7 AND 9 THEN 'AM 07-10' ELSE 'PM 16-19' END AS peak_window
  FROM events
  WHERE dayofweek(event_date) BETWEEN 2 AND 6
    AND (event_hour BETWEEN 7 AND 9 OR event_hour BETWEEN 16 AND 18)
),
weekdays AS (
  SELECT system, count(DISTINCT start_date) AS n_weekdays
  FROM silver_trips WHERE dayofweek(start_date) BETWEEN 2 AND 6 GROUP BY system
)
SELECT
  p.system,
  p.station_id,
  max(p.station_name)                                         AS station_name,
  p.peak_window,
  any_value(w.n_weekdays)                                     AS n_weekdays,
  sum(p.departure)                                            AS departures,
  sum(p.arrival)                                              AS arrivals,
  round(sum(p.departure) / any_value(w.n_weekdays), 2)        AS avg_departures_per_weekday,
  round(sum(p.arrival) / any_value(w.n_weekdays), 2)          AS avg_arrivals_per_weekday,
  round((sum(p.arrival) - sum(p.departure)) / any_value(w.n_weekdays), 2) AS avg_net_flow_per_weekday,
  coalesce(any_value(d.lat), avg(p.trip_lat))                 AS lat,
  coalesce(any_value(d.lon), avg(p.trip_lng))                 AS lng
FROM peaks p
JOIN weekdays w ON w.system = p.system
LEFT JOIN dim_station d ON d.short_name = p.station_id
GROUP BY p.system, p.station_id, p.peak_window;

-- BQ3: how people ride. Counts are kept next to the shares so a consumer can re-aggregate correctly.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_ride_behaviour
COMMENT 'Duration percentiles, round-trip share and share over 45 minutes by system, month, bike type and rider type'
AS SELECT
  system, period, rideable_type, member_type,
  count(*)                                                    AS trips,
  round(percentile_approx(duration_min, 0.5), 2)              AS p50_duration_min,
  round(percentile_approx(duration_min, 0.9), 2)              AS p90_duration_min,
  count_if(is_round_trip)                                     AS round_trips,
  round(count_if(is_round_trip) / count(*), 4)                AS round_trip_share,
  count_if(duration_min > 45)                                 AS trips_over_45_min,
  round(count_if(duration_min > 45) / count(*), 4)            AS over_45_min_share
FROM silver_trips
GROUP BY system, period, rideable_type, member_type;

-- BQ4: availability from the GBFS snapshots, demand recomputed from Silver (not from gold_station_flow).
-- GBFS station_id is a UUID; the trip files' station id equals GBFS short_name, so dim_station is the bridge.
-- Only snapshots where the station was renting and returning are counted.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_station_health
COMMENT 'Share of GBFS snapshots empty or full per station, with trip demand; system taken from trips starting there'
AS WITH availability AS (
  SELECT station_id,
         count(*)                         AS snapshots,
         count_if(bikes_available = 0)    AS empty_snapshots,
         count_if(docks_available = 0)    AS full_snapshots,
         min(snapshot_ts)                 AS first_snapshot_ts,
         max(snapshot_ts)                 AS last_snapshot_ts
  FROM silver_station_status
  WHERE is_renting = 1 AND is_returning = 1
  GROUP BY station_id
),
demand AS (
  SELECT start_station_id AS short_name,
         max_by(system, n)                AS system,
         sum(n)                           AS departures,
         sum(peak_n)                      AS weekday_peak_departures
  FROM (SELECT start_station_id, system, count(*) AS n,
               count_if(dayofweek(start_date) BETWEEN 2 AND 6
                        AND (start_hour BETWEEN 7 AND 9 OR start_hour BETWEEN 16 AND 18)) AS peak_n
        FROM silver_trips WHERE start_station_id IS NOT NULL
        GROUP BY start_station_id, system)
  GROUP BY start_station_id
)
SELECT
  m.system,
  d.short_name                                                AS station_id,
  d.name                                                      AS station_name,
  d.capacity,
  a.snapshots,
  a.empty_snapshots,
  a.full_snapshots,
  round(a.empty_snapshots / a.snapshots, 4)                   AS empty_share,
  round(a.full_snapshots / a.snapshots, 4)                    AS full_share,
  a.first_snapshot_ts,
  a.last_snapshot_ts,
  m.departures,
  m.weekday_peak_departures,
  d.lat                                                       AS lat,
  d.lon                                                       AS lng
FROM availability a
JOIN dim_station d ON d.station_id = a.station_id
JOIN demand m      ON m.short_name = d.short_name;
