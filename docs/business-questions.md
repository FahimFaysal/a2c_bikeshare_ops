# Business questions — draft queries

Status: **first drafts on the dev slice** (February 2025 only, one GBFS snapshot taken
2026-09-30 08:01 UTC). The numbers below prove the queries run and are plausible; they are not the
client answers. Final SQL, results and interpretation come from the reconciled `prd` tables on Day 5.

Replace `dev_gold` with `prd_gold` for the final run.

## BQ1 — When and how does the city ride?

```sql
-- Trips per day, member share and e-bike share by month
SELECT period, count(DISTINCT start_date) AS days,
       round(sum(trips) / count(DISTINCT start_date)) AS trips_per_day,
       round(100 * sum(member_trips) / sum(trips), 1) AS member_pct,
       round(100 * sum(ebike_trips) / sum(trips), 1)  AS ebike_pct
FROM workspace.dev_gold.gold_weather_demand GROUP BY period ORDER BY period;

-- Hour x weekday heatmap
SELECT start_dow, start_hour, sum(trips) AS trips
FROM workspace.dev_gold.gold_demand_hourly GROUP BY ALL;

-- Weather effect, compared within each month, with the number of days in each class
SELECT period, is_rain_day, temp_band, count(DISTINCT start_date) AS days,
       round(sum(trips) / count(DISTINCT start_date)) AS avg_trips_per_day
FROM workspace.dev_gold.gold_weather_demand GROUP BY ALL ORDER BY period, is_rain_day, temp_band;
```

Dev result (February 2025): 74,140 trips per day, 90.4% by members, 69.7% on e-bikes. Busiest
cells: Wednesday and Tuesday 17:00 (about 36,000 trips each over the month), then Tuesday 08:00.

| Rain day | Max temperature | Days | Avg trips per day |
|---|---|---|---|
| no | below 5 °C | 13 | 64,903 |
| no | 5 to 15 °C | 11 | 94,065 |
| yes | below 5 °C | 3 | 55,695 |
| yes | 5 to 15 °C | 1 | 30,388 |

To fix for the final: the `2025-01` period shows 1 day with 267 trips (trips that started on
31 January but were published in the February file); months must be reported on complete data only.
The rain classes have 3 and 1 days, too few to state an effect from one month.

## BQ2 — Which stations drain, and which fill?

```sql
-- 20 stations that drain most in the morning peak (use ORDER BY ... DESC for those that fill,
-- and peak_window = 'PM 16-19' for the evening)
SELECT system, station_id, station_name, avg_departures_per_weekday,
       avg_arrivals_per_weekday, avg_net_flow_per_weekday, lat, lng
FROM workspace.dev_gold.gold_station_flow
WHERE peak_window = 'AM 07-10'
ORDER BY avg_net_flow_per_weekday ASC LIMIT 20;
```

Dev result, morning peak, top of each list (average per weekday over 21 weekdays):
- Drains: W 43 St & 10 Ave −45.4 (67.4 out, 22.0 in); FDR Drive & E 35 St −32.7; Grand St & Samuel Dickstein Plaza −32.1.
- Fills: E 47 St & Park Ave +68.5 (34.3 out, 102.8 in); North Moore St & Greenwich St +57.1; 1 Ave & E 68 St +49.8; in Jersey City, Grove St PATH +44.9.

Excluded because they have no station id: 673 trips without a start station (all e-bikes) and
4,593 without an end station (4,591 e-bikes, 2 classic bikes).

## BQ3 — How do people ride?

```sql
SELECT rideable_type, member_type, sum(trips) AS trips,
       round(100 * sum(round_trips) / sum(trips), 2)       AS round_trip_pct,
       round(100 * sum(trips_over_45_min) / sum(trips), 2) AS over_45_min_pct
FROM workspace.dev_gold.gold_ride_behaviour GROUP BY ALL;
-- p50 / p90 are read per month from the same table (percentiles cannot be summed across months)
```

Dev result (February 2025):

| Bike | Rider | Trips | Median min | p90 min | Round trips | Over 45 min |
|---|---|---|---|---|---|---|
| classic | casual | 40,719 | 11.79 | 31.27 | 5.09% | 4.30% |
| classic | member | 589,118 | 6.94 | 19.99 | 1.45% | 0.76% |
| electric | casual | 157,798 | 8.98 | 24.16 | 2.88% | 2.69% |
| electric | member | 1,288,287 | 7.11 | 17.63 | 1.02% | 0.46% |

The shortest trip in the data is just over one minute because the publisher removes trips under
60 seconds, so the low end of the distribution is cut off before it reaches us.

## BQ4 — Are the busiest stations the ones that run out?

```sql
SELECT system, station_id, station_name, snapshots, empty_share, full_share,
       departures, weekday_peak_departures, lat, lng
FROM workspace.dev_gold.gold_station_health
ORDER BY departures DESC LIMIT 20;
```

Dev result: not meaningful yet. There is one snapshot, so every share is 0 or 1. In that snapshot
92 of 2,104 matched stations had no bikes and 336 had no free docks. The answer needs the 8+ hours
of 30-minute snapshots collected in `prd`, and even then it covers hours, not months, and compares
2026 availability with 2024–25 demand; both limits must be stated with the answer.
