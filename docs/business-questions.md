# Business questions — SQL, results and interpretation

Source: the reconciled prod tables (`workspace.prd_gold`), 55,774,718 trips from May 2024 to June
2025, every monthly file reconciled to its source (README, Task 4.1). Queries run 2026-09-30.
BQ4 is **provisional**: it rests on 6 GBFS snapshots (about 2.5 hours), and the assignment asks for
8 or more hours; it is to be re-run and replaced (see the end of BQ4).

Definitions used throughout: a trip is counted in the month it starts; a rain day has at least
5 mm of precipitation at Central Park; temperature bands use the day's maximum (below 5, 5–15,
15–25, 25 °C and above). A one-day "April 2024" appears in the data (229 trips that started on
30 April and were published in the May file); it is left out of monthly comparisons.

## BQ1 — When and how does the city ride?

```sql
-- Trips per day, member share and e-bike share by month
SELECT period, count(DISTINCT start_date) AS days,
       round(sum(trips) / count(DISTINCT start_date)) AS trips_per_day,
       round(100 * sum(member_trips) / sum(trips), 1) AS member_pct,
       round(100 * sum(ebike_trips) / sum(trips), 1)  AS ebike_pct
FROM workspace.prd_gold.gold_weather_demand GROUP BY period ORDER BY period;

-- Hour x weekday heatmap
SELECT start_dow, start_hour, sum(trips) AS trips FROM workspace.prd_gold.gold_demand_hourly GROUP BY ALL;

-- Rain against dry days within each month, with the number of days in each class
WITH d AS (SELECT period, start_date, max(is_rain_day) AS r, sum(trips) AS t
           FROM workspace.prd_gold.gold_weather_demand GROUP BY 1, 2)
SELECT period, count_if(NOT r) AS dry_days, count_if(r) AS rain_days,
       round(avg(CASE WHEN NOT r THEN t END)) AS dry_per_day, round(avg(CASE WHEN r THEN t END)) AS rain_per_day
FROM d GROUP BY period HAVING count(*) >= 27 ORDER BY period;
```

| Month | Trips per day | Member share | E-bike share |
|---|---|---|---|
| 2024-05 | 136,430 | 79.2% | 64.9% |
| 2024-06 | 163,086 | 76.6% | 64.8% |
| 2024-07 | 155,937 | 76.8% | 65.8% |
| 2024-08 | 151,908 | 76.7% | 66.4% |
| 2024-09 | 170,391 | 79.0% | 66.3% |
| 2024-10 | 169,940 | 80.7% | 66.7% |
| 2024-11 | 126,454 | 83.2% | 68.1% |
| 2024-12 | 76,307 | 87.2% | 69.8% |
| 2025-01 | 70,157 | 90.4% | 70.2% |
| 2025-02 | 74,154 | 90.4% | 69.7% |
| 2025-03 | 104,538 | 85.8% | 70.3% |
| 2025-04 | 126,865 | 83.4% | 69.3% |
| 2025-05 | 142,521 | 80.4% | 69.4% |
| 2025-06 | 161,816 | 79.9% | 70.8% |

Rain against dry days, same month (trips per day; days in each class in brackets):

| Month | Dry | Rain | Difference |
|---|---|---|---|
| 2024-05 | 144,995 (23) | 111,805 (8) | −22.9% |
| 2024-06 | 163,517 (27) | 159,210 (3) | −2.6% |
| 2024-07 | 156,449 (29) | 148,510 (2) | −5.1% |
| 2024-08 | 158,190 (24) | 130,367 (7) | −17.6% |
| 2024-09 | 177,737 (27) | 104,273 (3) | −41.3% |
| 2024-10 | 169,940 (31) | no rain days | — |
| 2024-11 | 134,851 (27) | 50,881 (3) | −62.3% |
| 2024-12 | 76,592 (24) | 75,328 (7) | −1.7% |
| 2025-01 | 71,004 (30) | 44,726 (1) | −37.0% |
| 2025-02 | 78,285 (24) | 49,368 (4) | −36.9% |
| 2025-03 | 108,241 (26) | 85,282 (5) | −21.2% |
| 2025-04 | 133,148 (26) | 86,026 (4) | −35.4% |
| 2025-05 | 155,784 (22) | 110,100 (9) | −29.3% |
| 2025-06 | 166,958 (26) | 128,394 (4) | −23.1% |

Within a single month, dry days with warmer maximums had more trips (October 2024: 148,901 trips
per day on 3 days of 5–15 °C, 170,971 on 25 days of 15–25 °C, 182,386 on 3 days of 25 °C and above;
April 2025: 108,948 / 136,629 / 168,351 on 9 / 12 / 5 days).

**In plain language.** The city rides about two and a half times as much in the best month
(September 2024, about 170,000 trips a day) as in the quietest (January 2025, about 70,000).
Weekday demand peaks at 17:00 on Tuesdays to Thursdays (the single busiest cell is Tuesday at
17:00, 889,040 trips over the year) with a second peak around 08:00. Members make most trips and
their share is highest in winter (90% in January, 77% in summer); casual riders are a quarter of
weekend trips (26%) against 17% on weekdays. The e-bike share rose from 65% in May 2024 to 71%
in June 2025. On days with rain of 5 mm or more, there were on average 26% fewer trips than on dry
days of the same month (average of the 13 months that had rain days, 60 rain days in all).
**Limits:** the seasonal swing is far larger than the weather effect, which is why comparisons are
made inside each month; the rain effect in a single month rests on 1 to 9 rain days, so individual
months (for example −62% in November on 3 days) are noisy and only the overall figure should be
quoted; weather is one station (Central Park) and the wording is "fewer trips on rainy days", not
that rain caused the drop.

## BQ2 — Which stations drain, and which fill?

```sql
SELECT system, station_name, avg_departures_per_weekday, avg_arrivals_per_weekday,
       avg_net_flow_per_weekday, lat, lng
FROM workspace.prd_gold.gold_station_flow
WHERE peak_window = 'AM 07-10'            -- or 'PM 16-19'
ORDER BY avg_net_flow_per_weekday ASC     -- DESC for the stations that fill
LIMIT 20;
```

Top of each list (bikes per weekday; net = arrivals − departures; 305 NYC and 304 JC weekdays):

| Peak | Drains most | Net | Fills most | Net |
|---|---|---|---|---|
| Morning 07–10 | W 43 St & 10 Ave | −55.3 | E 47 St & Park Ave | +104.6 |
| | Grand St & Samuel Dickstein Plaza | −41.2 | Dock 72 Way & Market St | +60.6 |
| | W 44 St & 11 Ave | −40.4 | Grove St PATH (Jersey City) | +60.3 |
| Evening 16–19 | North Moore St & Greenwich St | −103.6 | FDR Drive & E 35 St | +39.7 |
| | E 47 St & Park Ave | −101.9 | W 43 St & 10 Ave | +36.4 |
| | Dock 72 Way & Market St | −62.4 | 12 Ave & W 40 St | +34.9 |

The full top 20 of each list, with coordinates for the map, is in the dashboard and in
`gold_station_flow`.

**In plain language.** The same stations flip direction between the peaks: E 47 St & Park Ave
(Midtown office area) receives about 105 more bikes than it loses each weekday morning and loses
about 102 more than it receives each evening, while W 43 St & 10 Ave, which drains in the morning,
refills in the evening. These are the stations where bikes have to be moved between the peaks.
**Limits:** 28,793 trips without a start station (all e-bikes) and 148,262 without an end station
(185 classic bikes, 148,077 e-bikes) of 55.8 M trips cannot be placed at a station and are left out;
averages divide by every weekday with trips, holidays included; station coordinates are generalised
for readers without the sensitive entitlement.

## BQ3 — How do people ride?

```sql
SELECT rideable_type, member_type, count(*) AS trips,
       round(percentile_approx(duration_min, 0.5), 2) AS p50_min,
       round(percentile_approx(duration_min, 0.9), 2) AS p90_min,
       round(100 * avg(CASE WHEN is_round_trip THEN 1 ELSE 0 END), 2)  AS round_trip_pct,
       round(100 * avg(CASE WHEN duration_min > 45 THEN 1 ELSE 0 END), 2) AS over_45_min_pct
FROM workspace.prd_lakehouse.silver_trips GROUP BY 1, 2 ORDER BY 1, 2;
```

| Bike | Rider | Trips | Median (min) | p90 (min) | Round trips | Over 45 min |
|---|---|---|---|---|---|---|
| classic | casual | 2,875,221 | 15.3 | 39.6 | 6.7% | 7.6% |
| classic | member | 15,142,951 | 7.6 | 22.8 | 1.9% | 1.2% |
| electric | casual | 7,740,928 | 12.0 | 35.4 | 4.0% | 6.4% |
| electric | member | 30,015,618 | 8.5 | 22.2 | 1.4% | 1.1% |

**In plain language.** Casual riders ride about twice as long as members (median 15 minutes on a
classic bike against 8), take round trips four times as often (6.7% against 1.9%) and are six
times as likely to keep a bike over 45 minutes (7.6% against 1.2%). Electric bikes shorten casual
rides (12 against 15 minutes) and barely change member rides. **Limits:** the publisher already
removed trips under 60 seconds, so the distribution starts at exactly one minute (the shortest trip
is 1.0 minute, 41,306 trips are under 1.05) and the real median is somewhat lower than shown;
percentiles are approximate (`percentile_approx`); the Gold table's trip-weighted medians differ from
these exact figures by under 0.2 minutes, which is why the exact ones above come from Silver.

## BQ4 — Are the busiest stations the ones that run out? (provisional)

```sql
SELECT system, station_name, departures, snapshots,
       round(100 * empty_share) AS empty_pct, round(100 * full_share) AS full_pct
FROM (SELECT * FROM workspace.prd_gold.gold_station_health ORDER BY departures DESC LIMIT 100)
ORDER BY empty_share DESC, departures DESC LIMIT 20;
```

The station keys differ between the two sources: the trip files use the GBFS `short_name`. The
join matches 2,165 of 2,365 NYC start stations (91.5%, 95.9% of NYC trips) and 174 of 183 Jersey
City stations (95.1%, 93.4% of trips); the unmatched are stations that existed in 2024–25 but are
not in today's station list.

Provisional result (6 snapshots, 11:30–13:59 UTC on 2026-09-30, renting and returning stations
only): of 2,121 matched stations, 183 were empty in at least one snapshot, 47 in half or more, and
133 were full in half or more. Among the 100 busiest stations the most often empty were
11 Ave & W 41 St (empty in half of the snapshots, 157,701 departures) and Columbus Ave & W 72 St,
1 Ave & E 18 St, Henry St & Grand St and E 10 St & 2 Ave (empty in a third each).
Across all stations the correlation between demand and the share of time empty is 0.03.

**In plain language (provisional).** On this first look, being busy does not predict running empty:
the correlation is close to zero, and the busiest stations that are often empty are a short list
worth watching, not a pattern. **Limits that must accompany any final statement:** the snapshots
cover hours of one day in September 2026, while demand covers May 2024 to June 2025; six snapshots
give shares in steps of 17%; availability at 11:30–14:00 UTC (morning in New York) says nothing
about the evening; no cause is claimed. **To do for the final answer:** after at least 8 hours of
snapshots (from about 19:30 UTC), refresh the prod pipeline, re-run this query, replace the numbers
above and re-check the correlation.
