# Stretch goal S3 — slowly changing station dimension (SCD Type 2)

`src/pipeline/station_history.py` builds `dim_station_history` with the pipeline's AUTO CDC flow
from snapshots (`create_auto_cdc_from_snapshot_flow`, key `station_id`, `stored_as_scd_type=2`). Its
source function returns, one at a time, the oldest daily `station_information` snapshot in
`ref_station_information` that has not been processed (version = the snapshot's `last_updated`); the
flow compares each snapshot with the previous one and writes one row per station per distinct set of
attributes, with `__START_AT` and `__END_AT` (NULL = the current row). The existing `dim_station`
(latest snapshot only) is unchanged and is still what the Gold layer joins.

Verified in `dev` on 2026-10-01 (pipeline updates `b5e14e` onwards):

- First snapshot: 2,520 rows for 2,520 stations, all current (`__END_AT` NULL).
- The real snapshots did not show a change within the available time (dev has one, prod one a day
  since 2026-09-30), so the change was tested with **a synthetic snapshot made for the test** and
  removed afterwards: a copy of the real snapshot one day later in which station
  `66db2a71-0aca-11e7-82f6-3863bb44ef7c` is renamed and its capacity raised from 0 to 10, and the
  last station (`2108372903577159978`, Union St & Bergen Ave) is absent.
- Result after loading it with COPY INTO and refreshing the pipeline:

| Station | Name | Capacity | `__START_AT` | `__END_AT` |
|---|---|---|---|---|
| 66db2a71-… | Barrow St & Hudson St | 0 | 1790755273 | 1790841673 |
| 66db2a71-… | Barrow St & Hudson St (TEST RENAMED) | 10 | 1790841673 | NULL |
| 2108372903577159978 | Union St & Bergen Ave | (as in snapshot 1) | 1790755273 | 1790841673 |

  2,521 rows in total: 2,519 current and 2 closed. A station that disappears from a snapshot has its
  row closed and no replacement.
- Cleanup: the synthetic file and its row in the dev reference table were deleted and the table was
  rebuilt with a full refresh of `dim_station_history` (2,520 rows, 0 test rows).

Not done: in prod the second real snapshot (`station_information_20261001T000011Z.json`) has landed
but has not been loaded into the prod reference table, because loading it needs a prod `setup_job`
run; with it, the first prod comparison of two real snapshots would be available. Whether any real
station changed between 30 September and 1 October is therefore not known.
