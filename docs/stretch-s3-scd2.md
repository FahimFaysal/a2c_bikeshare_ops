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

## Prod result (2026-10-01)

After the merge of the stretch pull requests, `setup_job` loaded the second real snapshot
(`station_information_20261001T000011Z.json`) into `prd_lakehouse.ref_station_information` (two
snapshots, versions `1790767813` and `1790812803`) and the prod pipeline was refreshed (update
`d00369`, COMPLETED). `dim_station_history` holds 2,529 rows for 2,520 stations: 2,520 current and 9
closed. **Nine real stations changed between 30 September and 1 October**, all in capacity, for example:

| Station | Capacity before | Capacity after |
|---|---|---|
| 1 Ave & E 42 St | 0 | 62 |
| 51 Ave & Van Loon St | 0 | 24 |
| 8 Ave & W 55 St | 71 | 33 |
| W 17 St & 7 Ave | 47 | 17 |
| 2 Ave & E 122 St | 18 | 25 |
| Cleveland Pl & Spring St | 16 | 33 |

Names and coordinates did not change. Several "before" values are 0, which the feed also shows for
many stations in the first snapshot; whether that is a real capacity or a placeholder in the feed was
not investigated, so those rows show that the flow detects a change, not what the true capacity was.

The same prod refresh was the first run of the merged `silver.py` (now importing `transforms.py`):
`silver_trips` is still 55,774,718 rows with 0 duplicate keys, and reconciliation still shows Bronze
55,789,305 = Silver clean + quarantine with no row not OK, so the refactor changed no result.
