"""Unit tests for the trip transformations used by the Silver layer (src/pipeline/transforms.py)."""
from chispa import assert_df_equality
from pyspark.sql import functions as F

from transforms import ALL_DROP, DROP_RULES, WARN_RULES, add_derived, conform

BRONZE = ("ride_id string, rideable_type string, started_at string, ended_at string, "
          "start_station_name string, start_station_id string, end_station_name string, end_station_id string, "
          "start_lat double, start_lng double, end_lat double, end_lng double, member_casual string, _source_file string")


def bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE)


def trip(ride_id="r1", started="2025-02-03 08:00:00", ended="2025-02-03 08:10:00", start_id="5329.03",
         end_id="6140.05", member="member", source="NYC_2025-02_1.csv", lat=40.73, lng=-73.99):
    return (ride_id, "classic_bike", started, ended, "A", start_id, "B", end_id, lat, lng, lat, lng, member, source)


def silver(spark, *rows):
    return add_derived(conform(bronze(spark, *rows)))


def test_station_ids_stay_text(spark):
    out = conform(bronze(spark, trip(start_id="5329.03", end_id="JC115"))).first()
    assert out.start_station_id == "5329.03"          # not 5329.03 as a double, not 5329.030000001
    assert out.end_station_id == "JC115"
    assert dict(conform(bronze(spark, trip())).dtypes)["start_station_id"] == "string"


def test_system_comes_from_the_file_name(spark):
    out = conform(bronze(spark, trip(source="JC_2025-02_1.csv"), trip(source="NYC_2025-02_1.csv")))
    assert [r.system for r in out.collect()] == ["JC", "NYC"]


def test_unparseable_timestamp_becomes_null_instead_of_failing(spark):
    out = conform(bronze(spark, trip(started="not a date"))).first()
    assert out.started_at is None                      # ANSI mode would raise with a plain cast


def test_derived_columns(spark):
    row = silver(spark, trip(started="2025-02-03 08:00:00", ended="2025-02-03 08:12:30")).first()
    assert row.duration_min == 12.5
    assert str(row.start_date) == "2025-02-03" and row.start_hour == 8 and row.start_dow == "Mon"
    assert row.period == "2025-02" and row.file_period == "2025-02"
    assert row._row_key == "r1"


def test_round_trip_flag(spark):
    out = silver(spark, trip(ride_id="a", start_id="1", end_id="1"), trip(ride_id="b", start_id="1", end_id="2"))
    assert {r.ride_id: r.is_round_trip for r in out.collect()} == {"a": True, "b": False}


def test_conform_keeps_one_row_per_input_row(spark):
    df = bronze(spark, trip(ride_id="a"), trip(ride_id="b"), trip(ride_id="c", started="bad"))
    assert conform(df).count() == df.count() == 3


def passes(spark, **kwargs):
    df = silver(spark, trip(**kwargs))
    return df.where(ALL_DROP).count() == 1


def test_a_normal_trip_passes_every_drop_rule(spark):
    assert passes(spark)


def test_trip_ending_before_it_starts_is_dropped(spark):
    assert not passes(spark, started="2025-02-03 09:00:00", ended="2025-02-03 08:00:00")


def test_duration_limits(spark):
    assert not passes(spark, ended="2025-02-03 08:00:30")            # 30 seconds
    assert passes(spark, ended="2025-02-03 08:01:00")                # exactly the 1 minute floor
    assert passes(spark, ended="2025-02-04 08:00:00")                # exactly 24 hours
    assert not passes(spark, ended="2025-02-04 08:00:01")            # one second over


def test_unknown_rider_type_is_dropped(spark):
    assert not passes(spark, member="subscriber")


def test_null_values_fail_closed(spark):
    # a NULL must fail a drop rule (so the row is quarantined), never slip through
    assert not passes(spark, ride_id=None)
    assert not passes(spark, started="not a date")
    assert not passes(spark, member=None)


def test_quarantine_is_exact_complement_of_clean(spark):
    df = silver(spark, trip(ride_id="ok"), trip(ride_id="short", ended="2025-02-03 08:00:10"),
                trip(ride_id=None), trip(ride_id="bad", started="nope"), trip(ride_id="rider", member="x"))
    clean, quarantine = df.where(ALL_DROP).count(), df.where(f"NOT ({ALL_DROP})").count()
    assert clean + quarantine == df.count() == 5      # the reconciliation identity, row for row
    assert clean == 1


def test_warn_rules_keep_the_row_but_flag_it(spark):
    df = silver(spark, trip(ride_id="no_station", end_id=None), trip(ride_id="far", lat=34.0, lng=-118.0),
                trip(ride_id="month", started="2025-01-31 23:55:00", ended="2025-02-01 00:10:00"))
    assert df.where(ALL_DROP).count() == 3            # none of them is dropped
    failed = {name: [r.ride_id for r in df.where(f"NOT coalesce({rule}, false)").collect()]
              for name, rule in WARN_RULES.items()}
    assert failed["stations_present"] == ["no_station"]
    assert failed["coords_in_area"] == ["far"]
    assert failed["period_matches_file"] == ["month"]


def test_rules_are_the_documented_ones():
    assert set(DROP_RULES) == {"ride_id_present", "ended_after_started", "duration_1min_to_24h", "member_type_valid"}
    assert set(WARN_RULES) == {"period_matches_file", "coords_in_area", "stations_present"}
    assert all(rule.startswith("coalesce(") for rule in DROP_RULES.values())


def test_chispa_frame_equality(spark):
    out = silver(spark, trip(ride_id="x")).select("ride_id", "system", "duration_min", "is_round_trip")
    expected = spark.createDataFrame([("x", "NYC", 10.0, False)],
                                     "ride_id string, system string, duration_min double, is_round_trip boolean")
    assert_df_equality(out, expected, ignore_nullable=True)   # CASE WHEN columns are non-nullable
