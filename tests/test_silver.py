"""Silver branch without broker or Postgres: invalidity, late data, batch identity and storable rows.

The upsert/ledger itself is covered by the lab run (the unit test does not start Postgres). What
stays here are the decisions that can break silently: which rule invalidates, which lateness is
dropped, what cannot enter the PK/`NOT NULL`, the timestamp serialization and the batch identity.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from producer.schema import EVENT_FIELDS
from streaming.schema import EVENT_SCHEMA
from streaming.silver import (
    SILVER_COLUMNS,
    apply_late_filter,
    batch_bounds,
    copy_frame,
    storable_filter,
    to_silver_events,
)

EVENT = {
    "trip_id": "bc7cdc0e1cdc29bd",
    "event_time": "2024-01-01T00:12:00",
    "dropoff_at": "2024-01-01T00:27:00",
    "pickup_zone_id": 114,
    "dropoff_zone_id": 48,
    "fare_amount": 12.5,
    "tip_amount": 2.0,
    "total_amount": 17.3,
    "payment_type": 1.0,
    "trip_distance": 2.4,
    "emit_time": "2024-01-01T12:00:00.000+00:00",
}
# Aware on purpose: `createDataFrame` converts naive `datetime` in the machine timezone, and the
# test cannot depend on the runner's `TZ`.
INGESTED_AT = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

INPUT_SCHEMA = T.StructType(
    [T.StructField(name, EVENT_SCHEMA[name].dataType, True) for name in EVENT_FIELDS]
    + [T.StructField("ingested_at", T.TimestampType(), True)]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def event(**overrides) -> dict:
    merged = dict(EVENT)
    merged.update(overrides)
    return merged


def frame(spark: SparkSession, *events: dict):
    rows = [tuple(item[name] for name in EVENT_FIELDS) + (INGESTED_AT,) for item in events]
    return spark.createDataFrame(rows, INPUT_SCHEMA)


@pytest.mark.parametrize(
    ("overrides", "valid"),
    [
        ({}, True),
        # the five contract rules
        ({"trip_id": None}, False),
        ({"pickup_zone_id": None}, False),
        ({"dropoff_at": "2024-01-01T00:12:00"}, False),  # zero duration
        ({"dropoff_at": "2024-01-01T00:07:00"}, False),  # negative duration
        ({"fare_amount": -0.01}, False),
        ({"total_amount": 10_000.01}, False),
        # limits and nulls: a null does not invalidate (otherwise the null would be a false negative)
        ({"total_amount": 10_000.0}, True),
        ({"fare_amount": None}, True),
        ({"dropoff_at": None}, True),
        # the ceiling uses the total rounded to 2 decimals, same as the producer `trip_id` (`%.2f`)
        ({"total_amount": 10_000.004}, True),
        ({"total_amount": 10_000.006}, False),
    ],
)
def test_validation_flags_the_five_rules(spark, overrides, valid) -> None:
    row = to_silver_events(frame(spark, event(**overrides))).collect()[0]
    assert row.is_valid_trip == valid


def test_silver_columns_follow_the_copy_contract(spark) -> None:
    assert to_silver_events(frame(spark, event())).columns == list(SILVER_COLUMNS)
    assert copy_frame(to_silver_events(frame(spark, event()))).columns == list(SILVER_COLUMNS)


def test_timestamps_keep_the_naive_new_york_wall_clock(spark) -> None:
    prepared = to_silver_events(frame(spark, event()))
    lag = prepared.collect()[0].lag_seconds
    row = copy_frame(prepared).collect()[0]

    # The text is New York naive: the COPY carries the same wall clock, without shifting.
    assert row.pickup_at == "2024-01-01 00:12:00.000000"
    assert row.event_time == row.pickup_at
    assert row.dropoff_at == "2024-01-01 00:27:00.000000"
    # emit_time has an offset and ingested_at is an instant: both come out in the session UTC clock.
    assert row.emit_time == "2024-01-01 12:00:00.000000"
    assert row.ingested_at == "2024-01-01 12:05:00.000000"
    assert lag == pytest.approx(300.0)


def test_first_batch_accepts_the_2002_outlier(spark) -> None:
    outlier = event(event_time="2002-12-31T23:00:00", dropoff_at="2002-12-31T23:10:00")
    prepared = to_silver_events(frame(spark, outlier))

    # NULL watermark = accept everything; with the batch's own max the outlier would die here.
    assert apply_late_filter(prepared, None).count() == 1


def test_late_filter_drops_only_more_than_ten_minutes_behind(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id="late_event", event_time="2024-01-01T11:49:00"),
            event(trip_id="on_the_edge", event_time="2024-01-01T11:50:00"),
            event(trip_id="on_time", event_time="2024-01-01T12:00:00"),
            event(trip_id="no_time", event_time=None),
        )
    )

    kept = {
        row.trip_id
        for row in apply_late_filter(prepared, datetime(2024, 1, 1, 12, 0)).collect()
    }
    # A row without event_time is not judged by lateness; it falls into the storable filter.
    assert kept == {"on_the_edge", "on_time", "no_time"}


def test_late_and_unstorable_is_counted_once(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id=None, event_time="2024-01-01T11:00:00"),
            event(trip_id="on_time", event_time="2024-01-01T12:00:00"),
        )
    )

    accepted = apply_late_filter(prepared, datetime(2024, 1, 1, 12, 0))
    storable = storable_filter(accepted)

    # The late row leaves before the storable filter: it counts as late, not as rejected.
    assert accepted.count() == 1
    assert storable.count() == 1


def test_rows_without_key_or_not_null_columns_are_not_storable(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id="ok"),
            event(trip_id=None),
            event(trip_id="no_pickup", event_time=None),
            event(trip_id="no_emit", emit_time=None),
            event(trip_id="with_nul\x00here"),
            event(trip_id="with\ttab"),
        )
    )

    # Missing PK/NOT NULL and NUL (which the COPY refuses) cannot enter: they count as `rejected`.
    # TAB/newline are escaped by psycopg, so they remain storable.
    assert storable_filter(prepared).count() == 2


def test_invalid_row_with_key_is_still_storable(spark) -> None:
    prepared = to_silver_events(frame(spark, event(fare_amount=-5.0)))

    row = prepared.collect()[0]
    assert row.is_valid_trip is False
    # Invalid is data: it enters the Silver for the reconciliation; only what cannot fit the
    # PK/NOT NULL leaves.
    assert storable_filter(prepared).count() == 1


def test_malformed_timestamp_does_not_raise(spark) -> None:
    # ANSI on in Spark 4: `to_timestamp` would raise and stall the batch; `try_` returns null.
    prepared = to_silver_events(frame(spark, event(event_time="not-a-date")))
    row = prepared.collect()[0]

    assert row.pickup_at is None
    assert storable_filter(prepared).count() == 0


def test_out_of_range_amount_is_invalid_not_fatal(spark) -> None:
    # Above what NUMERIC(10,2) accepts: the value becomes NULL, but the row cannot leave as valid.
    prepared = to_silver_events(frame(spark, event(fare_amount=1e20)))
    row = prepared.collect()[0]

    assert row.fare_amount is None
    assert row.is_valid_trip is False


def test_batch_bounds_identity_and_slowest_partition(spark) -> None:
    batch = spark.createDataFrame(
        [
            (0, 100, "2024-01-01T10:00:00"),
            (0, 101, "2024-01-01T12:00:00"),
            (1, 200, "2024-01-01T09:00:00"),
            (1, 201, "2024-01-01T11:00:00"),
        ],
        "kafka_partition int, kafka_offset long, event_time string",
    )

    key, watermark = batch_bounds(batch)

    assert key == "0:100-101;1:200-201"
    # Partition 1 is the most behind in event-time: it defines the watermark.
    assert watermark == "2024-01-01 11:00:00.000000"


def test_batch_bounds_without_timestamps_has_no_watermark(spark) -> None:
    batch = spark.createDataFrame(
        [(0, 100, None), (1, 200, "not-a-date")],
        "kafka_partition int, kafka_offset long, event_time string",
    )

    key, watermark = batch_bounds(batch)

    assert key == "0:100-100;1:200-200"
    assert watermark is None
