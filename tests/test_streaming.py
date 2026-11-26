"""Parse do Bronze sem broker: contrato do schema, JSON corrompido e partição em UTC.

Não sobe Kafka: `parse_events` recebe o DataFrame que o `readStream` produziria (`value`,
`partition`, `offset`). O que se testa aqui é o que pode quebrar em silêncio — ordem das colunas,
tipo do `payment_type`, mensagem corrompida e partição dependente de fuso.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from producer.schema import EVENT_FIELDS
from streaming.job import add_ingest_columns, jar_list, parse_events
from streaming.schema import BRONZE_COLUMNS, EVENT_SCHEMA

EVENT = {
    "trip_id": "bc7cdc0e1cdc29bd",
    "event_time": "2024-01-01T00:12:00",
    "dropoff_at": "2024-01-01T00:27:00",
    "pickup_zone_id": 114,
    "dropoff_zone_id": 48,
    "fare_amount": 12.5,
    "tip_amount": 2.0,
    "total_amount": 17.3,
    "payment_type": 1,
    "trip_distance": 2.4,
    "emit_time": "2024-01-01T12:00:00.000+00:00",
}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-streaming")
        # Mesmo fuso do job: a partição do Bronze é UTC (ver add_ingest_columns).
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def topic_frame(spark: SparkSession, payloads: list[bytes]):
    """O que o conector Kafka entrega: `value` binário + posição no tópico."""
    rows = [(payload, 3, 100 + i) for i, payload in enumerate(payloads)]
    return spark.createDataFrame(rows, "value binary, partition int, offset long")


def test_schema_follows_the_producer_contract() -> None:
    assert tuple(field.name for field in EVENT_SCHEMA.fields) == EVENT_FIELDS
    assert BRONZE_COLUMNS[: len(EVENT_FIELDS)] == EVENT_FIELDS
    assert BRONZE_COLUMNS[-3:] == ("ingested_at", "ingest_date", "hour")


def test_parse_events_keeps_values_and_order(spark: SparkSession) -> None:
    frame = parse_events(topic_frame(spark, [json.dumps(EVENT).encode()]))
    row = frame.collect()[0]

    assert frame.columns == list(EVENT_FIELDS) + ["kafka_partition", "kafka_offset"]
    assert row.trip_id == EVENT["trip_id"]
    # Timestamp fica texto no Bronze: quem interpreta o fuso é o Silver.
    assert row.event_time == EVENT["event_time"]
    assert row.emit_time == EVENT["emit_time"]
    assert row.pickup_zone_id == 114
    assert row.payment_type == pytest.approx(1.0)
    assert (row.kafka_partition, row.kafka_offset) == (3, 100)


def test_parse_events_turns_corrupt_json_into_nulls(spark: SparkSession) -> None:
    frame = parse_events(topic_frame(spark, [b'{"trip_id": "x", ']))
    row = frame.collect()[0]

    # PERMISSIVE: a linha entra (Bronze cru) e o trip_id nulo é invalidado no Silver.
    assert row.trip_id is None
    assert row.kafka_offset == 100


def test_bronze_columns_and_utc_partitions(spark: SparkSession) -> None:
    frame = add_ingest_columns(parse_events(topic_frame(spark, [json.dumps(EVENT).encode()])))
    assert frame.columns == list(BRONZE_COLUMNS)

    row = frame.collect()[0]
    now = datetime.now(UTC)
    allowed = {
        (now.date(), now.hour),
        ((now - timedelta(hours=1)).date(), (now - timedelta(hours=1)).hour),
    }
    assert (row.ingest_date, row.hour) in allowed
    assert row.ingested_at is not None


def test_add_ingest_columns_refuses_non_utc_session(spark: SparkSession) -> None:
    original = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        with pytest.raises(ValueError, match="UTC"):
            add_ingest_columns(parse_events(topic_frame(spark, [json.dumps(EVENT).encode()])))
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def test_jar_list_is_empty_before_download(tmp_path) -> None:
    assert jar_list(tmp_path) == []
