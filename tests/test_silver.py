"""Ramo Silver sem broker e sem Postgres: invalidez, atraso, identidade do lote e graváveis.

O upsert/livro-caixa em si são cobertos pelo run de laboratório (o teste unitário não sobe Postgres).
Aqui ficam as decisões que podem quebrar em silêncio: qual regra invalida, qual atraso é descartado,
o que não tem como entrar na PK/NOT NULL, a serialização dos tempos e a identidade do lote.
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
# Aware de propósito: `createDataFrame` converte `datetime` naive no fuso da máquina, e o teste
# não pode depender do `TZ` de quem roda.
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
        # as cinco regras do contrato
        ({"trip_id": None}, False),
        ({"pickup_zone_id": None}, False),
        ({"dropoff_at": "2024-01-01T00:12:00"}, False),  # duração zero
        ({"dropoff_at": "2024-01-01T00:07:00"}, False),  # duração negativa
        ({"fare_amount": -0.01}, False),
        ({"total_amount": 10_000.01}, False),
        # limites e nulos: nulo não invalida (senão o nulo viraria falso negativo)
        ({"total_amount": 10_000.0}, True),
        ({"fare_amount": None}, True),
        ({"dropoff_at": None}, True),
        # o teto usa o total arredondado a 2 casas, igual ao `trip_id` do produtor (`%.2f`)
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

    # O texto é naive de NY: o COPY carrega o mesmo relógio de parede, sem deslocar.
    assert row.pickup_at == "2024-01-01 00:12:00.000000"
    assert row.event_time == row.pickup_at
    assert row.dropoff_at == "2024-01-01 00:27:00.000000"
    # emit_time tem offset e ingested_at é instante: os dois saem no relógio UTC da sessão.
    assert row.emit_time == "2024-01-01 12:00:00.000000"
    assert row.ingested_at == "2024-01-01 12:05:00.000000"
    assert lag == pytest.approx(300.0)


def test_first_batch_accepts_the_2002_outlier(spark) -> None:
    outlier = event(event_time="2002-12-31T23:00:00", dropoff_at="2002-12-31T23:10:00")
    prepared = to_silver_events(frame(spark, outlier))

    # Watermark NULL = aceita tudo; com o max do próprio lote o outlier morreria aqui.
    assert apply_late_filter(prepared, None).count() == 1


def test_late_filter_drops_only_more_than_ten_minutes_behind(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id="atrasado", event_time="2024-01-01T11:49:00"),
            event(trip_id="no_limite", event_time="2024-01-01T11:50:00"),
            event(trip_id="em_dia", event_time="2024-01-01T12:00:00"),
            event(trip_id="sem_tempo", event_time=None),
        )
    )

    kept = {
        row.trip_id
        for row in apply_late_filter(prepared, datetime(2024, 1, 1, 12, 0)).collect()
    }
    # Linha sem event_time não é julgada pelo atraso; ela cai no filtro de graváveis.
    assert kept == {"no_limite", "em_dia", "sem_tempo"}


def test_late_and_unstorable_is_counted_once(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id=None, event_time="2024-01-01T11:00:00"),
            event(trip_id="em_dia", event_time="2024-01-01T12:00:00"),
        )
    )

    accepted = apply_late_filter(prepared, datetime(2024, 1, 1, 12, 0))
    storable = storable_filter(accepted)

    # A linha atrasada sai antes do filtro de graváveis: conta como late, não como rejeitada.
    assert accepted.count() == 1
    assert storable.count() == 1


def test_rows_without_key_or_not_null_columns_are_not_storable(spark) -> None:
    prepared = to_silver_events(
        frame(
            spark,
            event(trip_id="ok"),
            event(trip_id=None),
            event(trip_id="sem_pickup", event_time=None),
            event(trip_id="sem_emit", emit_time=None),
            event(trip_id="com_nul\x00aqui"),
            event(trip_id="com\ttab"),
        )
    )

    # PK/NOT NULL ausentes e NUL (que o COPY recusa) não podem entrar: contam em `rejected`.
    # TAB/quebra de linha o psycopg escapa, então continuam graváveis.
    assert storable_filter(prepared).count() == 2


def test_invalid_row_with_key_is_still_storable(spark) -> None:
    prepared = to_silver_events(frame(spark, event(fare_amount=-5.0)))

    row = prepared.collect()[0]
    assert row.is_valid_trip is False
    # Inválido é dado: entra no Silver para a reconciliação; só quem não cabe na PK/NOT NULL sai.
    assert storable_filter(prepared).count() == 1


def test_malformed_timestamp_does_not_raise(spark) -> None:
    # ANSI ligado no Spark 4: `to_timestamp` levantaria exceção e travaria o lote; `try_` devolve nulo.
    prepared = to_silver_events(frame(spark, event(event_time="nao-e-data")))
    row = prepared.collect()[0]

    assert row.pickup_at is None
    assert storable_filter(prepared).count() == 0


def test_out_of_range_amount_is_invalid_not_fatal(spark) -> None:
    # Acima do que o NUMERIC(10,2) aceita: o valor vira NULL, mas a linha não pode sair como válida.
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
    # A partição 1 é a mais atrasada em event-time: é ela quem define o watermark.
    assert watermark == "2024-01-01 11:00:00.000000"


def test_batch_bounds_without_timestamps_has_no_watermark(spark) -> None:
    batch = spark.createDataFrame(
        [(0, 100, None), (1, 200, "nao-e-data")],
        "kafka_partition int, kafka_offset long, event_time string",
    )

    key, watermark = batch_bounds(batch)

    assert key == "0:100-100;1:200-200"
    assert watermark is None
