"""Contrato do evento `trips.raw`: campos, serialização, `trip_id` e injeção de sujos."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from producer.schema import (
    DIRTY_KINDS,
    EVENT_FIELDS,
    FARE_CEILING,
    build_dirty_event,
    build_event,
    dumps,
    make_trip_id,
    partition_key,
)

EMIT = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def row(**overrides: Any) -> dict[str, Any]:
    """Linha do Parquet com o mínimo que o produtor lê (nomes originais do TLC)."""
    base: dict[str, Any] = {
        "tpep_pickup_datetime": datetime(2024, 1, 1, 0, 12, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 0, 27, 0),
        "PULocationID": 114,
        "DOLocationID": 48,
        "fare_amount": 12.5,
        "tip_amount": 2.0,
        "total_amount": 17.3,
        "payment_type": 1,
        "trip_distance": 2.4,
    }
    base.update(overrides)
    return base


def test_event_fields_in_order() -> None:
    """O dict do evento segue `EVENT_FIELDS` — o teste que o Dia 3 deixou para hoje."""
    assert tuple(build_event(row(), EMIT)) == EVENT_FIELDS


def test_build_event_maps_row() -> None:
    event = build_event(row(), EMIT)

    assert event["event_time"] == "2024-01-01T00:12:00"
    assert event["dropoff_at"] == "2024-01-01T00:27:00"
    assert event["pickup_zone_id"] == 114
    assert event["dropoff_zone_id"] == 48
    assert event["fare_amount"] == 12.5
    assert event["total_amount"] == 17.3
    assert event["emit_time"] == "2024-01-01T12:00:00.000+00:00"
    assert event["trip_id"] == make_trip_id(
        "2024-01-01T00:12:00", "2024-01-01T00:27:00", 114, 48, 17.3
    )


def test_build_event_rejects_missing_pickup() -> None:
    with pytest.raises(ValueError):
        build_event(row(tpep_pickup_datetime=None), EMIT)


def test_nan_and_inf_become_null() -> None:
    event = build_event(
        row(fare_amount=float("nan"), tip_amount=float("inf"), total_amount=float("-inf")),
        EMIT,
    )

    assert event["fare_amount"] is None
    assert event["tip_amount"] is None
    assert event["total_amount"] is None
    assert tuple(event) == EVENT_FIELDS


def test_trip_id_is_deterministic_and_sensitive() -> None:
    args = ("2024-01-01T00:12:00", "2024-01-01T00:27:00", 114, 48, 17.3)

    assert make_trip_id(*args) == make_trip_id(*args)
    assert len(make_trip_id(*args)) == 16
    assert make_trip_id(*args) != make_trip_id(*args[:4], 17.31)
    assert make_trip_id(*args) != make_trip_id(*args[:2], None, 48, 17.3)


def test_trip_id_golden() -> None:
    """Regressão: mudar a fórmula trocaria todos os ids e quebraria a idempotência entre versões."""
    assert make_trip_id(
        "2024-01-01T00:12:00", "2024-01-01T00:27:00", 114, 48, 17.3
    ) == "bc7cdc0e1cdc29bd"


def test_dumps_is_compact_json() -> None:
    event = build_event(row(), EMIT)
    payload = dumps(event)

    assert b" " not in payload
    assert json.loads(payload) == event
    assert payload == dumps(event)


def test_partition_key() -> None:
    assert partition_key(build_event(row(), EMIT)) == b"114"
    assert partition_key(build_event(row(PULocationID=None), EMIT)) == b"unknown"


def test_dirty_events_cover_every_reason_and_stay_unique() -> None:
    base = build_event(row(), EMIT)
    dirty = [build_dirty_event(base, seq, EMIT) for seq in range(len(DIRTY_KINDS))]

    # contrato do tópico preservado (mesmos campos, mesma ordem, JSON válido)
    for event in dirty:
        assert tuple(event) == EVENT_FIELDS
        json.loads(dumps(event))

    # um `trip_id` por evento: sujo não pode virar "duplicata" no Silver
    ids = [event["trip_id"] for event in dirty]
    assert len(set(ids)) == len(ids)
    assert set(ids).isdisjoint({base["trip_id"]})

    # e cada um viola exatamente a regra que o nome promete
    by_kind = dict(zip(DIRTY_KINDS, dirty, strict=True))
    assert by_kind["pickup_zone_null"]["pickup_zone_id"] is None
    zero = by_kind["non_positive_duration"]
    assert zero["dropoff_at"] == zero["event_time"]
    assert by_kind["negative_fare"]["fare_amount"] < 0
    assert by_kind["fare_over_ceiling"]["total_amount"] > FARE_CEILING


def test_dirty_shift_stays_unique_across_the_dirty_kinds_cycle() -> None:
    base = build_event(row(), EMIT)
    first = build_dirty_event(base, 0, EMIT)  # pickup_zone_null
    fifth = build_dirty_event(base, 4, EMIT)  # mesmo motivo, outra sequência

    assert first["trip_id"] != fifth["trip_id"]
    assert datetime.fromisoformat(fifth["event_time"]) - datetime.fromisoformat(
        first["event_time"]
    ) == timedelta(seconds=4)


def test_dirty_handles_null_dropoff_without_mutating_base() -> None:
    base = build_event(row(tpep_dropoff_datetime=None), EMIT)
    snapshot = deepcopy(base)

    zero_duration = build_dirty_event(base, 1, EMIT)  # non_positive_duration
    assert zero_duration["dropoff_at"] == zero_duration["event_time"]
    assert build_dirty_event(base, 0, EMIT)["dropoff_at"] is None  # preserva o nulo
    assert base == snapshot
