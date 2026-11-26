"""Explicit `trips.raw` event schema and deterministic `trip_id` generation.

The TLC dataset has no trip id, so the id is derived from the event content
(`sha1(pickup|dropoff|pu_zone|do_zone|total)[:16]`): replaying again produces the same ids, which is
what makes the Silver idempotency provable.

Two timestamps, on purpose (see `docs/notes.md`):

* ``event_time`` — pickup in New York local time (the TLC publishes a naive timestamp); drives the
  window and the watermark. It is the `pickup_at` of the dataset; the Silver job derives
  `pickup_at` from it instead of repeating the same column in the JSON.
* ``emit_time`` — wall clock in UTC at send time; the base of `lag_seconds`.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

# Event fields, in the order they appear in the JSON (the topic contract, as documentation).
EVENT_FIELDS: tuple[str, ...] = (
    "trip_id",
    "event_time",
    "dropoff_at",
    "pickup_zone_id",
    "dropoff_zone_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_distance",
    "emit_time",
)

# Columns read from the Parquet. The event is deliberately lean: Bronze stores the stream event, not
# a mirror of the Parquet (extra, mta_tax, congestion_surcharge etc. are left out).
SOURCE_COLUMNS: tuple[str, ...] = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_distance",
)


def format_naive(value: datetime | None) -> str | None:
    """Business timestamp (naive, New York local time) in ISO 8601, with seconds."""
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def as_number(value: Any) -> float | int | None:
    """Turn NaN/Inf into ``None``: JSON has no such values (Spark's Jackson would reject them)."""
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def as_zone(value: Any) -> int | None:
    """TLC zone (`PULocationID`/`DOLocationID`): integer or ``None`` (the Parquet uses NaN)."""
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value)
    return int(value)


def make_trip_id(
    event_time: str,
    dropoff_at: str | None,
    pickup_zone_id: int | None,
    dropoff_zone_id: int | None,
    total_amount: float | None,
) -> str:
    """``sha1`` of the five identity fields, as stable text (same input ⇒ same id)."""
    parts = (
        event_time,
        dropoff_at or "",
        "" if pickup_zone_id is None else str(pickup_zone_id),
        "" if dropoff_zone_id is None else str(dropoff_zone_id),
        "" if total_amount is None else f"{total_amount:.2f}",
    )
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def build_event(row: Mapping[str, Any], emit_time: datetime) -> dict[str, Any]:
    """Convert a Parquet row into the topic event.

    ``row`` uses the original TLC names. Raises ``ValueError`` when there is no pickup: without
    ``event_time`` the row has no order and no window, so it does not enter the stream (the producer
    counts it).
    """
    event_time = format_naive(row.get("tpep_pickup_datetime"))
    if event_time is None:
        raise ValueError("tpep_pickup_datetime is missing")

    dropoff_at = format_naive(row.get("tpep_dropoff_datetime"))
    pickup_zone_id = as_zone(row.get("PULocationID"))
    dropoff_zone_id = as_zone(row.get("DOLocationID"))
    total_amount = as_number(row.get("total_amount"))

    return {
        "trip_id": make_trip_id(
            event_time,
            dropoff_at,
            pickup_zone_id,
            dropoff_zone_id,
            total_amount,
        ),
        "event_time": event_time,
        "dropoff_at": dropoff_at,
        "pickup_zone_id": pickup_zone_id,
        "dropoff_zone_id": dropoff_zone_id,
        "fare_amount": as_number(row.get("fare_amount")),
        "tip_amount": as_number(row.get("tip_amount")),
        "total_amount": total_amount,
        "payment_type": as_number(row.get("payment_type")),
        "trip_distance": as_number(row.get("trip_distance")),
        "emit_time": emit_time.isoformat(timespec="milliseconds"),
    }


def partition_key(event: Mapping[str, Any]) -> bytes:
    """Message key = pickup zone; a null zone becomes ``unknown`` (never a null key)."""
    zone = event["pickup_zone_id"]
    return b"unknown" if zone is None else str(zone).encode("ascii")


def dumps(event: Mapping[str, Any]) -> bytes:
    """Compact serialization (no spaces) of the event."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# Invalidity reasons the Silver will refuse (see docs/notes.md, modeling decisions). A null
# `trip_id` is not listed: the producer always generates an id — that case only exists if
# deserialization fails.
DIRTY_KINDS: tuple[str, ...] = (
    "pickup_zone_null",
    "non_positive_duration",
    "negative_fare",
    "fare_over_ceiling",
)

# Fare sanity ceiling (Silver policy). Above this the event is born invalid.
FARE_CEILING = 10_000.0


def build_dirty_event(
    base: Mapping[str, Any], seq: int, emit_time: datetime
) -> dict[str, Any]:
    """Synthetic event the Silver must mark as ``is_valid_trip = false``.

    Derived from the last real event, with ``event_time`` shifted ``seq + 1`` seconds: the
    ``trip_id`` comes out unique (it does not become a duplicate) and the event stays close to the
    stream position, without falling into the late filter. The reason cycles through
    ``DIRTY_KINDS``, so an injection of 4 already covers every invalidity rule.
    """
    kind = DIRTY_KINDS[seq % len(DIRTY_KINDS)]
    shift = timedelta(seconds=seq + 1)
    event_time = datetime.fromisoformat(base["event_time"]) + shift
    dropoff_at = base["dropoff_at"]
    if dropoff_at is not None:
        dropoff_at = format_naive(datetime.fromisoformat(dropoff_at) + shift)

    event = dict(base)
    event["event_time"] = format_naive(event_time)
    event["dropoff_at"] = dropoff_at
    event["emit_time"] = emit_time.isoformat(timespec="milliseconds")

    if kind == "pickup_zone_null":
        event["pickup_zone_id"] = None
    elif kind == "non_positive_duration":
        event["dropoff_at"] = event["event_time"]
    elif kind == "negative_fare":
        event["fare_amount"] = -1.0 * (seq + 1)
        event["total_amount"] = -1.0 * (seq + 1)
    else:  # fare_over_ceiling
        event["total_amount"] = FARE_CEILING + seq + 1

    event["trip_id"] = make_trip_id(
        event["event_time"],
        event["dropoff_at"],
        event["pickup_zone_id"],
        event["dropoff_zone_id"],
        event["total_amount"],
    )
    return event
