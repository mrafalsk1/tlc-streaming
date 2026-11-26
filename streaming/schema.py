"""Explicit schema of the event consumed from `trips.raw`.

The topic carries JSON without a Schema Registry (Avro was left out of scope), so the producer is the
contract definition and this file is the only other end: no `inferSchema`, which on a small sample
picks the wrong type and turns `pickup_zone_id` into `string` just because one batch came out null.

The names come from `producer.schema.EVENT_FIELDS` exactly so the two ends cannot diverge silently
(a test pins the order). Both timestamps stay **text** in Bronze on purpose: it is the raw layer, and
converting "2024-01-01T00:12:00" (naive, New York local time) into a timestamp would already pick a
timezone at ingest. The side that interprets the clock is the Silver, with the timezone fixed in the
job.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from producer.schema import EVENT_FIELDS

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True),
        StructField("dropoff_at", StringType(), nullable=True),
        StructField("pickup_zone_id", IntegerType(), nullable=True),
        StructField("dropoff_zone_id", IntegerType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("payment_type", DoubleType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("emit_time", StringType(), nullable=True),
    ]
)

if tuple(field.name for field in EVENT_SCHEMA.fields) != EVENT_FIELDS:
    raise RuntimeError(
        "EVENT_SCHEMA diverged from the producer contract: "
        f"{tuple(f.name for f in EVENT_SCHEMA.fields)} != {EVENT_FIELDS}"
    )

# Columns Bronze adds to the event, in the order they come out in the Parquet. `ingest_date`/`hour`
# are partition columns derived in UTC (see `streaming.job.add_ingest_columns`).
BRONZE_COLUMNS: tuple[str, ...] = EVENT_FIELDS + (
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
    "ingest_date",
    "hour",
)
