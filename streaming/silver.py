"""Silver branch: validation, late filter and idempotent upsert into Postgres.

Bronze accepts everything; this is where the event is classified. The order is the contract one
(docs/notes.md): validation → late filter → upsert. Deduplication is **not** stateful: the Postgres
`PRIMARY KEY (trip_id)` does it, so the Spark state does not grow with the stream.

Six decisions that are not obvious:

* **Watermark from the previous batch, per partition.** The filter reads
  `silver.stream_state.watermark`, written by the previous batch, and only then writes this batch's
  watermark. The value is the **lowest** `max(event_time)` across Kafka partitions: the replay is
  keyed by zone and the per-trigger offset cap is split evenly across partitions, so they advance at
  different event-time rates (in an 8k batch the slices differed by up to ~6 h). Using the global max
  would make the fastest partition call "late" rows that are just the others' turn. The lowest max
  is the point every input has passed — the classic watermark. Using the current batch's own max
  would make the first replay event (the 2002 outlier) late right away. NULL = first batch, accepts
  everything. The update uses `GREATEST`: the watermark never goes back.
* **`is_valid_trip` with three values.** Comparison with null returns null, and a null written into a
  `NOT NULL` column kills the batch. So each rule is a `coalesce(..., false)` and the result is
  negated: a null `fare_amount` does not invalidate the row. The ceiling compares the value
  **rounded** to 2 decimals, the same the producer's `trip_id` uses (`%.2f`).
* **Counters come from the upsert, in a database ledger.** `ON CONFLICT DO UPDATE` counts inserts
  and updates alike; `RETURNING (xmax = 0)` separates them. `invalid` = rows stored with
  `is_valid_trip = false`; `duplicates_dropped` = rows the upsert did not insert (a duplicate inside
  the batch or a conflict with an existing row). The duplicate of an invalid event counts only as a
  duplicate. The batch result goes to `silver.batch_runs` in the same transaction, keyed by the
  batch identity (offset ranges per partition): a batch re-executed by the checkpoint is not counted
  twice, and `data/state/counters.json` is the table sum.
* **Time leaves Spark as text.** `collect()` converts `TIMESTAMP` to the **driver's local** timezone
  (not `spark.sql.session.timeZone`), which would shift the New York wall clock and the instants by
  3 h on a UTC-3 machine. `copy_frame` formats in the session timezone (UTC) and Postgres receives
  the string with the session in UTC: nothing is reinterpreted on the way.
* **A row that does not fit the destination must not kill the pipeline.** A null `trip_id` (corrupt
  JSON) is invalid but has no PK; null `pickup_at`/`emit_time` violate `NOT NULL`; a `trip_id` with
  NUL cannot cross the COPY (psycopg refuses it, even though it escapes TAB/newline). Those cases
  count as `rejected` and stay in Bronze.
* **The zone window is recomputed from the table, not aggregated from the batch.** The aggregates in
  `silver.zone_demand_5min` are recomputed (same transaction) for the windows the batch touched,
  counting `silver.trip_events` rows. A batch-local aggregate in Spark cannot be both correct and
  retry-safe: replacing a window's value erases the previous batches, and adding to it counts twice
  when the checkpoint replays the batch. Recomputing from the source makes a replay a rewrite of the
  same numbers and lets a late event inside the tolerance (which lands in an old window) update the
  window it belongs to. The cost is two scans per touched window (the recompute and the check), not
  a growing state store. A transaction-level advisory lock serializes the window work between
  writers, and the batch asserts `sum(trips) == count(valid trips)` for the windows it touched before
  committing.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from producer.schema import FARE_CEILING

LATE_TOLERANCE = timedelta(minutes=10)
DEFAULT_DSN = os.environ.get(
    "SILVER_DSN", "postgresql://stream:stream@localhost:5434/streaming"
)
DEFAULT_COUNTERS_PATH = "data/state/counters.json"
# Format that crosses Spark → Postgres. Microseconds so `ingested_at` is not truncated.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

# Order = column order in the COPY and the INSERT. `event_time` and `pickup_at` hold the same value:
# the table keeps both names for the zone window and for the reconciliation.
SILVER_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "pickup_at",
    "dropoff_at",
    "pickup_zone_id",
    "dropoff_zone_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "is_valid_trip",
    "event_time",
    "emit_time",
    "ingested_at",
    "lag_seconds",
)

COUNTER_COLUMNS: tuple[str, ...] = (
    "consumed",
    "valid_stored",
    "invalid",
    "late_dropped",
    "duplicates_dropped",
    "rejected",
)

_COLUMNS_SQL = ", ".join(SILVER_COLUMNS)
_STAGING = "silver_batch"

_CREATE_STAGING_SQL = f"""
CREATE TEMP TABLE {_STAGING} (
    trip_id          TEXT,
    pickup_at        TIMESTAMP,
    dropoff_at       TIMESTAMP,
    pickup_zone_id   INTEGER,
    dropoff_zone_id  INTEGER,
    fare_amount      NUMERIC(10, 2),
    tip_amount       NUMERIC(10, 2),
    total_amount     NUMERIC(10, 2),
    payment_type     SMALLINT,
    is_valid_trip    BOOLEAN,
    event_time       TIMESTAMP,
    emit_time        TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ,
    lag_seconds      DOUBLE PRECISION
) ON COMMIT DROP
"""

# `DISTINCT ON` collapses a duplicate that arrives inside the same batch: `ON CONFLICT DO UPDATE`
# refuses to affect the same row twice in the same statement (unlike `DO NOTHING`).
_UPSERT_SQL = f"""
WITH incoming AS (
    SELECT DISTINCT ON (trip_id) {_COLUMNS_SQL}
    FROM {_STAGING}
    ORDER BY trip_id
)
INSERT INTO silver.trip_events ({_COLUMNS_SQL})
SELECT {_COLUMNS_SQL} FROM incoming
ON CONFLICT (trip_id) DO UPDATE SET
    pickup_at       = EXCLUDED.pickup_at,
    dropoff_at      = EXCLUDED.dropoff_at,
    pickup_zone_id  = EXCLUDED.pickup_zone_id,
    dropoff_zone_id = EXCLUDED.dropoff_zone_id,
    fare_amount     = EXCLUDED.fare_amount,
    tip_amount      = EXCLUDED.tip_amount,
    total_amount    = EXCLUDED.total_amount,
    payment_type    = EXCLUDED.payment_type,
    is_valid_trip   = EXCLUDED.is_valid_trip,
    event_time      = EXCLUDED.event_time,
    emit_time       = EXCLUDED.emit_time,
    ingested_at     = EXCLUDED.ingested_at,
    lag_seconds     = EXCLUDED.lag_seconds
RETURNING (xmax = 0) AS inserted, is_valid_trip
"""

# Zone x 5-minute window, recomputed for the windows this batch touched. Tumbling boundaries are
# aligned to the epoch (`date_bin`, the same boundary Spark's `window()` uses because 5 min divides
# the hour), so a window never drifts between batches. `LEFT JOIN` keeps a touched window that ended
# with zero valid rows at `trips = 0` instead of leaving the old value behind. The aggregate reads
# `trip_events`, never the staging rows: see the last decision of the module.
#
# The window set is shared by the recompute and the assertion below, so the two cannot drift.
_TOUCHED_WINDOWS_SQL = f"""
touched AS (
    SELECT DISTINCT
        pickup_zone_id,
        date_bin('5 minutes', pickup_at, TIMESTAMP '2000-01-01 00:00:00') AS window_start
    FROM {_STAGING}
    WHERE pickup_zone_id IS NOT NULL AND pickup_at IS NOT NULL
)
"""

_WINDOW_SQL = f"""
WITH {_TOUCHED_WINDOWS_SQL}
INSERT INTO silver.zone_demand_5min
    (pickup_zone_id, window_start, window_end, trips, revenue, avg_duration_min, updated_at)
SELECT
    t.pickup_zone_id,
    t.window_start,
    t.window_start + interval '5 minutes',
    count(e.trip_id),
    COALESCE(sum(e.total_amount), 0),
    avg(extract(epoch FROM (e.dropoff_at - e.pickup_at)) / 60.0)
        FILTER (WHERE e.dropoff_at IS NOT NULL),
    now()
FROM touched t
LEFT JOIN silver.trip_events e
    ON  e.pickup_zone_id = t.pickup_zone_id
    AND e.pickup_at >= t.window_start
    AND e.pickup_at <  t.window_start + interval '5 minutes'
    AND e.is_valid_trip
GROUP BY t.pickup_zone_id, t.window_start
ON CONFLICT (pickup_zone_id, window_start) DO UPDATE SET
    window_end       = EXCLUDED.window_end,
    trips            = EXCLUDED.trips,
    revenue          = EXCLUDED.revenue,
    avg_duration_min = EXCLUDED.avg_duration_min,
    updated_at       = EXCLUDED.updated_at
"""

# The window read and the window write must not interleave with another writer. Taken at transaction
# level, right before the recompute: a second batch would otherwise read the aggregate from a
# snapshot that does not see the other transaction, wait for the row lock and commit the stale
# number. It also keeps two batches from taking the window rows in opposite orders (deadlock).
_WINDOW_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('silver.zone_demand_5min'))"

# The invariant of the window table, asserted before the commit: the sum of the touched windows must
# be the number of valid trips in them. A bug here would silently break the reconciliation, so the
# batch fails loudly (and the checkpoint replays it) instead of committing wrong numbers.
_CHECK_WINDOW_SQL = f"""
WITH {_TOUCHED_WINDOWS_SQL}
SELECT
    (SELECT COALESCE(sum(z.trips), 0)
       FROM touched t
       JOIN silver.zone_demand_5min z
         ON z.pickup_zone_id = t.pickup_zone_id
        AND z.window_start = t.window_start) AS window_trips,
    (SELECT count(*)
       FROM touched t
       JOIN silver.trip_events e
         ON e.pickup_zone_id = t.pickup_zone_id
        AND e.pickup_at >= t.window_start
        AND e.pickup_at <  t.window_start + interval '5 minutes'
      WHERE e.is_valid_trip) AS valid_trips
"""

_INSERT_BATCH_RUN_SQL = """
INSERT INTO silver.batch_runs
    (batch_key, consumed, valid_stored, invalid, late_dropped, duplicates_dropped, rejected)
VALUES
    (%(batch_key)s, %(consumed)s, %(valid_stored)s, %(invalid)s, %(late_dropped)s,
     %(duplicates_dropped)s, %(rejected)s)
ON CONFLICT (batch_key) DO NOTHING
"""

_SUM_COUNTERS_SQL = (
    "SELECT "
    + ", ".join(f"COALESCE(SUM({column}), 0)" for column in COUNTER_COLUMNS)
    + ", COUNT(*) FROM silver.batch_runs"
)


@dataclass
class BatchResult:
    """What this batch did to the Silver. Goes to `silver.batch_runs` and to the counters."""

    consumed: int = 0
    valid_stored: int = 0
    invalid: int = 0
    late_dropped: int = 0
    duplicates_dropped: int = 0
    rejected: int = 0


def to_silver_events(df: DataFrame) -> DataFrame:
    """Raw Bronze event → Silver columns, with `is_valid_trip` computed.

    `event_time`/`dropoff_at` arrive as New York naive text; `try_to_timestamp` in the UTC session
    keeps the wall clock as it is (no shift), which is the contract of the Silver `TIMESTAMP`
    columns. `emit_time` carries an offset in the text and becomes the correct instant. The `try_`
    is mandatory: Spark 4 has ANSI on and a malformed timestamp **raises** instead of becoming null —
    a single bad record would stall the batch forever.
    """
    pickup_at = F.try_to_timestamp("event_time")
    dropoff_at = F.try_to_timestamp("dropoff_at")
    emit_time = F.try_to_timestamp("emit_time")
    duration_seconds = F.unix_timestamp(dropoff_at) - F.unix_timestamp(pickup_at)
    # The producer's `trip_id` uses `%.2f` of the total; comparing the raw double would create two
    # ids for the same value when the difference is beyond the second decimal.
    total_amount = F.round(F.col("total_amount"), 2)
    # `try_cast`: a value outside NUMERIC(10,2) becomes NULL instead of killing the batch. But the
    # row cannot be reported as valid with the fare lost on the way.
    fare_stored = F.col("fare_amount").try_cast("decimal(10, 2)")
    total_stored = F.col("total_amount").try_cast("decimal(10, 2)")
    unstorable_amount = (F.col("fare_amount").isNotNull() & fare_stored.isNull()) | (
        F.col("total_amount").isNotNull() & total_stored.isNull()
    )

    violates = (
        F.col("trip_id").isNull()
        | F.col("pickup_zone_id").isNull()
        | F.coalesce(duration_seconds <= 0, F.lit(False))
        | F.coalesce(F.col("fare_amount") < 0, F.lit(False))
        | F.coalesce(total_amount > FARE_CEILING, F.lit(False))
        | unstorable_amount
    )
    lag_seconds = (
        F.unix_timestamp("ingested_at") - F.unix_timestamp(emit_time)
    ).cast("double")

    return df.select(
        F.col("trip_id"),
        pickup_at.alias("pickup_at"),
        dropoff_at.alias("dropoff_at"),
        F.col("pickup_zone_id").cast("int"),
        F.col("dropoff_zone_id").cast("int"),
        fare_stored.alias("fare_amount"),
        F.col("tip_amount").try_cast("decimal(10, 2)").alias("tip_amount"),
        total_stored.alias("total_amount"),
        F.col("payment_type").try_cast("short").alias("payment_type"),
        (~violates).alias("is_valid_trip"),
        pickup_at.alias("event_time"),
        emit_time.alias("emit_time"),
        F.col("ingested_at"),
        lag_seconds.alias("lag_seconds"),
    )


def copy_frame(df: DataFrame) -> DataFrame:
    """Columns ready for COPY: timestamps as text in the session timezone (UTC).

    See decision 4 of the module: `collect()` of a `TIMESTAMP` uses the driver's local timezone and
    would shift the New York columns. Text in `TS_FORMAT` crosses without reinterpretation.
    """
    return df.select(
        F.col("trip_id"),
        F.date_format("pickup_at", TS_FORMAT).alias("pickup_at"),
        F.date_format("dropoff_at", TS_FORMAT).alias("dropoff_at"),
        F.col("pickup_zone_id"),
        F.col("dropoff_zone_id"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.col("payment_type"),
        F.col("is_valid_trip"),
        F.date_format("event_time", TS_FORMAT).alias("event_time"),
        F.date_format("emit_time", TS_FORMAT).alias("emit_time"),
        F.date_format("ingested_at", TS_FORMAT).alias("ingested_at"),
        F.col("lag_seconds"),
    )


def apply_late_filter(df: DataFrame, watermark: datetime | None) -> DataFrame:
    """Drop events more than 10 min behind the previous batch's watermark.

    A NULL watermark is the first batch: accepts everything. The watermark comes from Postgres as a
    naive `datetime` (New York clock); the cutoff is serialized as text so Spark does not interpret
    it in the driver's local timezone. A row without `event_time` is not judged here: it counts as
    `rejected` later.
    """
    if watermark is None:
        return df
    cutoff = (watermark - LATE_TOLERANCE).strftime("%Y-%m-%d %H:%M:%S")
    return df.where(
        F.col("event_time").isNull()
        | (F.col("event_time") >= F.try_to_timestamp(F.lit(cutoff)))
    )


def batch_bounds(batch: DataFrame) -> tuple[str, str | None]:
    """Batch identity and watermark, in a single pass.

    * Identity = `(min, max)` offset ranges per partition. It is stable when the checkpoint
      re-executes the batch, so the `silver.batch_runs` PK absorbs the repetition.
    * Watermark = the lowest `max(event_time)` across partitions, as text. The slowest partition is
      in charge: only below it is the pipeline sure no input will go back.
    """
    rows = (
        batch.groupBy("kafka_partition")
        .agg(
            F.min("kafka_offset").alias("lo"),
            F.max("kafka_offset").alias("hi"),
            F.date_format(F.max(F.try_to_timestamp("event_time")), TS_FORMAT).alias("max_event"),
        )
        .orderBy("kafka_partition")
        .collect()
    )
    key = ";".join(f"{row.kafka_partition}:{row.lo}-{row.hi}" for row in rows)
    # Fixed-width format: lexicographic order is chronological order.
    events = [row.max_event for row in rows if row.max_event is not None]
    return key, min(events) if events else None


def storable_filter(df: DataFrame) -> DataFrame:
    """Row that Postgres accepts.

    `trip_id` is the PK and `pickup_at`/`emit_time` are `NOT NULL`; a null would kill the whole batch
    and the checkpoint would stay stuck on the same offset. NUL does not cross the COPY (psycopg
    refuses any 0x00 byte in text); TAB/newline/backslash are escaped by psycopg itself.
    """
    return df.where(
        F.col("trip_id").isNotNull()
        & F.col("pickup_at").isNotNull()
        & F.col("emit_time").isNotNull()
        & (F.instr(F.col("trip_id"), F.lit("\x00")) == 0)
    )


def save_counters(path: Path, counters: dict) -> None:
    """Write atomically: a reader never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(counters, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def export_counters(conn: psycopg.Connection, path: Path) -> dict:
    """`data/state/counters.json` = sum of `silver.batch_runs` (not of the process).

    A batch re-executed by the checkpoint is not summed twice, and a process that dies after the
    commit still has the batch counters in the next export.
    """
    with conn.cursor() as cur:
        cur.execute(_SUM_COUNTERS_SQL)
        row = cur.fetchone()
    counters = {column: int(value) for column, value in zip(COUNTER_COLUMNS, row[:-1], strict=True)}
    counters["batches"] = int(row[-1])
    counters["updated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    save_counters(path, counters)
    return counters


def process_batch(
    batch: DataFrame,
    batch_id: int,
    dsn: str = DEFAULT_DSN,
    counters_path: Path | str = DEFAULT_COUNTERS_PATH,
) -> BatchResult:
    """Validate, filter late events and upsert one micro-batch; return what was done.

    Everything in one transaction: if the upsert, the ledger, the watermark update or the window
    recompute fails, the batch is repeated by the checkpoint with the old watermark, and the filter
    decides the same as it would on the first attempt. The `silver.batch_runs` PK keeps the
    repetition from counting twice.
    """
    prepared = to_silver_events(batch).persist()
    try:
        total = prepared.count()
        result = BatchResult(consumed=int(total or 0))
        if result.consumed == 0:
            return result

        batch_key, batch_max = batch_bounds(batch)

        with psycopg.connect(dsn, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                # `now()` is TIMESTAMPTZ; without a fixed timezone the server decides the session
                # offset.
                cur.execute("SET TIME ZONE 'UTC'")
                # A stuck Postgres or a concurrent lock must not stop the stream silently.
                cur.execute("SET statement_timeout = '10min'")
                cur.execute("SET lock_timeout = '30s'")
                cur.execute("SELECT watermark FROM silver.stream_state WHERE id = 1")
                row = cur.fetchone()
                watermark = row[0] if row else None

            accepted = apply_late_filter(prepared, watermark).persist()
            storable = storable_filter(accepted).persist()
            try:
                accepted_rows = accepted.count()
                storable_rows = storable.count()
                result.late_dropped = result.consumed - accepted_rows
                result.rejected = accepted_rows - storable_rows

                with conn.cursor() as cur:
                    cur.execute(_CREATE_STAGING_SQL)
                    if storable_rows:
                        rows = copy_frame(storable).collect()
                        with cur.copy(
                            f"COPY {_STAGING} ({_COLUMNS_SQL}) FROM STDIN"
                        ) as copy:
                            for row in rows:
                                copy.write_row(row)

                        cur.execute(_UPSERT_SQL)
                        returned = cur.fetchall()
                        distinct_ids = len({row.trip_id for row in rows})
                        inserted = sum(1 for was_insert, _ in returned if was_insert)
                        result.invalid = sum(
                            1
                            for was_insert, valid in returned
                            if was_insert and not valid
                        )
                        result.valid_stored = inserted - result.invalid
                        # A row that entered the COPY and did not become an INSERT: a duplicate in
                        # the batch or a conflict with an existing row. It is the PK dedup, not
                        # Spark's.
                        result.duplicates_dropped = storable_rows - inserted
                        # Real check, before the commit: DISTINCT ON must return one row per
                        # distinct trip_id in the batch.
                        if len(returned) != distinct_ids:
                            raise RuntimeError(
                                f"batch {batch_id}: DISTINCT ON returned {len(returned)} rows "
                                f"for {distinct_ids} distinct trip_id"
                            )

                        # The window work runs under the lock; the read must see every writer that
                        # committed before it.
                        cur.execute(_WINDOW_LOCK_SQL)
                        cur.execute(_WINDOW_SQL)
                        cur.execute(_CHECK_WINDOW_SQL)
                        window_trips, valid_trips = cur.fetchone()
                        if window_trips != valid_trips:
                            raise RuntimeError(
                                f"batch {batch_id}: windows sum {window_trips} != "
                                f"{valid_trips} valid trips in the touched windows"
                            )

                    cur.execute(_INSERT_BATCH_RUN_SQL, {"batch_key": batch_key, **asdict(result)})
                    if batch_max is not None:
                        # GREATEST: a batch with only old events (or a missing partition) cannot
                        # move the watermark back.
                        cur.execute(
                            "UPDATE silver.stream_state SET watermark = "
                            "GREATEST(COALESCE(watermark, %s::timestamp), %s::timestamp), "
                            "updated_at = now() WHERE id = 1",
                            (batch_max, batch_max),
                        )
            finally:
                accepted.unpersist()
                storable.unpersist()

            conn.commit()
            export_counters(conn, Path(counters_path))

        return result
    finally:
        prepared.unpersist()
