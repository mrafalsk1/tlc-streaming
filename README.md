# TLC Streaming — Kafka + Spark Structured Streaming

Streaming pipeline over the NYC TLC trip records: a Python producer replays the Parquet files as
events into Redpanda (Kafka API), Spark Structured Streaming validates and lands them in a Bronze
(Parquet) and a Silver layer (Postgres), and dbt models the Gold layer on top of Silver.

**Status:** the producer, the Bronze layer and the Silver layer (validation, late filter, idempotent
upsert) are running; dbt (Gold) and the metrics are next. Numbers and diagrams land as they are
measured — nothing in this README is estimated.

```
Parquet TLC
   │  producer (replay in pickup_at order, rate limited)        ← implemented (replay, pacing, --dupes/--dirty)
   ▼
Redpanda  topic: trips.raw (6 partitions, key = pickup_zone_id, retention 24h)
   │
   ▼
Spark Structured Streaming (10s trigger)                       ← implemented (explicit schema, checkpoint)
   ├─ Bronze: raw append Parquet  data/bronze/                 ← implemented
   └─ Silver (Postgres): validated, late-filtered, deduplicated by trip_id   ← implemented
   │
   ▼
dbt (Gold) → fct_trips_stream, agg_zone_hourly, agg_payment_behavior
```

## Running it

```bash
make up      # redpanda (9092), console (8081), postgres (5434)
make deps    # python 3.12 venv + pinned requirements (uv)
make silver  # applies the Silver DDL (idempotent)
make smoke   # Spark local up on JDK 21
make jars    # downloads the Kafka connector once into jars/ (needs network; offline afterwards)
make topic   # creates trips.raw with 6 partitions
make produce # replays a 20k-event sample at 500 ev/s (RATE=0 LIMIT=0 for the full run)
make stream  # Spark consumes trips.raw into Bronze and Silver (Ctrl+C stops; STREAM_ARGS=--available-now drains and exits)
make bronze  # counts the Bronze with pyarrow, no JVM (BRONZE_ARGS=--check for nulls/repeated offsets)
make counters     # incremental Silver counters (invalid / late / duplicates / rejected)
make silver-stats # Silver row counts (rows, distinct trip_ids, invalid)
make test    # pytest of the producer and the streaming job
make lint    # ruff on producer, streaming, scripts and tests
make down    # keeps the volumes
```

`make produce DUPES=50 DIRTY=100` injects exact duplicates and invalid events (the Silver dedup and
validation proof needs them in a known amount; naturally invalid rows also exist in the TLC data and
are counted too).

## Bronze

`make stream` reads `trips.raw` with an explicit schema (no `inferSchema`), adds `kafka_partition`,
`kafka_offset` and `ingested_at`, and writes append-only Parquet under
`data/bronze/ingest_date=YYYY-MM-DD/hour=HH/` — partitions in **UTC**, so the folder layout does not
depend on the machine timezone. Invalid and late events **do** land here on purpose: Bronze is the
audit and replay base, and the reconciliation starts from it. Nothing is filtered before Silver.

Measured on the 100k replay with `--dupes 50 --dirty 100`: **100,150 rows, 18 Parquet files, 0 null
`trip_id`, 100,150 distinct `(kafka_partition, kafka_offset)` pairs**. That is three micro-batches
(49,997 + 49,998 + 155): the 50k cap is a target split across the 6 partitions, so it is not an
exact batch size. Running the job again with the same checkpoint appends **nothing** — the offsets
come from the checkpoint, not from the start of the topic.

## Silver

The Silver branch runs in the same `foreachBatch`, after the Parquet write: validation, late filter
and upsert into Postgres 5434. An invalid event is **stored** with `is_valid_trip = false` (it is
data, not noise); only a row without `trip_id`, `pickup_at` or `emit_time` (PK / `NOT NULL`) — or
with a NUL in the id, which the COPY cannot carry — is counted as `rejected`.
Deduplication is the `PRIMARY KEY (trip_id)` upsert — no Spark state grows with the stream.

The late filter compares `event_time` with the watermark of the previous batch, which is the
**lowest** of the per-partition `max(event_time)` values. The replay is keyed by zone and the offset
cap is split across partitions, so they advance at different event-time rates (in an 8k batch, slice
ends differed by ~6 h); the global maximum would mark in-order rows from the slower partitions as
late. The lowest maximum is the point every input has passed. The effective tolerance is therefore
the previous batch's event-time span plus 10 minutes: with a wide batch, a genuinely late event is
only classified once the watermark has passed it — `late_dropped` is exact in the accounting, not a
promise that every out-of-order event is dropped. The limit of this single-row state is the silent
partition: a partition with no traffic for several batches is not represented in the watermark.

The per-batch result is written to `silver.batch_runs` in the same transaction, keyed by the batch's
offset ranges. A batch replayed by the checkpoint is a no-op there, so the counters never count the
same work twice; `data/state/counters.json` is the sum of that table.

Measured on the same replay:

| Counter | Value |
|---|---|
| consumed | 100,150 |
| valid stored | 98,421 |
| invalid | 1,679 |
| late dropped | 0 |
| duplicates dropped | 50 |
| rejected | 0 |

`98,421 + 1,679 + 0 + 50 = 100,150`: the reconciliation closes. `invalid` is 1,579 naturally
invalid TLC rows plus the 100 injected dirty events. `silver.trip_events` ends with **100,100 rows
and 100,100 distinct `trip_id`** — the 50 duplicates never became rows — and a second run with the
same checkpoint changes neither Bronze, Silver nor the counters. A re-run with a fresh Bronze
checkpoint replays the same offsets: Bronze doubles by design (at-least-once), while Silver and
the counters stay unchanged — the ledger absorbs the replay. Timestamps keep their contract:
`pickup_at` is the naive New York wall clock (min `2002-12-31 22:59:39`, the TLC outlier; max
`2024-01-02 11:02:23`); `lag_seconds` is `ingested_at - emit_time` and depends on the backlog of the
run, so the steady-state latency numbers come with the metrics, not from this drain.

## Design decisions

| Decision | Why | Trade-off accepted |
|---|---|---|
| Redpanda instead of Kafka + Zookeeper | single binary, no JVM, starts in seconds, same Kafka API | not "official Kafka" |
| Key = `pickup_zone_id` | preserves per-zone order, which is what the zone aggregation needs | hotspot: busy zones load one partition |
| At-least-once + idempotent upsert | easy to prove; real exactly-once needs distributed transactions | replays are possible; the idempotency lives in the destination |
| Two timestamps: `event_time` (pickup) + `emit_time` (send) | the replay is historical, so pickup time cannot measure pipeline latency | producer must replay in chronological order |
| No Spark stateful dedup | 9.5M keys would grow the state store forever | dedup is the `PRIMARY KEY (trip_id)` upsert |
| Job-decided late filter | `late_dropped` is exact by construction; the Spark watermark stays only in the window | one more component to maintain |
| Watermark = lowest per-partition `max(event_time)` | in a zone-keyed replay the partitions advance at different event-time rates, so the global max would drop in-order rows | the watermark moves at the slowest partition's pace |
| Counters in a DB ledger (`silver.batch_runs`) | a batch replayed by the checkpoint does not count twice; the JSON is derived from the table | one more table to keep in the DDL |
| `try_to_timestamp`/`try_cast` on the Silver side | Spark 4 has ANSI on: one malformed timestamp or out-of-range amount would abort every retry of the batch | the bad value becomes NULL (and the row is flagged or rejected) instead of failing |
| Bronze keeps `event_time`/`dropoff_at` as text | the TLC timestamp is naive New York time; casting at ingest would pick a timezone silently | Silver must cast before windowing |
| Fixed consumer group id (`spark-tlc-stream`) | consumer lag per partition is read with `rpk group describe` | a killed query can collide with its dying consumer, so session timeout is dropped to 10 s |

This is a **single-node lab, at-least-once**, not production. It is described that way everywhere,
including here.

## Limitations

- Single Redpanda node, single Spark local mode, no replication.
- Bronze is append at-least-once: a crash between the Parquet write and the checkpoint commit
  duplicates rows in Bronze by design (and that duplication is measured, not hidden).
- Synthetic `trip_id` (SHA-1 of the natural key) — the TLC dataset has no trip id.

## Next steps

- Schema Registry + Avro, Kafka Connect S3 sink, Debezium CDC.
- Compare micro-batch vs continuous processing latency.
