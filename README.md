# TLC Streaming — Kafka + Spark Structured Streaming

Streaming pipeline over the NYC TLC trip records: a Python producer replays the Parquet files as
events into Redpanda (Kafka API), Spark Structured Streaming validates and lands them in a Bronze
(Parquet) and a Silver layer (Postgres), and dbt models the Gold layer on top of Silver.

**Status:** day 5 of 15 — the producer and the Bronze layer are running; Silver (validation, late
data, upsert) is next. Numbers and diagrams land as they are measured — nothing in this README is
estimated.

```
Parquet TLC
   │  producer (replay in pickup_at order, rate limited)        ← implemented (day 3; tests + --dupes/--dirty on day 4)
   ▼
Redpanda  topic: trips.raw (6 partitions, key = pickup_zone_id, retention 24h)
   │
   ▼
Spark Structured Streaming (10s trigger)                       ← day 5: reads trips.raw, writes Bronze
   ├─ Bronze: raw append Parquet  data/bronze/                 ← implemented (day 5)
   └─ Silver (Postgres): validated, late-filtered, deduplicated by trip_id   ← target, days 6-8
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
make stream  # Spark consumes trips.raw into data/bronze (Ctrl+C stops; STREAM_ARGS=--available-now drains and exits)
make bronze  # counts the Bronze with pyarrow, no JVM (BRONZE_ARGS=--check for nulls/repeated offsets)
make test    # pytest of the producer and the streaming job
make lint    # ruff on producer, streaming, scripts and tests
make down    # keeps the volumes
```

`make produce DUPES=50 DIRTY=100` injects exact duplicates and invalid events (the day-6 dedup and
validation proof needs them in a known amount; naturally invalid rows also exist in the TLC data and
are counted too).

## Bronze (day 5)

`make stream` reads `trips.raw` with an explicit schema (no `inferSchema`), adds `kafka_partition`,
`kafka_offset` and `ingested_at`, and writes append-only Parquet under
`data/bronze/ingest_date=YYYY-MM-DD/hour=HH/` — partitions in **UTC**, so the folder layout does not
depend on the machine timezone. Invalid and late events **do** land here on purpose: Bronze is the
audit and replay base, and the reconciliation starts from it. Nothing is filtered before Silver.

Measured on the 100k replay: **100,000 rows, 18 Parquet files, 0 null `trip_id`, 100,000 distinct
`(kafka_partition, kafka_offset)` pairs**, ~15 s end to end on a cold JVM. That is three
micro-batches (49,998 + 49,996 + 6): the 50k cap is split across the 6 partitions, so it is a target,
not an exact batch size. Running the job again with the same checkpoint appends **nothing** — the
offsets come from the checkpoint, not from the start of the topic.

## Design decisions

| Decision | Why | Trade-off accepted |
|---|---|---|
| Redpanda instead of Kafka + Zookeeper | single binary, no JVM, starts in seconds, same Kafka API | not "official Kafka" |
| Key = `pickup_zone_id` | preserves per-zone order, which is what the zone aggregation needs | hotspot: busy zones load one partition |
| At-least-once + idempotent upsert | easy to prove; real exactly-once needs distributed transactions | replays are possible; the idempotency lives in the destination |
| Two timestamps: `event_time` (pickup) + `emit_time` (send) | the replay is historical, so pickup time cannot measure pipeline latency | producer must replay in chronological order |
| No Spark stateful dedup | 9.5M keys would grow the state store forever | dedup is the `PRIMARY KEY (trip_id)` upsert |
| Bronze keeps `event_time`/`dropoff_at` as text | the TLC timestamp is naive New York time; casting at ingest would pick a timezone silently | Silver must cast before windowing (day 6) |
| Fixed consumer group id (`spark-tlc-stream`) | day 13 reads consumer lag per partition with `rpk group describe` | a killed query can collide with its dying consumer, so session timeout is dropped to 10 s |

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
