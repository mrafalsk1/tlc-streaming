# TLC Streaming — Kafka + Spark Structured Streaming

Streaming pipeline over the NYC TLC trip records: a Python producer replays the Parquet files as
events into Redpanda (Kafka API), Spark Structured Streaming validates and lands them in a Bronze
(Parquet) and a Silver layer (Postgres), and dbt models the Gold layer on top of Silver.

**Status:** under construction (day 2 of 15). Numbers and diagrams land as they are measured — nothing
in this README is estimated.

```
Parquet TLC
   │  producer (replay in pickup_at order, rate limited)
   ▼
Redpanda  topic: trips.raw (6 partitions, key = pickup_zone_id, retention 24h)
   │
   ▼
Spark Structured Streaming (10s trigger)
   ├─ Bronze: raw append Parquet  data/bronze/
   └─ Silver (Postgres): validated, late-filtered, deduplicated by trip_id
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
make topic   # creates trips.raw with 6 partitions
make down    # keeps the volumes
```

## Design decisions

| Decision | Why | Trade-off accepted |
|---|---|---|
| Redpanda instead of Kafka + Zookeeper | single binary, no JVM, starts in seconds, same Kafka API | not "official Kafka" |
| Key = `pickup_zone_id` | preserves per-zone order, which is what the zone aggregation needs | hotspot: busy zones load one partition |
| At-least-once + idempotent upsert | easy to prove; real exactly-once needs distributed transactions | replays are possible; the idempotency lives in the destination |
| Two timestamps: `event_time` (pickup) + `emit_time` (send) | the replay is historical, so pickup time cannot measure pipeline latency | producer must replay in chronological order |
| No Spark stateful dedup | 9.5M keys would grow the state store forever | dedup is the `PRIMARY KEY (trip_id)` upsert |

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
