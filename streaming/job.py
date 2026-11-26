"""Spark reads `trips.raw` and writes Bronze (Parquet) and Silver (Postgres) in the same micro-batch.

Bronze is append-only and **raw**: valid, invalid and late events all land there. Cleanup and dedup
belong to the Silver — this job only carries what arrived. Assumed consequence: if it dies between
the Parquet write and the checkpoint commit, the micro-batch is written again and Bronze gets rows
repeated by `(kafka_partition, kafka_offset)`. Not a bug, it is the at-least-once picture; the
restart test measures it.

The Silver branch runs in the same `foreachBatch`, after the Parquet: it validates, filters late
data through `silver.stream_state` and upserts into Postgres (see `streaming.silver`). One batch, two
layers, the same offsets — what Bronze stored is exactly what the Silver judged.

Three decisions that are not obvious:

* **Fixed `kafka.group.id`** (`spark-tlc-stream`): without it Spark generates a random id per run and
  `rpk group describe` cannot find the group — there is no way to talk about consumer lag.
* **Offsets come from the checkpoint**, not from the group: `startingOffsets=earliest` only applies on
  the first run (new checkpoint). Stopping and starting again continues from where it stopped, without
  reprocessing from the beginning.
* **Partition by `ingest_date`/`hour` in UTC**: `ingested_at` is the write instant; deriving the
  partition in the session timezone would put the same event in a different folder depending on the
  machine `TZ`. That is why the session runs in **UTC** and `add_ingest_columns` refuses any other
  timezone instead of silently writing the wrong partition. The same UTC serves the Silver: the
  `event_time` arrives as New York naive text and `to_timestamp` keeps the wall clock (it does not
  shift), while `emit_time`/`ingested_at` become the correct instants.

Usage (the Makefile already exports the JDK 21 `JAVA_HOME`):

    make jars     # once, with network: downloads the connector and copies it to jars/
    make stream   # runs until Ctrl+C
    make stream STREAM_ARGS="--available-now"   # drains what exists and exits
"""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming import silver
from streaming.schema import EVENT_SCHEMA

DEFAULT_GROUP_ID = "spark-tlc-stream"
# Bronze stores `event_time`/`dropoff_at` as text (raw fidelity), so the job does not need to
# interpret the New York clock; the session stays in UTC so the partitions are stable.
SESSION_TIMEZONE = "UTC"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume trips.raw and write Bronze + Silver.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="trips.raw")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--checkpoint-dir", default="data/checkpoints/bronze")
    parser.add_argument("--jars-dir", default="jars")
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50_000)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--dsn", default=silver.DEFAULT_DSN, help="Postgres DSN for the Silver")
    parser.add_argument(
        "--counters",
        default=silver.DEFAULT_COUNTERS_PATH,
        help="incremental Silver counters",
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="write only Bronze (debug; the normal pipeline includes the Silver)",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process what already exists in the topic (all batches) and exit",
    )
    args = parser.parse_args(argv)
    if args.max_offsets_per_trigger <= 0:
        parser.error(
            "--max-offsets-per-trigger must be positive: the Silver collects the batch on the "
            "driver, and an unlimited cap turns into OOM (0 = no cap in Spark)"
        )
    return args


def jar_list(jars_dir: Path) -> list[str]:
    """Connector jars already downloaded; an empty list means `make jars` has not run yet."""
    return [str(path) for path in sorted(jars_dir.glob("*.jar"))]


def build_session(args: argparse.Namespace, jars: list[str]) -> SparkSession:
    builder = (
        SparkSession.builder.master(args.master)
        .appName("tlc-stream")
        # `ingest_date`/`hour` come from Spark date functions, which use the session timezone.
        # UTC here is what makes the partition independent of the machine `TZ` (see
        # add_ingest_columns).
        .config("spark.sql.session.timeZone", SESSION_TIMEZONE)
        # Single-node lab: the default (200) only creates empty tasks and pollutes the log.
        .config("spark.sql.shuffle.partitions", "4")
    )
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
    return builder.getOrCreate()


def parse_events(df: DataFrame) -> DataFrame:
    """Topic JSON → event columns + Kafka position.

    `PERMISSIVE` (default) returns nulls for a corrupt line instead of killing the job: Bronze is
    raw and a null `trip_id` is exactly one of the Silver invalidity rules (there it becomes
    `rejected`, because there is no PK to receive a null).
    """
    parsed = df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
    )
    return parsed.select("event.*", "kafka_partition", "kafka_offset")


def add_ingest_columns(df: DataFrame) -> DataFrame:
    """Add `ingested_at` and the `ingest_date`/`hour` partitions, derived in UTC.

    `to_date`/`hour` read the timestamp in the session timezone; that is why the job session is
    fixed to UTC and the function **refuses** any other timezone instead of writing the wrong
    partition without warning.
    """
    session_tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if session_tz != SESSION_TIMEZONE:
        raise ValueError(
            f"session timezone is {session_tz!r}, expected {SESSION_TIMEZONE!r}: "
            "the ingest_date/hour partitions would come out wrong"
        )
    ingested_at = F.current_timestamp()
    return df.select(
        "*",
        ingested_at.alias("ingested_at"),
        F.to_date(ingested_at).alias("ingest_date"),
        F.hour(ingested_at).alias("hour"),
    )


def build_stream(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("kafka.group.id", args.group_id)
        # Fixed group + `kill -9` in the restart test: without this the coordinator waits the 45 s
        # Kafka default to declare the consumer dead and the restart looks stuck. The default
        # heartbeat (3 s) stays below the 3× minimum required by the broker.
        .option("kafka.session.timeout.ms", "10000")
        # Only effective on the first run (new checkpoint); afterwards the checkpoint decides.
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )
    return add_ingest_columns(parse_events(raw))


def handle_batch(
    batch: DataFrame,
    batch_id: int,
    *,
    bronze_dir: str,
    dsn: str,
    counters_path: str,
    bronze_only: bool = False,
) -> None:
    """`foreachBatch` target: Bronze first (raw), then the Silver branch."""
    batch.persist()
    try:
        (
            batch.write.format("parquet")
            .mode("append")
            .partitionBy("ingest_date", "hour")
            .save(bronze_dir)
        )
        if bronze_only:
            return
        result = silver.process_batch(batch, batch_id, dsn=dsn, counters_path=counters_path)
        print(
            f"batch {batch_id}: consumed={result.consumed} "
            f"valid={result.valid_stored} invalid={result.invalid} "
            f"late={result.late_dropped} duplicates={result.duplicates_dropped} "
            f"rejected={result.rejected}",
            flush=True,
        )
    finally:
        batch.unpersist()


def run(args: argparse.Namespace) -> int:
    jars = jar_list(Path(args.jars_dir))
    if not jars:
        print(
            f"no jars in {args.jars_dir}/ — run 'make jars' once (needs network)",
            flush=True,
        )
        return 2

    spark = build_session(args, jars)
    print(
        f"spark {spark.version} | {args.master} | group {args.group_id} | "
        f"max batch {args.max_offsets_per_trigger} | tz {SESSION_TIMEZONE}",
        flush=True,
    )

    handler = partial(
        handle_batch,
        bronze_dir=args.bronze_dir,
        dsn=args.dsn,
        counters_path=args.counters,
        bronze_only=args.bronze_only,
    )
    writer = (
        build_stream(spark, args)
        .writeStream.foreachBatch(handler)
        .option("checkpointLocation", args.checkpoint_dir)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    print(
        f"bronze at {args.bronze_dir} | checkpoint {args.checkpoint_dir} | "
        f"silver {'off (--bronze-only)' if args.bronze_only else 'on'}",
        flush=True,
    )
    query = writer.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Ctrl+C — stopping the query and releasing the checkpoint", flush=True)
        query.stop()
    finally:
        spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
