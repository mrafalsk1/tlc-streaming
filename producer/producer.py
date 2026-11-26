"""Replay of the TLC Parquet files into the ``trips.raw`` topic.

    uv run python producer/producer.py --rate 500 --limit 20000

Each file is one month; sorting by pickup inside the file and concatenating Jan→Mar gives the global
chronological order (which is what keeps almost every event from being born "late"). Key = pickup
zone, ``acks=all`` + idempotence (the replay must be reproducible: same events, same ``trip_id``).

``--rate 0`` disables the throttle and ``--limit 0`` replays the whole files. ``--dupes N`` resends
N events (same payload, same ``trip_id``) and ``--dirty N`` injects N invalid events with a unique
id — both flags exist so the Silver can prove dedup and validation for real.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Producer

if __package__ in (None, ""):  # run as a script: sys.path does not include the repo root
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.schema import (
    SOURCE_COLUMNS,
    build_dirty_event,
    build_event,
    dumps,
    partition_key,
)

DEFAULT_RAW_DIR = Path(os.environ.get("TLC_RAW_DIR", "~/projetos/tlc-elt/data/raw")).expanduser()
DEFAULT_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
CHUNK_ROWS = 10_000
MESSAGE_TIMEOUT_MS = 30_000
# The flush waits longer than the message timeout, otherwise it gives up before the error callbacks
# arrive and the process would exit with "0 failures" after losing a message silently.
FLUSH_TIMEOUT_S = 45.0
MAX_ERROR_SAMPLES = 5
# A broker that is not receiving: insisting only repeats the timeout. 100 failures already mean
# something is wrong.
ABORT_AFTER_FAILURES = 100
# `--dirty` shifts `event_time` up to `N` seconds ahead of the stream position. The Silver late
# filter uses 10 min; 5 min of headroom keep the dirty event from pushing the max and making the
# following real events look late (the opposite of the injection goal).
MAX_DIRTY_AHEAD_S = 300


class AbortReplay(RuntimeError):
    """Too many delivery failures: better to stop than to keep stacking timeouts."""

log = logging.getLogger("producer")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay TLC Parquet into the trips.raw topic.")
    parser.add_argument("--rate", type=float, default=500.0, help="events per second (0 = no throttle)")
    parser.add_argument("--limit", type=int, default=20_000, help="total events (0 = all)")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"Parquet directory (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--files",
        type=Path,
        nargs="+",
        help="explicit Parquet files, in the given order (overrides --raw-dir)",
    )
    parser.add_argument("--brokers", default=DEFAULT_BROKERS, help=f"default: {DEFAULT_BROKERS}")
    parser.add_argument("--topic", default="trips.raw")
    parser.add_argument("--dupes", type=int, default=0, help="exact duplicates to inject")
    parser.add_argument(
        "--dirty",
        type=int,
        default=0,
        help=f"invalid events to inject, reasons cycling through DIRTY_KINDS (max {MAX_DIRTY_AHEAD_S})",
    )
    parser.add_argument("--log-every", type=float, default=5.0, help="ev/s log interval")
    args = parser.parse_args(argv)
    if args.rate < 0:
        parser.error("--rate cannot be negative")
    if args.limit < 0:
        parser.error("--limit cannot be negative")
    if args.dupes < 0:
        parser.error("--dupes cannot be negative")
    if args.dirty < 0:
        parser.error("--dirty cannot be negative")
    if args.dirty > MAX_DIRTY_AHEAD_S:
        parser.error(
            f"--dirty maximum is {MAX_DIRTY_AHEAD_S}: the dirty event shifts event_time up to N s "
            "ahead and the Silver late filter uses 10 min"
        )
    if args.log_every <= 0:
        parser.error("--log-every must be positive")
    return args


def resolve_files(args: argparse.Namespace) -> list[Path]:
    if args.files:
        files = list(args.files)
    else:
        files = sorted(args.raw_dir.glob("yellow_tripdata_*.parquet"))
    if not files:
        raise SystemExit(f"no Parquet in {args.raw_dir} (use --files or TLC_RAW_DIR)")
    return files


def iter_rows(path: Path, chunk_rows: int = CHUNK_ROWS) -> Iterator[dict[str, Any]]:
    """Rows of the file in ``tpep_pickup_datetime`` order.

    Sorts the whole month: only then does the ``--limit`` sample come out chronological. The Arrow
    table stays alive during the iteration (the accepted cost); ``to_pylist`` runs in blocks so
    memory does not blow up with 3 million dicts.
    """
    table = pq.read_table(path, columns=list(SOURCE_COLUMNS))
    indices = pc.sort_indices(table, sort_keys=[("tpep_pickup_datetime", "ascending")])
    for start in range(0, table.num_rows, chunk_rows):
        chunk = table.take(indices.slice(start, chunk_rows))
        yield from chunk.to_pylist()


def pace(started: float, sent: int, rate: float) -> None:
    """Hold the pace at ``rate`` ev/s, sleeping only what is missing (``rate 0`` = no throttle)."""
    if rate <= 0:
        return
    delay = started + sent / rate - time.monotonic()
    if delay > 0.005:  # below this the sleep costs more than it helps
        time.sleep(delay)


def run(args: argparse.Namespace) -> int:
    files = resolve_files(args)
    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "client.id": "tlc-replay",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "message.timeout.ms": MESSAGE_TIMEOUT_MS,
        }
    )
    delivery_failures = 0
    error_samples: list[str] = []

    def on_delivery(err: Any, msg: Any) -> None:
        nonlocal delivery_failures
        if err is not None:
            delivery_failures += 1
            if len(error_samples) < MAX_ERROR_SAMPLES:
                error_samples.append(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {err}")

    def send(event: dict[str, Any]) -> None:
        """Send without flooding the local queue; stop when the broker has refused too many."""
        key = partition_key(event)
        payload = dumps(event)
        while True:
            if delivery_failures >= ABORT_AFTER_FAILURES:
                raise AbortReplay(
                    f"{delivery_failures} delivery failures: the broker is not receiving"
                )
            try:
                producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                return
            except BufferError:
                # local queue full: serve callbacks to free space and try again
                producer.poll(0.5)

    sent = 0
    dupes_sent = 0
    dirty_sent = 0
    skipped = 0
    started = time.monotonic()
    paced_start: float | None = None
    window_start = started
    window_count = 0
    last_event: dict[str, Any] | None = None
    aborted = False

    try:
        for path in files:
            if args.limit and sent >= args.limit:
                break
            log.info("replay %s", path.name)
            # reading/sorting each month does not become pacing credit (a burst at the boundary)
            paced_start = None
            for row in iter_rows(path):
                if args.limit and sent >= args.limit:
                    break
                if row.get("tpep_pickup_datetime") is None:
                    skipped += 1
                    continue
                event = build_event(row, datetime.now(UTC))
                send(event)
                last_event = event
                sent += 1
                window_count += 1
                # Injection: a duplicate is the same payload again; a dirty event is derived from
                # the current event, with a unique trip_id and an invalidity reason cycling through
                # DIRTY_KINDS.
                if dupes_sent < args.dupes:
                    send(event)
                    dupes_sent += 1
                if dirty_sent < args.dirty:
                    send(build_dirty_event(event, dirty_sent, datetime.now(UTC)))
                    dirty_sent += 1
                producer.poll(0)  # serve delivery callbacks (does not drain the produce queue)
                if paced_start is None:
                    # anchor after the first send: sorting the month does not become burst credit
                    paced_start = time.monotonic()
                pace(paced_start, sent + dupes_sent + dirty_sent, args.rate)

                now = time.monotonic()
                if now - window_start >= args.log_every:
                    log.info(
                        "sent=%d  %.0f ev/s total  %.0f ev/s in the last %.0fs",
                        sent,
                        sent / (now - started),
                        window_count / (now - window_start),
                        now - window_start,
                    )
                    window_start = now
                    window_count = 0
        # --dupes/--dirty larger than the stream: the remainder goes out anchored on the last real
        # event (the flag contract is "inject N" — the Silver counter must match the request).
        if last_event is not None:
            while dupes_sent < args.dupes:
                send(last_event)
                dupes_sent += 1
            while dirty_sent < args.dirty:
                send(build_dirty_event(last_event, dirty_sent, datetime.now(UTC)))
                dirty_sent += 1
        elif args.dupes or args.dirty:
            log.warning(
                "no real event to anchor the injection: 0 of %d duplicates and 0 of %d dirty",
                args.dupes,
                args.dirty,
            )
    except AbortReplay as exc:
        log.error("%s", exc)
        aborted = True
    finally:
        pending = producer.flush(FLUSH_TIMEOUT_S)
        if pending and len(error_samples) < MAX_ERROR_SAMPLES:
            error_samples.append(
                f"{pending} messages unconfirmed after {FLUSH_TIMEOUT_S:.0f}s (message.timeout "
                f"{MESSAGE_TIMEOUT_MS}ms)"
            )

    elapsed = time.monotonic() - started
    total = sent + dupes_sent + dirty_sent
    log.info(
        "done: %d sent, %d duplicates, %d dirty, %d skipped (null pickup), "
        "%d delivery failures, %d unconfirmed, in %.1fs = %.0f ev/s real "
        "(%.0f ev/s at the broker)",
        sent,
        dupes_sent,
        dirty_sent,
        skipped,
        delivery_failures,
        pending,
        elapsed,
        sent / elapsed if elapsed else 0.0,
        total / elapsed if elapsed else 0.0,
    )
    if delivery_failures or pending or aborted:
        if error_samples:
            log.error("first failures: %s", "; ".join(error_samples))
        return 1
    if args.limit and sent < args.limit:
        log.error("limit of %d not reached: only %d events available", args.limit, sent)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
