"""Count the Bronze in Parquet **without starting a JVM** — ingest verification.

`pyarrow.dataset` reads only the file metadata for the row total (fast even with many files) and
understands the Hive-style `ingest_date=/hour=/` folders the job writes. With `--check`, it also
reads `trip_id` and the Kafka position to answer the restart test's two questions: how many events
arrived without a `trip_id` (corrupt) and how many rows repeat `(kafka_partition, kafka_offset)`
(the Bronze at-least-once).

Usage:
    make bronze                    # row and file totals
    make bronze BRONZE_ARGS=--check
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.dataset as ds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Count the Bronze rows in Parquet.")
    parser.add_argument("--dir", default="data/bronze")
    parser.add_argument(
        "--check",
        action="store_true",
        help="also count null trip_id and repeated offsets",
    )
    args = parser.parse_args(argv)

    path = Path(args.dir)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        print(f"no parquet in {path} — run 'make stream'")
        return 1

    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    rows = dataset.count_rows()
    print(f"rows={rows} files={len(files)}")

    if args.check:
        table = dataset.to_table(columns=["trip_id", "kafka_partition", "kafka_offset"])
        frame = table.to_pandas()
        nulls = int(frame["trip_id"].isna().sum())
        positions = frame[["kafka_partition", "kafka_offset"]].drop_duplicates()
        print(
            f"trip_id_null={nulls} offset_pairs={len(positions)} "
            f"offset_duplicates={rows - len(positions)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
