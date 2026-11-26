"""Download the Spark Kafka connector into `jars/` (once, with network).

The download uses its **own** Ivy (`jars/.ivy`, covered by the `jars/` `.gitignore`) instead of the
user cache: that way the copied jars are exactly the ones the connector POM resolved. Scanning
`~/.ivy2*` would pick the newest version of each artifact from any project on the machine — a
`kafka-clients 4.x` from another job would enter Bronze without warning.

Spark 4 detail: it uses Ivy 2.5.2 and flattens the jar name
(`org.apache.spark_spark-sql-kafka-0-10_2.13-4.3.0.jar`), so matching is by substring, not by
prefix. After the first download the job runs **offline**, with `spark.jars` pointing at `jars/`.

Usage:
    make jars                                      # download and copy (skip if already complete)
    uv run python scripts/fetch_jars.py --force    # download again and overwrite jars/

The four artifacts Spark does **not** ship (the rest of the connector dependencies already come in
`pyspark/jars`): `spark-sql-kafka-0-10`, `spark-token-provider-kafka`, `kafka-clients` and
`commons-pool2` (Spark brings `commons-pool` 1.5.4, which does not work).
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
ARTIFACTS: tuple[str, ...] = (
    "spark-sql-kafka-0-10",
    "spark-token-provider-kafka",
    "kafka-clients",
    "commons-pool2",
)
VERSION_RE = re.compile(r"-(\d+(?:\.\d+)+)\.jar$")


def artifact_of(name: str) -> str | None:
    """Artifact that owns the file, matched by substring (`kafka-clients` ≠ `spark-sql-kafka-0-10`)."""
    for artifact in ARTIFACTS:
        if artifact in name:
            return artifact
    return None


def version_key(name: str) -> tuple[int, ...]:
    match = VERSION_RE.search(name)
    return tuple(int(part) for part in match.group(1).split(".")) if match else (0,)


def download(ivy_dir: Path) -> None:
    print(f"resolving {CONNECTOR} through Ivy in {ivy_dir} (needs network)...", flush=True)
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("fetch-jars")
        .config("spark.jars.packages", CONNECTOR)
        .config("spark.jars.ivy", str(ivy_dir.resolve()))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.stop()


def find_candidates(ivy_dir: Path) -> dict[str, list[Path]]:
    """Jars of each artifact in the dedicated Ivy (`<ivy>/jars` and `<ivy>/cache`)."""
    found: dict[str, list[Path]] = {artifact: [] for artifact in ARTIFACTS}
    for root in (ivy_dir / "jars", ivy_dir / "cache"):
        if not root.is_dir():
            continue
        for path in root.rglob("*.jar"):
            if path.name.endswith(("-sources.jar", "-javadoc.jar")):
                continue
            artifact = artifact_of(path.name)
            if artifact is not None:
                found[artifact].append(path)
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Copy the Kafka connector from Ivy into jars/.")
    parser.add_argument("--dest", default="jars")
    parser.add_argument("--force", action="store_true", help="download again and overwrite jars/")
    args = parser.parse_args(argv)

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    present = {artifact_of(path.name) for path in dest.glob("*.jar")}
    complete = all(artifact in present for artifact in ARTIFACTS)
    if complete and not args.force:
        print(f"{dest}/ already has the {len(ARTIFACTS)} artifacts — nothing to do (use --force to redo)")
        return 0

    ivy_dir = dest / ".ivy"
    candidates = find_candidates(ivy_dir)
    if args.force or not all(candidates[artifact] for artifact in ARTIFACTS):
        download(ivy_dir)
        candidates = find_candidates(ivy_dir)

    missing = [artifact for artifact in ARTIFACTS if not candidates[artifact]]
    if missing:
        print(f"not found in the Ivy of {ivy_dir}: {', '.join(missing)}", file=sys.stderr)
        print("the download failed (no network?) — check the Spark error above", file=sys.stderr)
        return 1

    for artifact in ARTIFACTS:
        newest = max(candidates[artifact], key=lambda path: version_key(path.name))
        target = dest / newest.name
        if target.exists() and not args.force:
            print(f"ok       {newest.name}")
            continue
        # `--force` also removes old versions of the same artifact so the classpath has no two.
        for stale in dest.glob("*.jar"):
            if stale.name != target.name and artifact_of(stale.name) == artifact:
                stale.unlink()
        shutil.copy2(newest, target)
        print(f"copied   {newest.name}")

    print(f"\n{len(list(dest.glob('*.jar')))} jars in {dest}/ — the job runs offline from here")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
