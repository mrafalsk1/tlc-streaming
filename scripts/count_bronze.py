"""Conta o Bronze em Parquet **sem subir JVM** — verificação do ingest.

`pyarrow.dataset` lê só o metadado dos arquivos para o total de linhas (rápido mesmo com muitos
arquivos) e entende as pastas Hive-style `ingest_date=/hour=/` geradas pelo job. Com `--check`,
lê também `trip_id` e a posição no Kafka para responder as duas perguntas do restart test:
quantos eventos chegaram sem `trip_id` (corrompidos) e quantas linhas repetem
`(kafka_partition, kafka_offset)` (o at-least-once do Bronze).

Uso:
    make bronze                    # total de linhas e arquivos
    make bronze BRONZE_ARGS=--check
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.dataset as ds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Conta linhas do Bronze em Parquet.")
    parser.add_argument("--dir", default="data/bronze")
    parser.add_argument(
        "--check",
        action="store_true",
        help="também conta trip_id nulo e offsets repetidos",
    )
    args = parser.parse_args(argv)

    path = Path(args.dir)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        print(f"nenhum parquet em {path} — rode 'make stream'")
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
