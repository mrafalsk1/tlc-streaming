"""Baixa o conector Kafka do Spark para `jars/` (uma vez, com rede).

O download usa um Ivy **próprio** (`jars/.ivy`, coberto pelo `.gitignore` do `jars/`) em vez do
cache do usuário: assim os jars copiados são exatamente os que o POM do conector resolveu. Varrer
`~/.ivy2*` pegaria a versão mais nova de cada artefato em qualquer projeto da máquina — uma
`kafka-clients 4.x` de outro trabalho entraria no Bronze sem aviso.

Detalhe do Spark 4: ele usa Ivy 2.5.2 e achata o nome do jar
(`org.apache.spark_spark-sql-kafka-0-10_2.13-4.3.0.jar`), então o casamento é por trecho, não por
prefixo. Depois do primeiro download o job roda **offline**, com `spark.jars` apontando para `jars/`.

Uso:
    make jars                                      # baixa e copia (pula se já estiver completo)
    uv run python scripts/fetch_jars.py --force    # refaz o download e sobrescreve jars/

Os quatro artefatos que o Spark **não** distribui (o resto das dependências do conector já vem no
`pyspark/jars`): `spark-sql-kafka-0-10`, `spark-token-provider-kafka`, `kafka-clients` e
`commons-pool2` (o Spark traz `commons-pool` 1.5.4, que não serve).
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
    """Artefato dono do arquivo, casando por trecho (`kafka-clients` ≠ `spark-sql-kafka-0-10`)."""
    for artifact in ARTIFACTS:
        if artifact in name:
            return artifact
    return None


def version_key(name: str) -> tuple[int, ...]:
    match = VERSION_RE.search(name)
    return tuple(int(part) for part in match.group(1).split(".")) if match else (0,)


def download(ivy_dir: Path) -> None:
    print(f"resolvendo {CONNECTOR} via Ivy em {ivy_dir} (precisa de rede)...", flush=True)
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
    """Jars de cada artefato no Ivy dedicado (`<ivy>/jars` e `<ivy>/cache`)."""
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
    parser = argparse.ArgumentParser(description="Copia o conector Kafka do Ivy para jars/.")
    parser.add_argument("--dest", default="jars")
    parser.add_argument("--force", action="store_true", help="baixa de novo e sobrescreve jars/")
    args = parser.parse_args(argv)

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    present = {artifact_of(path.name) for path in dest.glob("*.jar")}
    complete = all(artifact in present for artifact in ARTIFACTS)
    if complete and not args.force:
        print(f"{dest}/ já tem os {len(ARTIFACTS)} artefatos — nada a fazer (use --force para refazer)")
        return 0

    ivy_dir = dest / ".ivy"
    candidates = find_candidates(ivy_dir)
    if args.force or not all(candidates[artifact] for artifact in ARTIFACTS):
        download(ivy_dir)
        candidates = find_candidates(ivy_dir)

    missing = [artifact for artifact in ARTIFACTS if not candidates[artifact]]
    if missing:
        print(f"não achei no Ivy de {ivy_dir}: {', '.join(missing)}", file=sys.stderr)
        print("o download falhou (sem rede?) — veja o erro do Spark acima", file=sys.stderr)
        return 1

    for artifact in ARTIFACTS:
        newest = max(candidates[artifact], key=lambda path: version_key(path.name))
        target = dest / newest.name
        if target.exists() and not args.force:
            print(f"ok       {newest.name}")
            continue
        # `--force` também limpa versões antigas do mesmo artefato para o classpath não ter duas.
        for stale in dest.glob("*.jar"):
            if stale.name != target.name and artifact_of(stale.name) == artifact:
                stale.unlink()
        shutil.copy2(newest, target)
        print(f"copiado  {newest.name}")

    print(f"\n{len(list(dest.glob('*.jar')))} jars em {dest}/ — o job roda offline a partir daqui")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
