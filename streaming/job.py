"""Spark lê `trips.raw` e grava Bronze (Parquet) e Silver (Postgres) no mesmo micro-lote.

O Bronze é append-only e **cru**: válidos, inválidos e atrasados entram todos. Limpeza e dedup são
do Silver — este job só transporta o que chegou. Consequência assumida: se ele morrer entre a
escrita do Parquet e o commit do checkpoint, o micro-lote é reescrito e o Bronze ganha linhas
repetidas por `(kafka_partition, kafka_offset)`. Não é bug, é o retrato do at-least-once; o restart
test mede isso.

O ramo Silver roda no mesmo `foreachBatch`, depois do Parquet: valida, filtra atraso pelo
`silver.stream_state` e faz upsert no Postgres (ver `streaming.silver`). Um lote, duas camadas, os
mesmos offsets — o que o Bronze guardou é exatamente o que o Silver julgou.

Três decisões que não são óbvias:

* **`kafka.group.id` fixo** (`spark-tlc-stream`): sem ele o Spark gera um id aleatório por execução e
  o `rpk group describe` não acha o grupo — não dá para falar de lag de consumidor.
* **Offset vem do checkpoint**, não do grupo: `startingOffsets=earliest` só vale na primeira
  execução (checkpoint novo). Parar e subir de novo continua de onde parou, sem reprocessar do zero.
* **Partição por `ingest_date`/`hour` em UTC**: `ingested_at` é o instante da escrita; derivar a
  partição no fuso da sessão faria o mesmo evento cair em pasta diferente conforme o `TZ` da
  máquina. Por isso a sessão roda em **UTC** e `add_ingest_columns` recusa qualquer outro fuso, em
  vez de gravar a partição errada em silêncio. O mesmo UTC serve ao Silver: o `event_time` chega
  como texto naive de NY e o `to_timestamp` mantém o relógio de parede (não desloca), enquanto
  `emit_time`/`ingested_at` viram instantes corretos.

Uso (o Makefile já exporta o `JAVA_HOME` do JDK 21):

    make jars     # uma vez, com rede: baixa o conector e copia para jars/
    make stream   # roda até Ctrl+C
    make stream STREAM_ARGS="--available-now"   # drena o que existe e encerra
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
# O Bronze guarda `event_time`/`dropoff_at` como texto (fidelidade do cru), então o job não precisa
# interpretar o horário de NY; a sessão fica em UTC para as partições serem estáveis.
SESSION_TIMEZONE = "UTC"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consome trips.raw e grava Bronze + Silver.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="trips.raw")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--checkpoint-dir", default="data/checkpoints/bronze")
    parser.add_argument("--jars-dir", default="jars")
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50_000)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--dsn", default=silver.DEFAULT_DSN, help="DSN do Postgres do Silver")
    parser.add_argument(
        "--counters",
        default=silver.DEFAULT_COUNTERS_PATH,
        help="contadores incrementais do Silver",
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="grava só o Bronze (depuração; o pipeline normal inclui o Silver)",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="processa o que já existe no tópico (todos os lotes) e encerra",
    )
    args = parser.parse_args(argv)
    if args.max_offsets_per_trigger <= 0:
        parser.error(
            "--max-offsets-per-trigger deve ser positivo: o Silver coleta o lote no driver, "
            "e um teto ilimitado vira OOM (0 = sem teto no Spark)"
        )
    return args


def jar_list(jars_dir: Path) -> list[str]:
    """Jars do conector já baixados; lista vazia significa que `make jars` ainda não rodou."""
    return [str(path) for path in sorted(jars_dir.glob("*.jar"))]


def build_session(args: argparse.Namespace, jars: list[str]) -> SparkSession:
    builder = (
        SparkSession.builder.master(args.master)
        .appName("tlc-stream")
        # `ingest_date`/`hour` saem das funções de data do Spark, que usam o fuso da sessão.
        # UTC aqui é o que torna a partição independente do `TZ` da máquina (ver add_ingest_columns).
        .config("spark.sql.session.timeZone", SESSION_TIMEZONE)
        # Lab de 1 nó: o default (200) só cria tarefas vazias e polui o log.
        .config("spark.sql.shuffle.partitions", "4")
    )
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
    return builder.getOrCreate()


def parse_events(df: DataFrame) -> DataFrame:
    """JSON do tópico → colunas do evento + posição no Kafka.

    `PERMISSIVE` (default) devolve nulos para linha corrompida em vez de derrubar o job: o Bronze é
    cru e o `trip_id` nulo é justamente uma das regras de invalidez do Silver (lá ele vira
    `rejected`, porque não há PK para receber nulo).
    """
    parsed = df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
    )
    return parsed.select("event.*", "kafka_partition", "kafka_offset")


def add_ingest_columns(df: DataFrame) -> DataFrame:
    """Acrescenta `ingested_at` e as partições `ingest_date`/`hour`, derivadas em UTC.

    `to_date`/`hour` leem o timestamp no fuso da sessão; por isso a sessão do job é fixada em UTC e
    a função **recusa** outro fuso em vez de gravar a partição errada sem avisar.
    """
    session_tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if session_tz != SESSION_TIMEZONE:
        raise ValueError(
            f"fuso da sessão é {session_tz!r}, esperado {SESSION_TIMEZONE!r}: "
            "as partições ingest_date/hour sairiam tortas"
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
        # Grupo fixo + `kill -9` no restart test: sem isto o coordenador espera os 45 s default do
        # Kafka para declarar o consumidor morto e o restart parece travado. Heartbeat default
        # (3 s) continua abaixo do mínimo de 3× exigido pelo broker.
        .option("kafka.session.timeout.ms", "10000")
        # Só tem efeito na primeira execução (checkpoint novo); depois manda o checkpoint.
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
    """Destino do `foreachBatch`: Bronze primeiro (cru), depois o ramo Silver."""
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
            f"lote {batch_id}: consumidos={result.consumed} "
            f"validos={result.valid_stored} invalidos={result.invalid} "
            f"atrasados={result.late_dropped} duplicados={result.duplicates_dropped} "
            f"rejeitados={result.rejected}",
            flush=True,
        )
    finally:
        batch.unpersist()


def run(args: argparse.Namespace) -> int:
    jars = jar_list(Path(args.jars_dir))
    if not jars:
        print(
            f"nenhum jar em {args.jars_dir}/ — rode 'make jars' uma vez (precisa de rede)",
            flush=True,
        )
        return 2

    spark = build_session(args, jars)
    print(
        f"spark {spark.version} | {args.master} | grupo {args.group_id} | "
        f"lote máx {args.max_offsets_per_trigger} | fuso {SESSION_TIMEZONE}",
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
        f"bronze em {args.bronze_dir} | checkpoint {args.checkpoint_dir} | "
        f"silver {'off (--bronze-only)' if args.bronze_only else 'on'}",
        flush=True,
    )
    query = writer.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Ctrl+C — encerrando a query e liberando o checkpoint", flush=True)
        query.stop()
    finally:
        spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
