"""Schema explícito do evento consumido de `trips.raw`.

O tópico carrega JSON sem Schema Registry (Avro ficou fora do escopo), então o produtor é a
definição do contrato e este arquivo é a única outra ponta: nada de `inferSchema`, que numa amostra
pequena decide tipo errado e transforma `pickup_zone_id` em `string` só porque um lote veio nulo.

Os nomes vêm de `producer.schema.EVENT_FIELDS` justamente para as duas pontas não divergirem em
silêncio (há teste amarrando a ordem). Os dois tempos ficam **texto** no Bronze de propósito: é a
camada crua, e converter "2024-01-01T00:12:00" (naive, hora local de NY) em timestamp já escolheria
um fuso no ingest. Quem interpreta o relógio é o Silver (Dia 6), com o fuso fixado no job.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from producer.schema import EVENT_FIELDS

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True),
        StructField("dropoff_at", StringType(), nullable=True),
        StructField("pickup_zone_id", IntegerType(), nullable=True),
        StructField("dropoff_zone_id", IntegerType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("payment_type", DoubleType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("emit_time", StringType(), nullable=True),
    ]
)

if tuple(field.name for field in EVENT_SCHEMA.fields) != EVENT_FIELDS:
    raise RuntimeError(
        "EVENT_SCHEMA divergiu do contrato do produtor: "
        f"{tuple(f.name for f in EVENT_SCHEMA.fields)} != {EVENT_FIELDS}"
    )

# Colunas que o Bronze acrescenta ao evento, na ordem em que saem no Parquet. `ingest_date`/`hour`
# são colunas de partição derivadas em UTC (ver `streaming.job.add_ingest_columns`).
BRONZE_COLUMNS: tuple[str, ...] = EVENT_FIELDS + (
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
    "ingest_date",
    "hour",
)
