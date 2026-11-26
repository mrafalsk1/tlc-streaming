"""Ramo Silver: validação, filtro de atraso e upsert idempotente no Postgres.

O Bronze aceita tudo; é aqui que o evento é classificado. A ordem é a do contrato (docs/notes.md):
validação → filtro de atraso → upsert. A dedup **não** é stateful: quem deduplica é o
`PRIMARY KEY (trip_id)` do Postgres, então o estado do Spark não cresce com o stream.

Quatro decisões que não são óbvias:

* **Watermark do lote anterior, por partição.** O filtro lê `silver.stream_state.watermark`, gravado
  pelo lote anterior, e só depois grava o watermark deste lote. O valor é o **menor** dos
  `max(event_time)` de cada partição do Kafka: o replay é particionado por zona e o limite de
  offsets por trigger é dividido igualmente entre as partições, então elas avançam em ritmos
  diferentes de event-time (num lote de 8k, as fatias chegam a variar ~6 h). Usar o máximo global
  faria a partição mais adiantada declarar "atrasado" o que é só a vez das outras. O menor máximo é
  o ponto até onde todos os inputs avançaram — o watermark clássico. Se usasse o max do próprio
  lote, o primeiro evento do replay (outlier de 2002) já nasceria atrasado. Watermark NULL =
  primeiro lote, aceita tudo.
* **`is_valid_trip` com três valores.** Comparação com nulo devolve nulo, e um nulo gravado numa
  coluna `NOT NULL` derruba o lote. Por isso cada regra é um `coalesce(..., false)` e o resultado é
  negado: nulo em `fare_amount` não invalida a linha.
* **Contadores saem do upsert.** `ON CONFLICT DO UPDATE` conta insert e update igual; `RETURNING
  (xmax = 0)` separa os dois. `invalid` = gravadas com `is_valid_trip = false`; `duplicates_dropped`
  = linhas que o upsert não inseriu (duplicata dentro do lote ou conflito com linha existente). A
  duplicata de um evento inválido conta só como duplicata, senão a reconciliação conta duas vezes.
* **Tempo sai do Spark como texto.** `collect()` converte `TIMESTAMP` para o fuso **local do driver**
  (não o `spark.sql.session.timeZone`), o que deslocaria o relógio de NY e os instantes em 3 h numa
  máquina em UTC-3. `copy_frame` formata no fuso da sessão (UTC) e o Postgres recebe a string com a
  sessão em UTC: nada é reinterpretado no caminho.

`trip_id` nulo (JSON corrompido) é inválido mas não tem como entrar na PK: conta em `rejected` e
continua no Bronze. O mesmo vale para linha sem `pickup_at` (coluna `NOT NULL` do Silver).
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from producer.schema import FARE_CEILING

LATE_TOLERANCE = timedelta(minutes=10)
DEFAULT_DSN = os.environ.get(
    "SILVER_DSN", "postgresql://stream:stream@localhost:5434/streaming"
)
DEFAULT_COUNTERS_PATH = "data/state/counters.json"
# Formato que atravessa Spark → Postgres. Microssegundos para não perder `ingested_at`.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

# Ordem = ordem das colunas no COPY e no INSERT. `event_time` e `pickup_at` guardam o mesmo valor:
# a tabela mantém os dois nomes por compatibilidade com a janela (Dia 8) e com a reconciliação.
SILVER_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "pickup_at",
    "dropoff_at",
    "pickup_zone_id",
    "dropoff_zone_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "is_valid_trip",
    "event_time",
    "emit_time",
    "ingested_at",
    "lag_seconds",
)

_COLUMNS_SQL = ", ".join(SILVER_COLUMNS)
_STAGING = "silver_batch"

_CREATE_STAGING_SQL = f"""
CREATE TEMP TABLE {_STAGING} (
    trip_id          TEXT,
    pickup_at        TIMESTAMP,
    dropoff_at       TIMESTAMP,
    pickup_zone_id   INTEGER,
    dropoff_zone_id  INTEGER,
    fare_amount      NUMERIC(10, 2),
    tip_amount       NUMERIC(10, 2),
    total_amount     NUMERIC(10, 2),
    payment_type     SMALLINT,
    is_valid_trip    BOOLEAN,
    event_time       TIMESTAMP,
    emit_time        TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ,
    lag_seconds      DOUBLE PRECISION
) ON COMMIT DROP
"""

# `DISTINCT ON` colapsa a duplicata que chega dentro do mesmo lote: `ON CONFLICT DO UPDATE` recusa
# afetar a mesma linha duas vezes na mesma instrução (ao contrário de `DO NOTHING`).
_UPSERT_SQL = f"""
WITH incoming AS (
    SELECT DISTINCT ON (trip_id) {_COLUMNS_SQL}
    FROM {_STAGING}
    ORDER BY trip_id
)
INSERT INTO silver.trip_events ({_COLUMNS_SQL})
SELECT {_COLUMNS_SQL} FROM incoming
ON CONFLICT (trip_id) DO UPDATE SET
    pickup_at       = EXCLUDED.pickup_at,
    dropoff_at      = EXCLUDED.dropoff_at,
    pickup_zone_id  = EXCLUDED.pickup_zone_id,
    dropoff_zone_id = EXCLUDED.dropoff_zone_id,
    fare_amount     = EXCLUDED.fare_amount,
    tip_amount      = EXCLUDED.tip_amount,
    total_amount    = EXCLUDED.total_amount,
    payment_type    = EXCLUDED.payment_type,
    is_valid_trip   = EXCLUDED.is_valid_trip,
    event_time      = EXCLUDED.event_time,
    emit_time       = EXCLUDED.emit_time,
    ingested_at     = EXCLUDED.ingested_at,
    lag_seconds     = EXCLUDED.lag_seconds
RETURNING (xmax = 0) AS inserted, is_valid_trip
"""


@dataclass
class BatchResult:
    """O que este lote fez com o Silver. Somado em `data/state/counters.json`."""

    consumed: int = 0
    valid_stored: int = 0
    invalid: int = 0
    late_dropped: int = 0
    duplicates_dropped: int = 0
    rejected: int = 0

    @property
    def accounted(self) -> int:
        """Fecha a conta: consumidos = válidos + inválidos + atrasados + duplicados + rejeitados."""
        return (
            self.valid_stored
            + self.invalid
            + self.late_dropped
            + self.duplicates_dropped
            + self.rejected
        )


def to_silver_events(df: DataFrame) -> DataFrame:
    """Evento cru do Bronze → colunas do Silver, com `is_valid_trip` calculado.

    `event_time`/`dropoff_at` chegam como texto naive de NY; `to_timestamp` na sessão UTC mantém o
    relógio de parede como está (não desloca), que é o contrato das colunas `TIMESTAMP` do Silver.
    `emit_time` carrega offset no texto e vira o instante correto.
    """
    pickup_at = F.to_timestamp("event_time")
    dropoff_at = F.to_timestamp("dropoff_at")
    emit_time = F.to_timestamp("emit_time")
    duration_seconds = F.unix_timestamp(dropoff_at) - F.unix_timestamp(pickup_at)

    violates = (
        F.col("trip_id").isNull()
        | F.col("pickup_zone_id").isNull()
        | F.coalesce(duration_seconds <= 0, F.lit(False))
        | F.coalesce(F.col("fare_amount") < 0, F.lit(False))
        | F.coalesce(F.col("total_amount") > FARE_CEILING, F.lit(False))
    )
    lag_seconds = (
        F.unix_timestamp("ingested_at") - F.unix_timestamp(emit_time)
    ).cast("double")

    return df.select(
        F.col("trip_id"),
        pickup_at.alias("pickup_at"),
        dropoff_at.alias("dropoff_at"),
        F.col("pickup_zone_id").cast("int"),
        F.col("dropoff_zone_id").cast("int"),
        F.col("fare_amount").cast("decimal(10, 2)"),
        F.col("tip_amount").cast("decimal(10, 2)"),
        F.col("total_amount").cast("decimal(10, 2)"),
        F.col("payment_type").cast("short"),
        (~violates).alias("is_valid_trip"),
        pickup_at.alias("event_time"),
        emit_time.alias("emit_time"),
        F.col("ingested_at"),
        lag_seconds.alias("lag_seconds"),
    )


def copy_frame(df: DataFrame) -> DataFrame:
    """Colunas prontas para o COPY: instantes como texto no fuso da sessão (UTC).

    Ver a decisão 4 do módulo: `collect()` de `TIMESTAMP` usa o fuso local do driver e deslocaria as
    colunas de NY. Texto no formato `TS_FORMAT` atravessa sem reinterpretação.
    """
    return df.select(
        F.col("trip_id"),
        F.date_format("pickup_at", TS_FORMAT).alias("pickup_at"),
        F.date_format("dropoff_at", TS_FORMAT).alias("dropoff_at"),
        F.col("pickup_zone_id"),
        F.col("dropoff_zone_id"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.col("payment_type"),
        F.col("is_valid_trip"),
        F.date_format("event_time", TS_FORMAT).alias("event_time"),
        F.date_format("emit_time", TS_FORMAT).alias("emit_time"),
        F.date_format("ingested_at", TS_FORMAT).alias("ingested_at"),
        F.col("lag_seconds"),
    )


def apply_late_filter(df: DataFrame, watermark: datetime | None) -> DataFrame:
    """Descarta evento com atraso > 10 min em relação ao watermark do lote anterior.

    Watermark NULL é o primeiro lote: aceita tudo. O `watermark` vem do Postgres como `datetime`
    naive (relógio de NY); o corte é serializado em texto para o Spark não interpretá-lo no fuso
    local do driver. Linha sem `event_time` não é julgada aqui: conta como `rejected` adiante.
    """
    if watermark is None:
        return df
    cutoff = (watermark - LATE_TOLERANCE).strftime("%Y-%m-%d %H:%M:%S")
    return df.where(
        F.col("event_time").isNull() | (F.col("event_time") >= F.to_timestamp(F.lit(cutoff)))
    )


def batch_watermark(batch: DataFrame) -> str | None:
    """Menor dos `max(event_time)` por partição do Kafka, como texto.

    A partição mais lenta é quem manda: só abaixo dela o pipeline tem certeza de que nenhum input
    vai voltar. Texto para não passar `datetime` do Spark pelo fuso local do driver.
    """
    row = (
        batch.groupBy("kafka_partition")
        .agg(F.max(F.to_timestamp("event_time")).alias("partition_max"))
        .agg(F.date_format(F.min("partition_max"), TS_FORMAT).alias("watermark"))
        .first()
    )
    return row["watermark"] if row else None


def storable_filter(df: DataFrame) -> DataFrame:
    """Linha que o Postgres aceita: `trip_id` (PK) e `pickup_at` (`NOT NULL`) não nulos."""
    return df.where(F.col("trip_id").isNotNull() & F.col("pickup_at").isNotNull())


def load_counters(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_counters(path: Path, counters: dict[str, Any]) -> None:
    """Grava de forma atômica: o leitor nunca vê o arquivo pela metade."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(counters, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def merge_counters(path: Path, delta: BatchResult) -> dict[str, Any]:
    """Soma o lote aos contadores acumulados (incrementais e auditáveis)."""
    counters = load_counters(path)
    for field in fields(delta):
        counters[field.name] = int(counters.get(field.name, 0)) + getattr(delta, field.name)
    counters["batches"] = int(counters.get("batches", 0)) + 1
    counters["updated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    save_counters(path, counters)
    return counters


def process_batch(
    batch: DataFrame,
    batch_id: int,
    dsn: str = DEFAULT_DSN,
    counters_path: Path | str = DEFAULT_COUNTERS_PATH,
) -> BatchResult:
    """Valida, filtra atraso e faz o upsert de um micro-lote; devolve o que foi feito.

    Tudo numa transação: se o upsert ou a atualização do watermark falhar, o lote é repetido pelo
    checkpoint com o watermark antigo, e o filtro decide o mesmo que decidiria na primeira tentativa.
    """
    prepared = to_silver_events(batch).persist()
    try:
        total = prepared.count()
        result = BatchResult(consumed=int(total or 0))
        if result.consumed == 0:
            return result

        # Menor máximo por partição do lote (ver decisão 1 do módulo), como texto.
        batch_max = batch_watermark(batch)

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # `now()` é TIMESTAMPTZ; sem fuso fixado o servidor decide o deslocamento da sessão.
                cur.execute("SET TIME ZONE 'UTC'")
                cur.execute("SELECT watermark FROM silver.stream_state WHERE id = 1")
                row = cur.fetchone()
                watermark = row[0] if row else None

            accepted = apply_late_filter(prepared, watermark).persist()
            storable = storable_filter(accepted).persist()
            try:
                accepted_rows = accepted.count()
                storable_rows = storable.count()
                result.late_dropped = result.consumed - accepted_rows
                result.rejected = accepted_rows - storable_rows

                with conn.cursor() as cur:
                    cur.execute(_CREATE_STAGING_SQL)
                    if storable_rows:
                        rows = copy_frame(storable).collect()
                        with cur.copy(
                            f"COPY {_STAGING} ({_COLUMNS_SQL}) FROM STDIN"
                        ) as copy:
                            for row in rows:
                                copy.write_row(row)

                        cur.execute(_UPSERT_SQL)
                        returned = cur.fetchall()
                        inserted = sum(1 for was_insert, _ in returned if was_insert)
                        result.invalid = sum(
                            1
                            for was_insert, valid in returned
                            if was_insert and not valid
                        )
                        result.valid_stored = inserted - result.invalid
                        # Linha que entrou no COPY e não virou INSERT: duplicata no lote ou
                        # conflito com linha já existente. É a dedup do PK, não do Spark.
                        result.duplicates_dropped = storable_rows - inserted

                    if batch_max is not None:
                        cur.execute(
                            "UPDATE silver.stream_state "
                            "SET watermark = %s, updated_at = now() WHERE id = 1",
                            (batch_max,),
                        )
            finally:
                accepted.unpersist()
                storable.unpersist()

        if result.accounted != result.consumed:
            raise RuntimeError(
                f"lote {batch_id} não fecha a conta: consumidos={result.consumed}, "
                f"contabilizados={result.accounted} ({asdict(result)})"
            )

        merge_counters(Path(counters_path), result)
        return result
    finally:
        prepared.unpersist()
