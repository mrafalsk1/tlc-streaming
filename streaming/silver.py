"""Ramo Silver: validação, filtro de atraso e upsert idempotente no Postgres.

O Bronze aceita tudo; é aqui que o evento é classificado. A ordem é a do contrato (docs/notes.md):
validação → filtro de atraso → upsert. A dedup **não** é stateful: quem deduplica é o
`PRIMARY KEY (trip_id)` do Postgres, então o estado do Spark não cresce com o stream.

Cinco decisões que não são óbvias:

* **Watermark do lote anterior, por partição.** O filtro lê `silver.stream_state.watermark`, gravado
  pelo lote anterior, e só depois grava o watermark deste lote. O valor é o **menor** dos
  `max(event_time)` de cada partição do Kafka: o replay é particionado por zona e o limite de
  offsets por trigger é dividido igualmente entre as partições, então elas avançam em ritmos
  diferentes de event-time (num lote de 8k, as fatias chegam a variar ~6 h). Usar o máximo global
  faria a partição mais adiantada declarar "atrasado" o que é só a vez das outras. O menor máximo é
  o ponto até onde todos os inputs avançaram — o watermark clássico. Se usasse o max do próprio
  lote, o primeiro evento do replay (outlier de 2002) já nasceria atrasado. Watermark NULL =
  primeiro lote, aceita tudo. A atualização usa `GREATEST`: watermark nunca regride.
* **`is_valid_trip` com três valores.** Comparação com nulo devolve nulo, e um nulo gravado numa
  coluna `NOT NULL` derruba o lote. Por isso cada regra é um `coalesce(..., false)` e o resultado é
  negado: nulo em `fare_amount` não invalida a linha. O teto compara o valor **arredondado** a 2
  casas, o mesmo que o `trip_id` do produtor usa (`%.2f`).
* **Contadores saem do upsert, num livro-caixa no banco.** `ON CONFLICT DO UPDATE` conta insert e
  update igual; `RETURNING (xmax = 0)` separa os dois. `invalid` = gravadas com `is_valid_trip =
  false`; `duplicates_dropped` = linhas que o upsert não inseriu (duplicata dentro do lote ou
  conflito com linha existente). A duplicata de um evento inválido conta só como duplicata. O
  resultado do lote vai para `silver.batch_runs` na mesma transação, com PK na identidade do lote
  (faixas de offset por partição): um lote reexecutado pelo checkpoint não soma duas vezes, e
  `data/state/counters.json` é a soma da tabela.
* **Tempo sai do Spark como texto.** `collect()` converte `TIMESTAMP` para o fuso **local do driver**
  (não o `spark.sql.session.timeZone`), o que deslocaria o relógio de NY e os instantes em 3 h numa
  máquina em UTC-3. `copy_frame` formata no fuso da sessão (UTC) e o Postgres recebe a string com a
  sessão em UTC: nada é reinterpretado no caminho.
* **Linha que não cabe no destino não derruba o pipeline.** `trip_id` nulo (JSON corrompido) é
  inválido mas não tem PK; `pickup_at`/`emit_time` nulos violam `NOT NULL`; `trip_id` com NUL não
  atravessa o COPY (o psycopg recusa, mesmo escapando TAB/quebra de linha). Esses casos contam em
  `rejected` e continuam no Bronze.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

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
# a tabela mantém os dois nomes por compatibilidade com a janela por zona e com a reconciliação.
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

COUNTER_COLUMNS: tuple[str, ...] = (
    "consumed",
    "valid_stored",
    "invalid",
    "late_dropped",
    "duplicates_dropped",
    "rejected",
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

_INSERT_BATCH_RUN_SQL = """
INSERT INTO silver.batch_runs
    (batch_key, consumed, valid_stored, invalid, late_dropped, duplicates_dropped, rejected)
VALUES
    (%(batch_key)s, %(consumed)s, %(valid_stored)s, %(invalid)s, %(late_dropped)s,
     %(duplicates_dropped)s, %(rejected)s)
ON CONFLICT (batch_key) DO NOTHING
"""

_SUM_COUNTERS_SQL = (
    "SELECT "
    + ", ".join(f"COALESCE(SUM({column}), 0)" for column in COUNTER_COLUMNS)
    + ", COUNT(*) FROM silver.batch_runs"
)


@dataclass
class BatchResult:
    """O que este lote fez com o Silver. Vai para `silver.batch_runs` e para os contadores."""

    consumed: int = 0
    valid_stored: int = 0
    invalid: int = 0
    late_dropped: int = 0
    duplicates_dropped: int = 0
    rejected: int = 0


def to_silver_events(df: DataFrame) -> DataFrame:
    """Evento cru do Bronze → colunas do Silver, com `is_valid_trip` calculado.

    `event_time`/`dropoff_at` chegam como texto naive de NY; `try_to_timestamp` na sessão UTC mantém
    o relógio de parede como está (não desloca), que é o contrato das colunas `TIMESTAMP` do Silver.
    `emit_time` carrega offset no texto e vira o instante correto. O `try_` é obrigatório: no Spark 4
    o ANSI vem ligado e um timestamp malformado **levanta exceção** em vez de virar nulo — um único
    registro ruim travaria o lote para sempre.
    """
    pickup_at = F.try_to_timestamp("event_time")
    dropoff_at = F.try_to_timestamp("dropoff_at")
    emit_time = F.try_to_timestamp("emit_time")
    duration_seconds = F.unix_timestamp(dropoff_at) - F.unix_timestamp(pickup_at)
    # O `trip_id` do produtor usa `%.2f` do total; comparar o double cru criaria dois ids para o
    # mesmo valor quando a diferença está além da segunda casa.
    total_amount = F.round(F.col("total_amount"), 2)
    # `try_cast`: valor fora da faixa do NUMERIC(10,2) vira NULL em vez de derrubar o lote. Mas a
    # linha não pode ser dada como válida com a tarifa perdida no caminho.
    fare_stored = F.col("fare_amount").try_cast("decimal(10, 2)")
    total_stored = F.col("total_amount").try_cast("decimal(10, 2)")
    unstorable_amount = (F.col("fare_amount").isNotNull() & fare_stored.isNull()) | (
        F.col("total_amount").isNotNull() & total_stored.isNull()
    )

    violates = (
        F.col("trip_id").isNull()
        | F.col("pickup_zone_id").isNull()
        | F.coalesce(duration_seconds <= 0, F.lit(False))
        | F.coalesce(F.col("fare_amount") < 0, F.lit(False))
        | F.coalesce(total_amount > FARE_CEILING, F.lit(False))
        | unstorable_amount
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
        fare_stored.alias("fare_amount"),
        F.col("tip_amount").try_cast("decimal(10, 2)").alias("tip_amount"),
        total_stored.alias("total_amount"),
        F.col("payment_type").try_cast("short").alias("payment_type"),
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
        F.col("event_time").isNull()
        | (F.col("event_time") >= F.try_to_timestamp(F.lit(cutoff)))
    )


def batch_bounds(batch: DataFrame) -> tuple[str, str | None]:
    """Identidade do lote e watermark, numa passada só.

    * Identidade = faixas `(min, max)` de offset por partição. É estável quando o checkpoint
      reexecuta o lote, então a PK de `silver.batch_runs` absorve a repetição.
    * Watermark = menor dos `max(event_time)` por partição, como texto. A partição mais lenta é
      quem manda: só abaixo dela o pipeline tem certeza de que nenhum input vai voltar.
    """
    rows = (
        batch.groupBy("kafka_partition")
        .agg(
            F.min("kafka_offset").alias("lo"),
            F.max("kafka_offset").alias("hi"),
            F.date_format(F.max(F.try_to_timestamp("event_time")), TS_FORMAT).alias("max_event"),
        )
        .orderBy("kafka_partition")
        .collect()
    )
    key = ";".join(f"{row.kafka_partition}:{row.lo}-{row.hi}" for row in rows)
    # Formato fixo de largura constante: a ordem lexicográfica é a ordem temporal.
    events = [row.max_event for row in rows if row.max_event is not None]
    return key, min(events) if events else None


def storable_filter(df: DataFrame) -> DataFrame:
    """Linha que o Postgres aceita.

    `trip_id` é PK e `pickup_at`/`emit_time` são `NOT NULL`; um nulo derrubaria o lote inteiro e o
    checkpoint ficaria preso no mesmo offset. NUL não atravessa o COPY (o psycopg recusa qualquer
    byte 0x00 num texto); TAB/quebra de linha/barra são escapados pelo próprio psycopg.
    """
    return df.where(
        F.col("trip_id").isNotNull()
        & F.col("pickup_at").isNotNull()
        & F.col("emit_time").isNotNull()
        & (F.instr(F.col("trip_id"), F.lit("\x00")) == 0)
    )


def save_counters(path: Path, counters: dict) -> None:
    """Grava de forma atômica: o leitor nunca vê o arquivo pela metade."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(counters, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def export_counters(conn: psycopg.Connection, path: Path) -> dict:
    """`data/state/counters.json` = soma de `silver.batch_runs` (não do processo).

    Assim um lote reexecutado pelo checkpoint não soma duas vezes, e um processo que morre depois do
    commit ainda tem os contadores do lote na próxima exportação.
    """
    with conn.cursor() as cur:
        cur.execute(_SUM_COUNTERS_SQL)
        row = cur.fetchone()
    counters = {column: int(value) for column, value in zip(COUNTER_COLUMNS, row[:-1], strict=True)}
    counters["batches"] = int(row[-1])
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

    Tudo numa transação: se o upsert, o livro-caixa ou a atualização do watermark falhar, o lote é
    repetido pelo checkpoint com o watermark antigo, e o filtro decide o mesmo que decidiria na
    primeira tentativa. A PK de `silver.batch_runs` faz a repetição não contar duas vezes.
    """
    prepared = to_silver_events(batch).persist()
    try:
        total = prepared.count()
        result = BatchResult(consumed=int(total or 0))
        if result.consumed == 0:
            return result

        batch_key, batch_max = batch_bounds(batch)

        with psycopg.connect(dsn, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                # `now()` é TIMESTAMPTZ; sem fuso fixado o servidor decide o deslocamento da sessão.
                cur.execute("SET TIME ZONE 'UTC'")
                # Postgres travado ou lock concorrente não podem parar o stream em silêncio.
                cur.execute("SET statement_timeout = '10min'")
                cur.execute("SET lock_timeout = '30s'")
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
                        distinct_ids = len({row.trip_id for row in rows})
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
                        # Checagem real, antes do commit: o DISTINCT ON tem que devolver uma linha
                        # por trip_id distinto do lote.
                        if len(returned) != distinct_ids:
                            raise RuntimeError(
                                f"lote {batch_id}: DISTINCT ON devolveu {len(returned)} linhas "
                                f"para {distinct_ids} trip_id distintos"
                            )

                    cur.execute(_INSERT_BATCH_RUN_SQL, {"batch_key": batch_key, **asdict(result)})
                    if batch_max is not None:
                        # GREATEST: um lote só com evento antigo (ou uma partição ausente) não pode
                        # fazer o watermark voltar.
                        cur.execute(
                            "UPDATE silver.stream_state SET watermark = "
                            "GREATEST(COALESCE(watermark, %s::timestamp), %s::timestamp), "
                            "updated_at = now() WHERE id = 1",
                            (batch_max, batch_max),
                        )
            finally:
                accepted.unpersist()
                storable.unpersist()

            conn.commit()
            export_counters(conn, Path(counters_path))

        return result
    finally:
        prepared.unpersist()
