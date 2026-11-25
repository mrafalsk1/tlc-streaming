"""Schema explícito do evento `trips.raw` e geração determinística do `trip_id`.

O dataset do TLC não tem id de corrida, então o id é derivado do conteúdo do evento
(`sha1(pickup|dropoff|pu_zone|do_zone|total)[:16]`): reexecutar o replay gera os mesmos ids, que é o
que permite provar a idempotência do Silver.

Dois tempos, de propósito (ver `docs/notes.md` e `ESPECIFICACAO.md` §"Dois tempos, não um"):

* ``event_time`` — pickup em hora local de NY (o TLC publica timestamp naive); dirige janela e
  watermark. É o `pickup_at` do dataset; o job do Silver deriva `pickup_at` dele em vez de repetir a
  mesma coluna no JSON.
* ``emit_time`` — relógio de parede em UTC no momento do envio; base de `lag_seconds`.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any

# Campos do evento, na ordem em que saem no JSON (documental: é o contrato do tópico).
EVENT_FIELDS: tuple[str, ...] = (
    "trip_id",
    "event_time",
    "dropoff_at",
    "pickup_zone_id",
    "dropoff_zone_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_distance",
    "emit_time",
)

# Colunas lidas do Parquet. O evento é enxuto de propósito: Bronze guarda o evento do stream, não um
# espelho do Parquet (extra, mta_tax, congestion_surcharge etc. ficam de fora).
SOURCE_COLUMNS: tuple[str, ...] = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_distance",
)


def format_naive(value: datetime | None) -> str | None:
    """Timestamp de negócio (naive, hora local de NY) em ISO 8601, com segundos."""
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def as_number(value: Any) -> float | int | None:
    """Converte NaN/Inf em ``None``: JSON não tem esses valores (Jackson do Spark recusaria)."""
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def as_zone(value: Any) -> int | None:
    """Zona TLC (`PULocationID`/`DOLocationID`): inteiro ou ``None`` (o Parquet usa NaN)."""
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value)
    return int(value)


def make_trip_id(
    event_time: str,
    dropoff_at: str | None,
    pickup_zone_id: int | None,
    dropoff_zone_id: int | None,
    total_amount: float | None,
) -> str:
    """``sha1`` dos cinco campos identitários, em texto estável (mesmo input ⇒ mesmo id)."""
    parts = (
        event_time,
        dropoff_at or "",
        "" if pickup_zone_id is None else str(pickup_zone_id),
        "" if dropoff_zone_id is None else str(dropoff_zone_id),
        "" if total_amount is None else f"{total_amount:.2f}",
    )
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def build_event(row: Mapping[str, Any], emit_time: datetime) -> dict[str, Any]:
    """Converte uma linha do Parquet no evento do tópico.

    ``row`` usa os nomes originais do TLC. Levanta ``ValueError`` quando não há pickup: sem
    ``event_time`` a linha não tem ordem nem janela, então não entra no stream (o produtor conta).
    """
    event_time = format_naive(row.get("tpep_pickup_datetime"))
    if event_time is None:
        raise ValueError("tpep_pickup_datetime ausente")

    dropoff_at = format_naive(row.get("tpep_dropoff_datetime"))
    pickup_zone_id = as_zone(row.get("PULocationID"))
    dropoff_zone_id = as_zone(row.get("DOLocationID"))
    total_amount = as_number(row.get("total_amount"))

    return {
        "trip_id": make_trip_id(
            event_time,
            dropoff_at,
            pickup_zone_id,
            dropoff_zone_id,
            total_amount,
        ),
        "event_time": event_time,
        "dropoff_at": dropoff_at,
        "pickup_zone_id": pickup_zone_id,
        "dropoff_zone_id": dropoff_zone_id,
        "fare_amount": as_number(row.get("fare_amount")),
        "tip_amount": as_number(row.get("tip_amount")),
        "total_amount": total_amount,
        "payment_type": as_number(row.get("payment_type")),
        "trip_distance": as_number(row.get("trip_distance")),
        "emit_time": emit_time.isoformat(timespec="milliseconds"),
    }


def partition_key(event: Mapping[str, Any]) -> bytes:
    """Chave da mensagem = zona de embarque; zona nula vira ``unknown`` (nunca key nula)."""
    zone = event["pickup_zone_id"]
    return b"unknown" if zone is None else str(zone).encode("ascii")


def dumps(event: Mapping[str, Any]) -> bytes:
    """Serialização compacta (sem espaços) do evento."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
