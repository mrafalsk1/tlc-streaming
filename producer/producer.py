"""Replay dos Parquet do TLC para o tópico ``trips.raw`` (Dia 3).

    uv run python producer/producer.py --rate 500 --limit 20000

Cada arquivo é de um mês; ordenar por pickup dentro do arquivo e concatenar jan→mar dá a ordem
cronológica global (é o que impede quase todo evento de nascer "atrasado"). Key = zona de embarque,
``acks=all`` + idempotência (o replay precisa ser reprodutível: mesmos eventos, mesmos ``trip_id``).

``--rate 0`` desliga o freio e ``--limit 0`` replaya os arquivos inteiros.
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

if __package__ in (None, ""):  # executado como script: sys.path não inclui a raiz do repo
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.schema import SOURCE_COLUMNS, build_event, dumps, partition_key

DEFAULT_RAW_DIR = Path(os.environ.get("TLC_RAW_DIR", "~/projetos/tlc-elt/data/raw")).expanduser()
DEFAULT_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
CHUNK_ROWS = 10_000
MESSAGE_TIMEOUT_MS = 30_000
# O flush espera mais que o timeout da mensagem, senão ele desiste antes de os callbacks de erro
# chegarem e o processo sairia com "0 falhas" depois de perder mensagem em silêncio.
FLUSH_TIMEOUT_S = 45.0
MAX_ERROR_SAMPLES = 5

log = logging.getLogger("producer")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay do TLC Parquet para o tópico trips.raw.")
    parser.add_argument("--rate", type=float, default=500.0, help="eventos por segundo (0 = sem freio)")
    parser.add_argument("--limit", type=int, default=20_000, help="total de eventos (0 = tudo)")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"diretório dos Parquet (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--files",
        type=Path,
        nargs="+",
        help="Parquets explícitos, na ordem dada (vence --raw-dir)",
    )
    parser.add_argument("--brokers", default=DEFAULT_BROKERS, help=f"default: {DEFAULT_BROKERS}")
    parser.add_argument("--topic", default="trips.raw")
    parser.add_argument("--log-every", type=float, default=5.0, help="intervalo do log de ev/s")
    args = parser.parse_args(argv)
    if args.rate < 0:
        parser.error("--rate não pode ser negativo")
    if args.limit < 0:
        parser.error("--limit não pode ser negativo")
    if args.log_every <= 0:
        parser.error("--log-every deve ser positivo")
    return args


def resolve_files(args: argparse.Namespace) -> list[Path]:
    if args.files:
        files = list(args.files)
    else:
        files = sorted(args.raw_dir.glob("yellow_tripdata_*.parquet"))
    if not files:
        raise SystemExit(f"nenhum Parquet em {args.raw_dir} (use --files ou TLC_RAW_DIR)")
    return files


def iter_rows(path: Path, chunk_rows: int = CHUNK_ROWS) -> Iterator[dict[str, Any]]:
    """Linhas do arquivo em ordem de ``tpep_pickup_datetime``.

    Ordena o mês inteiro: só assim a amostra de ``--limit`` sai cronológica. A tabela Arrow fica
    viva durante a iteração (é o custo aceito); o ``to_pylist`` vai em blocos para não estourar
    memória com 3 milhões de dicts.
    """
    table = pq.read_table(path, columns=list(SOURCE_COLUMNS))
    indices = pc.sort_indices(table, sort_keys=[("tpep_pickup_datetime", "ascending")])
    for start in range(0, table.num_rows, chunk_rows):
        chunk = table.take(indices.slice(start, chunk_rows))
        yield from chunk.to_pylist()


def pace(started: float, sent: int, rate: float) -> None:
    """Segura o ritmo em ``rate`` ev/s, dormindo só o que falta (``rate 0`` = sem freio)."""
    if rate <= 0:
        return
    delay = started + sent / rate - time.monotonic()
    if delay > 0.005:  # abaixo disso o sleep custa mais que o benefício
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
    failures = 0
    error_samples: list[str] = []

    def on_delivery(err: Any, msg: Any) -> None:
        nonlocal failures
        if err is not None:
            failures += 1
            if len(error_samples) < MAX_ERROR_SAMPLES:
                error_samples.append(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {err}")

    def send(event: dict[str, Any]) -> None:
        """Envia sem estourar a fila local quando o replay vai sem freio."""
        key = partition_key(event)
        payload = dumps(event)
        while True:
            try:
                producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                return
            except BufferError:
                # fila local cheia: serve callbacks para liberar espaço e tenta de novo
                producer.poll(0.5)

    sent = 0
    skipped = 0
    started = time.monotonic()
    paced_start: float | None = None
    window_start = started
    window_count = 0

    try:
        for path in files:
            if args.limit and sent >= args.limit:
                break
            log.info("replay %s", path.name)
            for row in iter_rows(path):
                if args.limit and sent >= args.limit:
                    break
                if row.get("tpep_pickup_datetime") is None:
                    skipped += 1
                    continue
                send(build_event(row, datetime.now(UTC)))
                sent += 1
                window_count += 1
                producer.poll(0)  # serve os callbacks de entrega (não drena a fila de produção)
                if paced_start is None:
                    # âncora depois do primeiro envio: a ordenação do mês não vira burst de crédito
                    paced_start = time.monotonic()
                pace(paced_start, sent, args.rate)

                now = time.monotonic()
                if now - window_start >= args.log_every:
                    log.info(
                        "enviados=%d  %.0f ev/s no total  %.0f ev/s nos ultimos %.0fs",
                        sent,
                        sent / (now - started),
                        window_count / (now - window_start),
                        now - window_start,
                    )
                    window_start = now
                    window_count = 0
    finally:
        pending = producer.flush(FLUSH_TIMEOUT_S)
        if pending:
            failures += pending
            error_samples.append(
                f"{pending} mensagens sem confirmação em {FLUSH_TIMEOUT_S:.0f}s (message.timeout "
                f"{MESSAGE_TIMEOUT_MS}ms)"
            )

    elapsed = time.monotonic() - started
    log.info(
        "fim: %d enviados, %d ignorados (pickup nulo), %d falhas de entrega em %.1fs = %.0f ev/s",
        sent,
        skipped,
        failures,
        elapsed,
        sent / elapsed if elapsed else 0.0,
    )
    if failures:
        log.error("primeiras falhas: %s", "; ".join(error_samples))
        return 1
    if args.limit and sent < args.limit:
        log.error("limite de %d não alcançado: só %d eventos disponíveis", args.limit, sent)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
