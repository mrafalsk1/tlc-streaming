"""Replay dos Parquet do TLC para o tópico ``trips.raw``.

    uv run python producer/producer.py --rate 500 --limit 20000

Cada arquivo é de um mês; ordenar por pickup dentro do arquivo e concatenar jan→mar dá a ordem
cronológica global (é o que impede quase todo evento de nascer "atrasado"). Key = zona de embarque,
``acks=all`` + idempotência (o replay precisa ser reprodutível: mesmos eventos, mesmos ``trip_id``).

``--rate 0`` desliga o freio e ``--limit 0`` replaya os arquivos inteiros. ``--dupes N`` reenvia
N eventos (mesmo payload, mesmo ``trip_id``) e ``--dirty N`` injeta N eventos inválidos com id único
— as duas flags existem para o Silver provar dedup e validação de verdade.
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

from producer.schema import (
    SOURCE_COLUMNS,
    build_dirty_event,
    build_event,
    dumps,
    partition_key,
)

DEFAULT_RAW_DIR = Path(os.environ.get("TLC_RAW_DIR", "~/projetos/tlc-elt/data/raw")).expanduser()
DEFAULT_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
CHUNK_ROWS = 10_000
MESSAGE_TIMEOUT_MS = 30_000
# O flush espera mais que o timeout da mensagem, senão ele desiste antes de os callbacks de erro
# chegarem e o processo sairia com "0 falhas" depois de perder mensagem em silêncio.
FLUSH_TIMEOUT_S = 45.0
MAX_ERROR_SAMPLES = 5
# Broker que não recebe: insistir só repete o timeout. 100 falhas já é sinal de que algo está errado.
ABORT_AFTER_FAILURES = 100
# O `--dirty` desloca o `event_time` até `N` segundos à frente do ponto do stream. O filtro de atraso
# do Silver usa 10 min; 5 min de folga impedem que o sujo empurre o max e faça os reais seguintes
# parecerem atrasados (o oposto do objetivo da injeção).
MAX_DIRTY_AHEAD_S = 300


class AbortReplay(RuntimeError):
    """Falhas de entrega demais: melhor parar do que seguir empilhando timeouts."""

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
    parser.add_argument("--dupes", type=int, default=0, help="duplicatas exatas a injetar")
    parser.add_argument(
        "--dirty",
        type=int,
        default=0,
        help=f"eventos inválidos a injetar, motivos ciclando em DIRTY_KINDS (máx {MAX_DIRTY_AHEAD_S})",
    )
    parser.add_argument("--log-every", type=float, default=5.0, help="intervalo do log de ev/s")
    args = parser.parse_args(argv)
    if args.rate < 0:
        parser.error("--rate não pode ser negativo")
    if args.limit < 0:
        parser.error("--limit não pode ser negativo")
    if args.dupes < 0:
        parser.error("--dupes não pode ser negativo")
    if args.dirty < 0:
        parser.error("--dirty não pode ser negativo")
    if args.dirty > MAX_DIRTY_AHEAD_S:
        parser.error(
            f"--dirty máximo é {MAX_DIRTY_AHEAD_S}: o sujo adia o event_time até N s e o filtro de "
            "atraso do Silver usa 10 min"
        )
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
    delivery_failures = 0
    error_samples: list[str] = []

    def on_delivery(err: Any, msg: Any) -> None:
        nonlocal delivery_failures
        if err is not None:
            delivery_failures += 1
            if len(error_samples) < MAX_ERROR_SAMPLES:
                error_samples.append(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {err}")

    def send(event: dict[str, Any]) -> None:
        """Envia sem estourar a fila local; para quando o broker já recusou demais."""
        key = partition_key(event)
        payload = dumps(event)
        while True:
            if delivery_failures >= ABORT_AFTER_FAILURES:
                raise AbortReplay(
                    f"{delivery_failures} falhas de entrega: o broker não está recebendo"
                )
            try:
                producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                return
            except BufferError:
                # fila local cheia: serve callbacks para liberar espaço e tenta de novo
                producer.poll(0.5)

    sent = 0
    dupes_sent = 0
    dirty_sent = 0
    skipped = 0
    started = time.monotonic()
    paced_start: float | None = None
    window_start = started
    window_count = 0
    last_event: dict[str, Any] | None = None
    aborted = False

    try:
        for path in files:
            if args.limit and sent >= args.limit:
                break
            log.info("replay %s", path.name)
            # a leitura/ordenação de cada mês não vira crédito de ritmo (rajada na fronteira)
            paced_start = None
            for row in iter_rows(path):
                if args.limit and sent >= args.limit:
                    break
                if row.get("tpep_pickup_datetime") is None:
                    skipped += 1
                    continue
                event = build_event(row, datetime.now(UTC))
                send(event)
                last_event = event
                sent += 1
                window_count += 1
                # Injeção: duplicata é o mesmo payload de novo; sujo é derivado do evento
                # atual, com trip_id único e motivo de invalidez ciclando em DIRTY_KINDS.
                if dupes_sent < args.dupes:
                    send(event)
                    dupes_sent += 1
                if dirty_sent < args.dirty:
                    send(build_dirty_event(event, dirty_sent, datetime.now(UTC)))
                    dirty_sent += 1
                producer.poll(0)  # serve os callbacks de entrega (não drena a fila de produção)
                if paced_start is None:
                    # âncora depois do primeiro envio: a ordenação do mês não vira burst de crédito
                    paced_start = time.monotonic()
                pace(paced_start, sent + dupes_sent + dirty_sent, args.rate)

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
        # --dupes/--dirty maiores que o stream: o que sobrou sai ancorado no último evento real
        # (contrato das flags é "injetar N" — o contador do Silver precisa bater com o pedido).
        if last_event is not None:
            while dupes_sent < args.dupes:
                send(last_event)
                dupes_sent += 1
            while dirty_sent < args.dirty:
                send(build_dirty_event(last_event, dirty_sent, datetime.now(UTC)))
                dirty_sent += 1
        elif args.dupes or args.dirty:
            log.warning(
                "sem evento real para ancorar a injeção: 0 de %d duplicatas e 0 de %d sujos",
                args.dupes,
                args.dirty,
            )
    except AbortReplay as exc:
        log.error("%s", exc)
        aborted = True
    finally:
        pending = producer.flush(FLUSH_TIMEOUT_S)
        if pending and len(error_samples) < MAX_ERROR_SAMPLES:
            error_samples.append(
                f"{pending} mensagens sem confirmação em {FLUSH_TIMEOUT_S:.0f}s (message.timeout "
                f"{MESSAGE_TIMEOUT_MS}ms)"
            )

    elapsed = time.monotonic() - started
    total = sent + dupes_sent + dirty_sent
    log.info(
        "fim: %d enviados, %d duplicatas, %d sujos, %d ignorados (pickup nulo), "
        "%d falhas de entrega, %d sem confirmação, em %.1fs = %.0f ev/s reais "
        "(%.0f ev/s no broker)",
        sent,
        dupes_sent,
        dirty_sent,
        skipped,
        delivery_failures,
        pending,
        elapsed,
        sent / elapsed if elapsed else 0.0,
        total / elapsed if elapsed else 0.0,
    )
    if delivery_failures or pending or aborted:
        if error_samples:
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
