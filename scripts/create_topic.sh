#!/usr/bin/env bash
# Cria o tópico de entrada. Idempotente e tolerante a broker subindo.
set -euo pipefail

TOPIC="${TOPIC:-trips.raw}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-86400000}"   # 24 h: o replay completo de jan–mar dura ~5 h

# Numa máquina limpa, `make up` retorna antes do healthcheck fechar (até 60 s).
# Esperar aqui evita que o `describe` falhe por broker indisponível e o `set -e` aborte.
for attempt in 1 2 3 4 5 6 7 8; do
  if docker compose exec -T redpanda rpk cluster info >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 8 ]; then
    echo "erro: o broker não respondeu em 40 s; confira 'make up' e 'make ps'" >&2
    exit 1
  fi
  echo "aguardando o broker responder (${attempt}/8)..."
  sleep 5
done

if docker compose exec -T redpanda rpk topic describe "$TOPIC" >/dev/null 2>&1; then
  echo "tópico $TOPIC já existe"
else
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" -c "retention.ms=$RETENTION_MS"
fi

docker compose exec -T redpanda rpk topic describe "$TOPIC"
