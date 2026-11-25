#!/usr/bin/env bash
# Cria o tópico de entrada. Idempotente: rodar duas vezes não falha.
set -euo pipefail

TOPIC="${TOPIC:-trips.raw}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-86400000}"   # 24 h: o run oficial do Dia 14 dura ~5 h

if docker compose exec -T redpanda rpk topic describe "$TOPIC" >/dev/null 2>&1; then
  echo "tópico $TOPIC já existe"
else
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" -c "retention.ms=$RETENTION_MS"
fi

docker compose exec -T redpanda rpk topic describe "$TOPIC"
