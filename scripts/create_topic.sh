#!/usr/bin/env bash
# Creates the input topic. Idempotent and tolerant to a broker that is still starting.
set -euo pipefail

TOPIC="${TOPIC:-trips.raw}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-86400000}"   # 24 h: the full Jan–Mar replay lasts ~5 h

# On a clean machine `make up` returns before the healthcheck closes (up to 60 s).
# Waiting here keeps `describe` from failing on an unavailable broker and `set -e` aborting.
for attempt in 1 2 3 4 5 6 7 8; do
  if docker compose exec -T redpanda rpk cluster info >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 8 ]; then
    echo "error: the broker did not answer in 40 s; check 'make up' and 'make ps'" >&2
    exit 1
  fi
  echo "waiting for the broker to answer (${attempt}/8)..."
  sleep 5
done

if docker compose exec -T redpanda rpk topic describe "$TOPIC" >/dev/null 2>&1; then
  echo "topic $TOPIC already exists"
else
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" -c "retention.ms=$RETENTION_MS"
fi

docker compose exec -T redpanda rpk topic describe "$TOPIC"
