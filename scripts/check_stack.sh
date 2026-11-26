#!/usr/bin/env bash
# Validates the environment: containers up, topic with 6 partitions, 24 h retention, ports answering.
set -uo pipefail
cd "$(dirname "$0")/.."

echo "== docker compose ps =="
docker compose ps

echo
echo "== topic =="
docker compose exec -T redpanda rpk topic describe trips.raw

echo
echo "== effective retention =="
docker compose exec -T redpanda rpk topic describe trips.raw | grep -E '^retention\.ms' || true

echo
echo "== postgres 5434 =="
docker compose exec -T postgres psql -U stream -d streaming -c 'select current_database(), version();'

echo
echo "== console 8081 =="
(code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8081/); echo "HTTP $code" || echo "console did not answer")
