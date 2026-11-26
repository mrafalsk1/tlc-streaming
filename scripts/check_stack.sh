#!/usr/bin/env bash
# Valida o ambiente: containers de pé, tópico com 6 partições, retenção 24 h, portas respondendo.
set -uo pipefail
cd "$(dirname "$0")/.."

echo "== docker compose ps =="
docker compose ps

echo
echo "== tópico =="
docker compose exec -T redpanda rpk topic describe trips.raw

echo
echo "== retenção efetiva =="
docker compose exec -T redpanda rpk topic describe trips.raw | grep -E '^retention\.ms' || true

echo
echo "== postgres 5434 =="
docker compose exec -T postgres psql -U stream -d streaming -c 'select current_database(), version();'

echo
echo "== console 8081 =="
(code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8081/); echo "HTTP $code" || echo "console não respondeu")
