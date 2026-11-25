# Notas de execução — evidências e decisões

Arquivo de apoio interno (pt-BR). Os números oficiais do README saem daqui, sempre medidos.

## Dia 2 — toolchain e schema Silver (25/11/2026)

Instalado: `uv 0.13.10`, `jdk21-openjdk 21.0.13.1`, Python 3.12.13 na `.venv`
(11 pacotes do `requirements.txt`).

Cheques verdes:

- `uv run python -c "import pyspark; print(pyspark.__version__)"` → `4.3.0`.
- Spark local `range(10).count()` → `10`, `java 21.0.13.1` (JDK 21, não o 27 default).
- `make silver` duas vezes → 2ª vez só `NOTICE ... skipping` e `INSERT 0 0`.
- `\dt silver.*` → `trip_events`, `zone_demand_5min`, `stream_state`.

Tropeços: `pacman -S` falhou com 404 em `jdk21/nss/nspr/uv` (DB local à frente do espelho) —
resolvido com `pacman -Syy`. `pandas` foi pinado em 2.3.3 porque o PySpark 4.3 avisa que não
suporta pandas >= 3.

### Decisões que os Dias 5–10 precisam respeitar

1. **Linha inválida entra no Silver com `is_valid_trip = false`**, não é descartada.
   Inválido = `trip_id` nulo, `trip_duration <= 0`, `fare < 0`, `pickup_zone_id` nulo, tarifa acima
   do teto de sanidade. Consequências: `zone_demand_5min` só recebe `is_valid_trip = true`; e a
   reconciliação do Dia 10 conta "gravados" = válidos e "inválidos" = linhas com flag falsa — a
   mesma linha nunca entra nas duas parcelas.
2. **`stream_state.watermark` NULL = "aceita tudo"**, não "descarta tudo". O primeiro lote grava
   `max(event_time)`; sem esse caso explícito, o filtro `event_time < watermark - 10 min` devolve
   NULL e o lote inteiro viraria `late_dropped`.
3. **`duplicates_dropped` vem de `RETURNING (xmax = 0)`** (ou `DO NOTHING` + rowcount), nunca de
   `ON CONFLICT` silencioso: `DO UPDATE` conta insert e update igual e o contador ficaria zero.
4. **A janela (Dia 8) sobrescreve** `trips`/`revenue` com `EXCLUDED.*`. Em `outputMode("update")`
   o valor da janela é cumulativo; somar dobraria a cada micro-batch.
5. **UTC nos dois lados**: `spark.sql.session.timeZone=UTC` no job e `SET TIME ZONE 'UTC'` na
   conexão do Silver. Sem isso, `now()` (TIMESTAMPTZ, TZ do servidor) e `event_time` (naive, TZ do
   JVM) se deslocam em silêncio.
6. **Spark roda via `make`**, nunca por `uv run` solto: o `JAVA_HOME` do JDK 21 está pinado no
   `Makefile` (`:=` + `check-java`) e não vale num shell novo, que pegaria o JDK 27 default. Daí
   `make smoke` hoje e `make stream` no Dia 5.
7. **Schema congelado a partir do Dia 9** (o restart test depende do dado). Até lá, mudança de DDL é
   `make silver-reset`, porque `CREATE TABLE IF NOT EXISTS` não altera tabela existente e o erro
   aparece longe da causa (coluna inexistente no meio do `foreachBatch` = retry infinito do lote).

## Dia 3 — producer: Parquet → JSON no tópico (25/11/2026)

`producer/schema.py` (schema explícito do evento + `trip_id`) e `producer/producer.py` (replay com
`--rate`/`--limit`, key = zona, `acks=all` + idempotência, log de ev/s a cada 5 s).

Cheques verdes:

- `uv run ruff check producer` → `All checks passed!`.
- `uv run python producer/producer.py --rate 500 --limit 20000` → **20.000 enviados, 0 ignorados,
  0 falhas de entrega em 40,0 s = 500 ev/s** (relógio do shell: 40,28 s).
- Sem freio (`--rate 0`) a mesma leitura sai a **~4.100 ev/s** (2.000 eventos em 0,5 s, já incluindo a
  ordenação do mês inteiro) — teto do produtor, a confirmar com 100k no Dia 4.
- Determinismo: dois runs de 2.000 num tópico descartável (`trips.smoke`, apagado em seguida) →
  4.000 mensagens, **2.000 `trip_id` distintos, cada um exatamente 2×, 0 fora do 1x/2x**.
- `rpk topic consume trips.raw -o start -n 3` → JSON com `trip_id`, `event_time` (pickup), `emit_time`
  (UTC) e `key` = `pickup_zone_id` (114, 48, 48 nas três primeiras).
- `retention.ms` do tópico = `86400000` (`DYNAMIC_TOPIC_CONFIG`), como o Dia 1 pediu.

Decisões do producer:

1. **`event_time` é o pickup**; o Silver deriva `pickup_at` dele em vez de o JSON repetir a coluna.
2. **`trip_id` = `sha1(event_time|dropoff_at|pu_zone|do_zone|total_amount)[:16]`**, com texto estável
   (`%.2f` na tarifa, vazio no que é nulo) — é isso que faz os dois runs darem os mesmos ids.
3. **NaN/Inf viram `null`**: JSON não tem esses valores e o Jackson do Spark os recusa por padrão
   (`ALLOW_NON_NUMERIC_NUMBERS` desligado).
4. **Zona nula usa a key `unknown`**: key nula faria o produtor distribuir sem ordem por zona; a
   string mantém o hash determinístico.
5. **Linha sem pickup é ignorada** (contador `ignorados`): sem `event_time` não há ordem nem janela.
   O resto da invalidez (tarifa, duração, zona) **não** é filtrado aqui — é política do Silver
   (`is_valid_trip`).
6. **Ordena o arquivo inteiro em memória** (Arrow `sort_indices`, ~200 MB por mês): com `--limit`
   pequeno o custo de leitura é o mesmo, mas a amostra sai cronológica de verdade.

Tropeço: rodando como script (`uv run python producer/producer.py`), o `sys.path[0]` é a pasta
`producer/` e `from producer.schema import ...` falhava com "'producer' is not a package" — resolvido
com `producer/__init__.py` mais uma linha que põe a raiz do repo no `sys.path` quando não há pacote.
Também: `# noqa: E402` no import foi acusado pelo ruff (`RUF100`, regra não habilitada no default);
o import ficou sem `noqa`.
