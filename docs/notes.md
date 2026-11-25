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
