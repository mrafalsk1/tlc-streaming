# Notas técnicas — evidências e decisões

Notas de engenharia do pipeline. Os números oficiais do README saem daqui, sempre medidos.

## Toolchain e schema Silver

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

### Decisões que o pipeline precisa respeitar

1. **Linha inválida entra no Silver com `is_valid_trip = false`**, não é descartada.
   Inválido = `trip_id` nulo, `trip_duration <= 0`, `fare < 0`, `pickup_zone_id` nulo, tarifa acima
   do teto de sanidade. Consequências: `zone_demand_5min` só recebe `is_valid_trip = true`; e a
   reconciliação ponta a ponta conta "gravados" = válidos e "inválidos" = linhas com flag falsa — a
   mesma linha nunca entra nas duas parcelas.
2. **`stream_state.watermark` NULL = "aceita tudo"**, não "descarta tudo". O primeiro lote grava
   `max(event_time)`; sem esse caso explícito, o filtro `event_time < watermark - 10 min` devolve
   NULL e o lote inteiro viraria `late_dropped`.
3. **`duplicates_dropped` vem de `RETURNING (xmax = 0)`** (ou `DO NOTHING` + rowcount), nunca de
   `ON CONFLICT` silencioso: `DO UPDATE` conta insert e update igual e o contador ficaria zero.
4. **A janela tumbling sobrescreve** `trips`/`revenue` com `EXCLUDED.*`. Em `outputMode("update")`
   o valor da janela é cumulativo; somar dobraria a cada micro-batch.
5. **UTC nos dois lados**: `spark.sql.session.timeZone=UTC` no job e `SET TIME ZONE 'UTC'` na
   conexão do Silver. Sem isso, `now()` (TIMESTAMPTZ, TZ do servidor) e `event_time` (naive, TZ do
   JVM) se deslocam em silêncio.
6. **Spark roda via `make`**, nunca por `uv run` solto: o `JAVA_HOME` do JDK 21 está pinado no
   `Makefile` (`:=` + `check-java`) e não vale num shell novo, que pegaria o JDK 27 default. Daí
   `make smoke` e `make stream`.
7. **Schema congelado depois do restart test** (o teste depende do dado). Até então, mudança de DDL
   é `make silver-reset`, porque `CREATE TABLE IF NOT EXISTS` não altera tabela existente e o erro
   aparece longe da causa (coluna inexistente no meio do `foreachBatch` = retry infinito do lote).

## Producer: Parquet → JSON no tópico

`producer/schema.py` (schema explícito do evento + `trip_id`) e `producer/producer.py` (replay com
`--rate`/`--limit`, key = zona, `acks=all` + idempotência, log de ev/s a cada 5 s).

Cheques verdes:

- `uv run ruff check producer` → `All checks passed!`.
- `uv run python producer/producer.py --rate 500 --limit 20000` → **20.000 enviados, 0 ignorados,
  0 falhas de entrega em 40,0 s = 500 ev/s** (relógio do shell: 40,28 s). Depois da revisão de
  código, a mesma medição dá **40,5 s = 494 ev/s** — a diferença é a leitura/ordenação do mês
  (~0,4 s) que passou a ficar fora da âncora do ritmo; o log de janela fica em 500 ev/s.
- Sem freio (`--rate 0`) a mesma leitura sai a **~4.100 ev/s** (2.000 eventos em 0,5 s, já incluindo a
  ordenação do mês inteiro) — teto do produtor (confirmado abaixo com 100k).
- Determinismo: dois runs de 2.000 num tópico descartável (`trips.smoke`, apagado em seguida) →
  4.000 mensagens, **2.000 `trip_id` distintos, cada um exatamente 2×, 0 fora do 1x/2x**.
- `rpk topic consume trips.raw -o start -n 3` → JSON com `trip_id`, `event_time` (pickup), `emit_time`
  (UTC) e `key` = `pickup_zone_id` (114, 48, 48 nas três primeiras).
- `retention.ms` do tópico = `86400000` (`DYNAMIC_TOPIC_CONFIG`), conforme o setup do tópico.

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

### Revisão de código (o que mudou)

Aplicado:

- **Flush podia esconder perda** (alto): `message.timeout.ms` (60 s) era maior que o timeout do
  `flush` (30 s), e `pending > 0` só virava log — o processo podia sair com "0 falhas". Agora
  `message.timeout.ms = 30 s`, `flush(45 s)` e `pending` soma em `failures` (exit 1).
- **`BufferError` no replay sem freio** (médio): a fila local (100k mensagens) podia estourar e
  abortar o run com traceback; agora `poll(0.5)` e reenvio, dentro de um `send()`.
- **Burst inicial** (médio): o `--rate` contava o tempo da ordenação do mês como crédito; a âncora do
  ritmo passou para o primeiro envio (a média de 20k não mudou).
- Amostras de erro limitadas a 5, com contador separado; comentário do `poll(0)` corrigido (ele serve
  callbacks de entrega, não drena a fila de produção).
- `make produce` distingue "redpanda fora do ar" de "tópico ausente" e exige **6 partições**
  (`retention.ms` já era conferido no setup).
- README: diagrama marca `producer` como implementado e Spark/Bronze/Silver/dbt como alvo.

Descartado, com motivo:

- **"`sort_indices` não é estável"**: a doc do pyarrow diz o contrário — "computes an array of indices
  that define a **stable** sort". Empate mantém a ordem do arquivo, e a checagem de determinismo
  compara o multiconjunto de `trip_id`; nada a mudar.
- **Filtrar pickup nulo em dois lugares** (produtor e `build_event`): é defesa em profundidade — o
  produtor evita a exceção no caminho quente e `build_event` continua recusando linha sem pickup.
- **`EVENT_FIELDS` divergir do dict**: vira teste (`tuple(event) == EVENT_FIELDS`), que é
  onde a checagem paga; em runtime seria custo por evento.

Ajustes complementares aplicados:

- **Rajada na fronteira de mês**: a âncora do `--rate` é zerada a cada arquivo, então a
  leitura/ordenação do mês seguinte não entra como crédito de ritmo.
- **Broker fora do ar**: `send()` aborta o replay depois de `ABORT_AFTER_FAILURES = 100` falhas de
  entrega (`AbortReplay`, exit 1) em vez de seguir até o fim empilhando timeouts.
- **Contagem dupla**: "falhas de entrega" (callbacks) e "sem confirmação" (`pending` do flush) agora
  são contadores separados no log; a amostra de `pending` respeita `MAX_ERROR_SAMPLES`.
- `make produce` passa `--topic $(TOPIC)` ao produtor (antes o guard checava um tópico e o produtor
  escrevia em `trips.raw`).
- Contabilidade conferida com broker morto: `--brokers localhost:9999 --limit 150` → 150 enviados,
  **150 falhas de entrega, 0 sem confirmação**, exit 1 em 31,0 s (tudo estoura em `message.timeout`;
  nada sai como "0 falhas").

O que o ingest do Bronze confirma:

- `emit_time` sai como `2026-11-25T19:06:16.482+00:00`: o Spark 4 aplica
  `spark.sql.session.timeZone` ao converter string com offset para `TimestampType`. Se o JSON real
  mostrar deslocamento, trocar por epoch millis.
- Ler o tópico com `from_json` + schema explícito, nunca `inferSchema`: `payment_type` é int64 em
  todos os arquivos, mas um lote todo nulo inferiria outro tipo e o append no Bronze conflitaria.
- Key `unknown` é só transporte (o payload tem `pickup_zone_id`); não fazer `cast(key AS int)`.
- `trip_id` usa `total_amount` formatado em `%.2f`: corrida com mais casas colidiria no PK do Silver.

## Testes, injeção e teto de throughput

`tests/test_schema.py` (contrato do evento) e `tests/test_producer.py` (ordenação, ritmo, flags e
contabilidade), com `pytest.ini` (`pythonpath = .` — sem isso o pytest só põe `tests/` no
`sys.path` e `import producer` falha).

Cheques verdes:

- `uv run pytest -q` → **16 passed** em 0,42 s.
- `uv run ruff check producer tests` → `All checks passed!`.
- Injeção ao vivo em `trips.smoke` (1.000 reais + 50 duplicatas + 100 sujos, tópico apagado depois):
  **1.150 mensagens, 1.100 `trip_id` distintos, 50 ids com exatamente 2 cópias, 0 fora do 1x/2x**.
  Pelas regras do Silver saem 115 ids "sujos": os 100 injetados (25 por motivo) mais **15 que já
  vêm sujos do TLC** no primeiro milhar (14 tarifas negativas e 1 corrida de duração zero) — o
  Silver vai contá-los como `invalid` também.
- **100k sem freio (`RATE=0`)**: 2,3 s = **42.785 ev/s**, 0 falhas, 0 sem confirmação; o high
  watermark por partição soma exatamente **100.000** (10.075–24.801). É o teto do produtor; o job
  de ingest não precisa acompanhar isso em local mode, mas o número fica medido.
- **Inválidos que já existem no dado**: nos mesmos 100k (jan/2024 em ordem de pickup), **1.579
  linhas reais** violam as regras do Silver — 1.545 tarifa negativa, 35 duração não positiva, 0 zona
  nula, 0 acima do teto. Logo o `invalid` do Silver será `100 injetados + 1.579 reais`; comparar com
  100 seco daria falso negativo.

Decisões das flags:

1. **`--dupes N` reenvia o mesmo payload logo depois do original** (mesmo `trip_id`, mesma key):
   duplicata exata, que é o que o Silver precisa para provar o `ON CONFLICT DO NOTHING`.
2. **`--dirty N` deriva do evento atual**, com `event_time` deslocado `seq + 1` s: o `trip_id` sai
   único (não vira duplicata) e o evento fica no ponto do stream, sem cair no filtro de atraso.
   O motivo cicla em `DIRTY_KINDS` (`pickup_zone_null`, `non_positive_duration`,
   `negative_fare`, `fare_over_ceiling`), então `--dirty 4` já cobre as quatro regras.
3. **`--limit` conta só eventos reais**: injeção é extra, e o log final separa `enviados`,
   `duplicatas` e `sujos`. Se a injeção passar do total do stream, a sobra sai ancorada no último
   evento real — o contrato é "injetar N", não "injetar até N".
4. **O ritmo (`--rate`) conta todas as mensagens produzidas**, não só as reais: o ev/s medido é o do
   broker, e injeção não pode virar rajada de graça.
5. **Teto de sanidade = 10.000** (`FARE_CEILING` em `schema.py`): o mesmo número que o Silver vai usar
   no `is_valid_trip` — um lugar só para mudar.

Tropeços: o assert da minha verificação esperava 1.050 ids distintos (1.000 reais + 50 duplicatas) e
deu 1.100 — duplicata repete id, então 1.000 reais + 100 sujos = 1.100; corrigido e repetido. E a
primeira versão do `--dirty` injetava no máximo um sujo por evento real: com `--limit 3 --dirty 4`
saíam 3; o teste de contagem pegou, e agora a sobra é injetada ao final.

### Revisão de código (o que mudou)

Aplicado:

- **`--dirty` limitado a 300 s** (`MAX_DIRTY_AHEAD_S`): o sujo adia o `event_time` até N segundos; com
  N perto dos 10 min do filtro de atraso, ele empurraria o max e os reais seguintes pareceriam
  atrasados. `parse_args` recusa acima de 300 e há teste.
- **`AbortReplay` marca `aborted`**: o exit 1 não depende mais só de `delivery_failures`/`pending`.
  Teste novo com um producer que recusa todas as entregas cobre esse caminho.
- **ev/s do log separa reais e broker**: a injeção conta no ritmo, mas o teto de 42.785 ev/s é sem
  injeção — as sobras de `--dupes/--dirty` maiores que o stream também não são paced (só existem
  quando N > stream).
- **Testes novos**: id dourado `bc7cdc0e1cdc29bd` (mudar a fórmula trocaria todos os ids e quebraria
  a idempotência entre versões), shift único entre ciclos de `DIRTY_KINDS`, `dropoff_at` nulo com
  base não mutada, cap do `--dirty` e falha de entrega.
- Help do `--dirty` corrigido: os motivos **ciclam** em `DIRTY_KINDS` (com 100 são 25 de cada), não
  "um por motivo".

Contrato do Silver (achado alto, corrigido antes de implementar):

1. As **cinco** regras de invalidez são as do schema Silver: `trip_id` nulo, `pickup_zone_id` nulo,
   `trip_duration <= 0`, `fare_amount < 0` e `total_amount > 10.000`. Além delas, valor que não cabe
   no `NUMERIC(10,2)` vira `NULL` no cast e a linha nasce inválida — perder a tarifa no caminho e
   gravar a linha como válida seria mentira. A primeira versão do contrato listava só três;
   sem zona nula e teto, metade dos sujos entraria como válida (`invalid` cairia para 50 + 1.579).
2. **Ordem**: validação → filtro de atraso → upsert (dedup no destino). Os contadores saem do
   resultado do upsert: `invalid` = linhas gravadas com `is_valid_trip = false`, `duplicates_dropped`
   = conflitos. Assim a reconciliação fecha: `graváveis válidos + invalid + late_dropped +
   duplicates_dropped = consumidos`.
3. **A primeira corrida de jan/2024 em ordem de pickup é um outlier de 2002-12-31 com tarifa −6,5**,
   logo uma das 50 duplicatas é de um evento já inválido. No cenário de 100k, 100 sujos e 50
   duplicatas: `invalid = 1.679` (1.579 reais + 100 sujos gravados) e `duplicates_dropped = 50`; a
   duplicata inválida conta só como duplicata.
4. **Watermark do lote anterior**: se o Silver filtrar atraso com o max do próprio lote, o outlier de
   2002 (primeiro evento do 100k) vira `late_dropped`; o contrato manda guardar o max do lote anterior.

Risco latente: `FARE_CEILING` mora em `producer/schema.py`. O job do Silver precisa importar do
pacote (ou repetir o shim de `sys.path` do produtor) para o teto não virar segunda definição.

## Bronze

Feito: `streaming/schema.py` (schema explícito amarrado a `producer.schema.EVENT_FIELDS`),
`streaming/job.py` (Kafka → Parquet), `scripts/fetch_jars.py`, `scripts/count_bronze.py`, alvos
`make jars/stream/bronze/bronze-reset`, e a suíte com 30 testes.

### Números medidos

- 100k do tópico → **100.000 linhas, 18 arquivos Parquet, 0 `trip_id` nulo, 100.000 pares
  `(kafka_partition, kafka_offset)` distintos**, ~15 s de ponta a ponta com JVM fria. São **3**
  micro-lotes (49.998 + 49.996 + 6): o teto de 50k é dividido entre as 6 partições, então ele é
  alvo, não tamanho exato de lote.
- Segunda execução com o mesmo checkpoint: **0 linhas novas** (offset vem do checkpoint).
- `rpk group describe spark-tlc-stream` → grupo existe, `TOTAL-LAG 0`, `MEMBERS 0` depois de parar.
- Partição em UTC confirmada: `ingest_date=2026-11-26/hour=6` com o relógio local em 03:29 (UTC-3).

### O que doeu

- **Spark 4 usa Ivy 2.5.2 e o cache é `~/.ivy2.5.2/`**, não `~/.ivy2/`; o nome do jar também vem
  achatado (`org.apache.spark_spark-sql-kafka-0-10_2.13-4.3.0.jar`). A primeira versão do
  `fetch_jars.py` procurava `~/.ivy2/jars` e `startswith("spark-sql-kafka")`, então baixou tudo e
  "não achou" nada. Agora o download usa um **Ivy dedicado** (`jars/.ivy`): os jars copiados são
  exatamente os que o POM do conector resolveu, sem pegar a `kafka-clients` mais nova de outro
  projeto da máquina — a alternativa de varrer `~/.ivy2*` e escolher a maior versão tem esse risco e
  foi descartada.
- Só **4** artefatos faltam no `pyspark/jars`: `spark-sql-kafka`, `spark-token-provider-kafka`,
  `kafka-clients` e `commons-pool2` (o Spark traz `commons-pool` 1.5.4, que não serve). Copiar os
  11 resolvidos pelo Ivy duplicaria ~63 MB de hadoop/lz4/snappy que o Spark já tem.
- `to_utc_timestamp(current_timestamp(), tz)` **não** devolve o instante em `tz`: ele lê o relógio
  local da sessão como se fosse daquele fuso. Com sessão em NY e tz Kiritimati a partição saiu 18 h
  errada — o teste pegou. Correção: sessão em UTC + `to_date`/`hour` direto, e
  `add_ingest_columns` **falha** se a sessão não estiver em UTC em vez de gravar pasta errada.

### Contrato para o Silver

- Bronze guarda `event_time`/`dropoff_at` como **texto** (naive de NY): o cast para timestamp é do
  Silver, com a sessão em **UTC** e o relógio de parede preservado (`to_timestamp` de texto naive
  não desloca); não inferir fuso no ingest.
- Bronze tem inválidos e atrasados de propósito; `is_valid_trip` e o filtro de atraso nascem no
  Silver, não aqui.
- `spark.sql.session.timeZone=UTC` vale para os dois ramos: mantém a partição do Bronze estável e
  faz `emit_time`/`ingested_at` virarem instantes corretos. O Silver **não** reutiliza
  `add_ingest_columns` (aquela guarda de partição é do Bronze).

### Risco latente

- `kafka.group.id` fixo faz o Spark avisar que é "unsafe": duas queries simultâneas com o mesmo
  grupo brigam por partições. No lab há uma query só; o restart test vai matar e subir de novo — por isso
  `kafka.session.timeout.ms=10000` (default 45 s) para o coordenador liberar o consumidor morto
  rápido. Se o restart ainda demorar, olhar isso antes de culpar o checkpoint.

## Silver

`streaming/silver.py` + o ramo `foreachBatch` de `streaming/job.py`: valida, filtra atraso pelo
`silver.stream_state` e faz upsert em `silver.trip_events` (PK `trip_id`). O Bronze continua cru:
inválido e atrasado entram no Parquet.

### Números medidos

Cenário 100k + 50 duplicatas + 100 sujos, drenado em 3 lotes (49.997 + 49.998 + 155):

- consumidos **100.150** = válidos **98.421** + inválidos **1.679** + atrasados **0** + duplicados
  **50** + rejeitados **0**. A conta fecha.
- `invalid` = 1.579 linhas reais do TLC + 100 sujos injetados. O detalhe por regra (25 zona nula,
  1.571 tarifa negativa, 60 duração não positiva, 25 acima do teto) soma 1.681 porque duas linhas
  violam duas regras — o contador de linhas é 1.679.
- `silver.trip_events`: **100.100 linhas = 100.100 `trip_id` distintos**; as 50 duplicatas nunca
  viraram linha.
- Bronze: **100.150 linhas, 18 arquivos, 0 `trip_id` nulo, 100.150 pares
  `(kafka_partition, kafka_offset)` distintos**.
- Segunda execução com o mesmo checkpoint: 0 lote — Bronze, Silver e contadores inalterados.
- **Reexecução com o checkpoint do Bronze apagado** (mesmo tópico, 100.150 eventos): o Bronze
  dobra (200.300 linhas, 100.150 pares `(partition, offset)` repetidos — at-least-once), o Silver
  fica em 100.100 e os contadores **não somam de novo**: `silver.batch_runs` continua com 3 linhas
  e os mesmos totais; o `counters.json` só muda o `updated_at`.
- `pickup_at` sem deslocamento: mínimo `2002-12-31 22:59:39` (outlier do TLC) e máximo
  `2024-01-02 11:02:23`. `lag_seconds` (emit → ingest) varia com o backlog do dreno; não é medida
  de latência de regime, que fica para o `metrics.py`.
- `stream_state.watermark` termina em `2024-01-02 11:01:48`, o menor máximo por partição (o maior
  era `11:02:23`).

### Decisões

1. **Watermark = menor dos `max(event_time)` por partição** do lote anterior. O replay é
   particionado por zona e o teto de offsets é dividido entre as partições, então elas avançam em
   ritmos de event-time diferentes: no probe do Bronze, na altura do offset 6.000 a partição 3
   estava em `07:59` e a partição 5 em `13:42` (~6 h de diferença). Com o **máximo global**, um dreno
   de 50k por lote marcou **9.014 linhas** como `late_dropped` sem nenhuma estar atrasada. O menor
   máximo é o ponto até onde todos os inputs avançaram; dentro de cada partição a ordem é preservada
   pelo producer, então não há falso positivo. Consequência honesta: a tolerância efetiva é o span de
   event-time do lote anterior + 10 min, então num lote largo um atraso real só é pego quando o
   watermark passa dele — `late_dropped` fecha a conta, mas não promete pegar todo fora de ordem.
   Limitação conhecida: o watermark considera só as partições presentes no lote; uma partição sem
   tráfego por vários lotes (e que volta com evento antigo) não está representada. No TLC todas as
   zonas têm tráfego contínuo.
2. **Contadores saem do upsert, num livro-caixa no banco**: `RETURNING (xmax = 0)` separa insert de
   update; `invalid` = insert com flag falsa e `duplicates_dropped` = linha do COPY que não virou
   insert (duplicata dentro do lote ou conflito com linha existente). Duplicata de inválido conta só
   como duplicata. O resultado vai para `silver.batch_runs` na **mesma transação**, com PK nas faixas
   de offset do lote: um lote reexecutado pelo checkpoint não soma duas vezes, e
   `data/state/counters.json` é a soma da tabela, não do processo.
3. **`DISTINCT ON (trip_id)` antes do `INSERT`**: `ON CONFLICT DO UPDATE` recusa afetar a mesma
   linha duas vezes na mesma instrução; a duplicata que chega no mesmo micro-lote precisa ser
   colapsada antes.
4. **`trip_id` nulo não entra**: a PK não aceita nulo, então a linha vai para `rejected` (e continua
   no Bronze). O mesmo para `pickup_at`/`emit_time` nulos (`NOT NULL`) e `trip_id` com NUL (o psycopg
   recusa 0x00 no COPY; TAB e quebra de linha ele escapa). Nenhum desses apareceu no cenário.
5. **COPY para tabela temporária** em vez de `execute_values` (não existe mais no psycopg 3.3):
   `CREATE TEMP TABLE ... ON COMMIT DROP` + COPY + `INSERT ... SELECT`. A transação inteira
   (watermark lido, upsert, watermark gravado) é um commit só: lote repetido pelo checkpoint vê o
   mesmo watermark e decide o mesmo.

### O que doeu

- **`collect()` de `TIMESTAMP` usa o fuso local do driver**, não `spark.sql.session.timeZone`. Na
  máquina em UTC-3, `pickup_at` de NY saía 3 h atrás e `lag_seconds` ganhava 3 h — o teste pegou
  (`datetime(2024, 1, 1, 0, 12)` virava `2023-12-31 21:12`). Correção: as colunas de tempo vão para o
  COPY como texto (`date_format` na sessão UTC) e o Postgres recebe a string com
  `SET TIME ZONE 'UTC'`. O watermark entra no Spark também como texto (`to_timestamp(lit(...))`);
  um `datetime` naive no `lit` seria interpretado no fuso do driver.
- **Os primeiros dois drenos perderam evento real**: 9.014 linhas "atrasadas" num e 6.015 por lote
  no outro, todas em ordem. A causa não era o dado, era o watermark global contra partições
  desalinhadas em event-time. O probe por partição no Bronze (`min/max` e `event_time` em offsets
  redondos) foi o que mostrou a diferença de ~6 h entre fatias.
- **ANSI ligado por padrão no Spark 4**: `to_timestamp('nao-e-data')` e `cast` de `1e20` para
  `decimal(10,2)` **levantam exceção** em vez de devolver nulo. Um único registro ruim faria o lote
  falhar para sempre no mesmo offset (e o Bronze ganharia um append por tentativa). Trocado por
  `try_to_timestamp`/`try_cast`; valor fora da faixa vira `NULL` e a linha é marcada inválida.

## Janela por zona

`silver.zone_demand_5min` (PK `pickup_zone_id, window_start`): `trips`, `revenue` e
`avg_duration_min` por janela tumbling de 5 min, só `is_valid_trip = true` (a média de duração também
exige `dropoff_at`). A janela é **recalculada** no Postgres, para as janelas que o lote tocou, lendo
`silver.trip_events` — na mesma transação do upsert (`_WINDOW_SQL` em `streaming/silver.py`). Limites
alinhados ao epoch (`date_bin`), o mesmo limite do `window()` do Spark porque 5 min divide a hora.

### Desvio do plano

O plano pedia agregação em Spark com `outputMode("update")` e watermark de 15 min. Trocado, e o
motivo é mecânico: agregado só do lote não consegue ser correto **e** reexecutável ao mesmo tempo —
substituir o valor apaga a contribuição dos lotes anteriores; somar conta duas vezes quando o
checkpoint reexecuta o lote. Recalcular da fonte é idempotente por construção (reexecução reescreve
os mesmos números) e resolve o evento atrasado aceito dentro da tolerância, que tem que atualizar a
janela dele, não a do lote. O watermark do Spark deixa de existir: o filtro de atraso do Silver já é
o único lugar onde atraso é decidido, e a janela herda isso. Custo aceito: duas varreduras por janela
tocada (recálculo e checagem, ambas usando `trip_events_zone_pickup_idx`), sem estado crescente.

### Números medidos

Cenário 100k + 50 duplicatas + 100 sujos, drenado em 3 lotes:

- **21.238 janelas**; `sum(trips) = 98.421 = count(*) where is_valid_trip` — a reconciliação fecha
  (`make windows`).
- 19 janelas tocadas ficaram com `trips = 0` (só linhas inválidas naquela zona/janela). Ficam na
  tabela com zero de propósito: a reconciliação soma a tabela, então a linha não pode sumir.
- Recálculo independente direto de `trip_events`: 21.219 janelas com linha válida, **0 divergências**
  nas 21.219; as 19 restantes são exatamente as vazias. 0 janelas desalinhadas e
  `window_end = window_start + 5 min` em todas.
- **Replay completo** (checkpoint do Bronze apagado **e** `stream_state.watermark = NULL`): os
  100.150 eventos passam como **100.150 duplicados**, `late=0`, nenhum insert; Silver, contadores e
  janelas **idênticos** — `md5` igual antes e depois sobre
  `(pickup_zone_id, window_start, window_end, trips, revenue, avg_duration_min)`; `updated_at` é
  reescrito de propósito e **não serve** como marcador incremental para o Gold. Nenhuma linha nova em
  `batch_runs`; `counters.json` só com `updated_at` novo.
- Execução seguinte com o checkpoint no fim: **0 lote**.
- `make windows-backfill` (reconstrução total): 21.219 janelas, `sum(trips)` e `revenue` idênticos e
  reconciliação fechando. Ele não recria as 19 janelas vazias — não há como derivá-las de
  `trip_events`; a soma é a mesma, o número de linhas não.
- `revenue` total **3.057.192,10**; janela maior: zona 79, `02:40`, 58 corridas, `1.457,06`.

### O que doeu

- `LEFT JOIN` + `count(e.trip_id)`: com `count(*)` a janela vazia contaria 1 e a reconciliação
  fecharia errada por 19 no cenário. O teste do SQL (transação revertida, 2.000 linhas) mostrou a
  janela vazia antes do dreno — foi o que pegou.
- `wsl -d archlinux -- bash -lc` com `||`, `$` e aspas: três tentativas de query inline viraram
  erro de parsing. Script em `Temp` rodado via `/mnt/c/...` é o caminho que funciona.
- **Replay com o watermark intacto não prova nada**: o primeiro replay depois do dreno deu
  `late=49.997` no primeiro lote e `late=49.367` no segundo — o `stream_state.watermark` ficou no fim
  do dado e o filtro descartou **99.364** eventos como atrasados. Os **786** dos últimos 10 min
  passaram como duplicados (`631` + `155`), recalcularam as janelas deles e nada mudou — então esse
  replay prova a armadilha e a idempotência, mas só nesses 786. O teste completo exige limpar o
  watermark antes: aí os 100.150 entram como duplicados e o recálculo roda em todas as 21.238
  janelas, com o mesmo `md5`. Fica a armadilha: `make bronze-reset` sozinho não é replay.

### Revisão de código (o que mudou)

Achados do revisor independente, aplicados:

- **Lost update entre dois escritores (alto, reproduzido)**: o recálculo lê o agregado antes de
  pegar o lock da linha da janela. Dois lotes na mesma janela (query reiniciada enquanto o consumidor
  antigo morre, dois jobs no mesmo tópico) calculavam sobre snapshots que não se viam; o segundo
  esperava o lock e gravava o número velho — `sum(trips) < count(*)` permanente. Correção:
  `pg_advisory_xact_lock` antes do recálculo, na mesma transação. O lock cobre só o trabalho da
  janela e de quebra tira o deadlock entre dois escritores; com um escritor, custo zero.
- **Invariante sem asserção (médio)**: até aqui só o `make windows` conferia. Agora, antes do
  commit, o lote compara `sum(trips)` das janelas tocadas com `count(*)` de válidas nas mesmas
  janelas e levanta erro se divergir — a transação desfaz e o checkpoint repete o lote. Uma regressão
  (trocar `count(e.trip_id)` por `count(*)`, perder o `is_valid_trip`) falha em vez de gravar errado.
- **Sem reparo (médio)**: `scripts/backfill_windows.sql` + `make windows-backfill` reconstroem a
  tabela inteira a partir de `trip_events` (restore parcial, tabela truncada à mão).
- Documentação corrigida: `avg_duration_min` também exige `dropoff_at`; "mesma fronteira do
  `window()`" vale porque 5 min divide a hora; `updated_at` é reescrito a cada recálculo e não é
  marcador incremental; `make windows` entrou no bloco "Running it" e a tabela no diagrama.

Riscos latentes documentados, não corrigidos: o recálculo só toca a janela **nova** — se um
`trip_id` já gravado mudar de zona ou `pickup_at` (hoje impossível, o hash cobre os dois), a janela
antiga fica com a contagem velha, e o backfill é a saída; e o `statement_timeout = '10min'` agora
cobre também a varredura da janela (no lab, 0,09 s para 50k linhas contíguas / 10.442 janelas).
