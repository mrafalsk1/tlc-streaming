.DEFAULT_GOAL := help
COMPOSE := docker compose
TOPIC ?= trips.raw

# Replay de amostra (Dia 3). Run completo: `make produce RATE=0 LIMIT=0`.
RATE ?= 500
LIMIT ?= 20000
RAW_DIR ?= $(HOME)/projetos/tlc-elt/data/raw

# Spark 4 não homologa o JDK 27 (default do repo): fixar o 21.
# `:=` de propósito: um JAVA_HOME exportado no shell (archlinux-java, SDKMAN) não pode vencer em silêncio.
JAVA_HOME := /usr/lib/jvm/java-21-openjdk
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-14s %s\n", $$1, $$2}'

up: ## Sobe Redpanda, Console e Postgres
	$(COMPOSE) up -d
	@$(COMPOSE) ps

down: ## Derruba os containers (mantém os volumes)
	$(COMPOSE) down

clean: ## Derruba e apaga os volumes (perde tópico e banco)
	$(COMPOSE) down -v

topic: ## Cria o tópico trips.raw (idempotente)
	@bash scripts/create_topic.sh

produce: ## Replay do TLC Parquet para trips.raw (RATE=500 LIMIT=20000; 0 = sem limite)
	@$(COMPOSE) exec -T redpanda rpk cluster info >/dev/null 2>&1 || { echo "redpanda fora do ar — rode 'make up'"; exit 1; }
	@parts=$$($(COMPOSE) exec -T redpanda rpk topic describe $(TOPIC) 2>/dev/null | awk '$$1 == "PARTITIONS" {print $$2}'); \
	  test "$$parts" = "6" || { echo "topico $(TOPIC) com particoes=$${parts:-0} (esperado 6) — rode 'make topic'"; exit 1; }
	@uv run python producer/producer.py --rate $(RATE) --limit $(LIMIT) --topic $(TOPIC) --raw-dir $(RAW_DIR)

venv: ## Cria a venv com Python 3.12 (uv lê .python-version; preserva a existente)
	uv venv --python 3.12 --allow-existing

deps: venv ## Instala as dependências do requirements.txt na venv (idempotente)
	uv pip install -r requirements.txt

check-java: ## Falha cedo se o JDK 21 não estiver no lugar
	@test -x $(JAVA_HOME)/bin/java || { echo "JDK 21 ausente em $(JAVA_HOME)"; echo "instale com: sudo pacman -S --needed jdk21-openjdk"; exit 1; }

smoke: check-java ## Spark local sobe e conta 10, com o mesmo JAVA_HOME dos jobs
	@uv run python -c "import pyspark; print('pyspark', pyspark.__version__)"
	@uv run python -c "from pyspark.sql import SparkSession as S; s=S.builder.master('local[2]').appName('smoke').getOrCreate(); print('count', s.range(10).count(), '| java', s.sparkContext._jvm.System.getProperty('java.version')); s.stop()"

silver: ## Aplica o DDL do schema Silver no Postgres (idempotente)
	@$(COMPOSE) ps --status running --services | grep -qx postgres || { echo "postgres parado — rode 'make up'"; exit 1; }
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming < scripts/init_silver.sql

silver-reset: ## DROP + recria o Silver (só até o Dia 8; do Dia 9 em diante o schema é congelado)
	@$(COMPOSE) ps --status running --services | grep -qx postgres || { echo "postgres parado — rode 'make up'"; exit 1; }
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming -c 'DROP SCHEMA IF EXISTS silver CASCADE'
	@$(MAKE) --no-print-directory silver

ps: ## Estado dos containers
	@$(COMPOSE) ps

logs: ## Logs do Redpanda
	@$(COMPOSE) logs -f redpanda

.PHONY: help up down clean topic produce venv deps check-java smoke silver silver-reset ps logs
