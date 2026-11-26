.DEFAULT_GOAL := help
COMPOSE := docker compose
TOPIC ?= trips.raw

# Sample replay. Full run: `make produce RATE=0 LIMIT=0`.
# DUPES/DIRTY inject duplicates and invalid events: `make produce DUPES=50 DIRTY=100`.
RATE ?= 500
LIMIT ?= 20000
DUPES ?= 0
DIRTY ?= 0
RAW_DIR ?= $(HOME)/projetos/tlc-elt/data/raw

# Spark → Bronze job. `STREAM_ARGS="--available-now"` processes what exists and exits.
JARS_DIR ?= jars
STREAM_ARGS ?=
BRONZE_ARGS ?=

# Spark 4 does not support JDK 27 (the repo default): pin 21.
# `:=` on purpose: a JAVA_HOME exported in the shell (archlinux-java, SDKMAN) cannot win silently.
JAVA_HOME := /usr/lib/jvm/java-21-openjdk
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

help: ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-14s %s\n", $$1, $$2}'

up: ## Start Redpanda, Console and Postgres
	$(COMPOSE) up -d
	@$(COMPOSE) ps

down: ## Stop the containers (keeps the volumes)
	$(COMPOSE) down

clean: ## Stop and delete the volumes (loses topic, database and counters)
	$(COMPOSE) down -v
	@rm -f data/state/counters.json

topic: ## Create the trips.raw topic (idempotent)
	@bash scripts/create_topic.sh

produce: ## Replay TLC Parquet into trips.raw (RATE=500 LIMIT=20000; 0 = no limit)
	@$(COMPOSE) exec -T redpanda rpk cluster info >/dev/null 2>&1 || { echo "redpanda is down — run 'make up'"; exit 1; }
	@parts=$$($(COMPOSE) exec -T redpanda rpk topic describe $(TOPIC) 2>/dev/null | awk '$$1 == "PARTITIONS" {print $$2}'); \
	  test "$$parts" = "6" || { echo "topic $(TOPIC) has partitions=$${parts:-0} (expected 6) — run 'make topic'"; exit 1; }
	@uv run python producer/producer.py --rate $(RATE) --limit $(LIMIT) --dupes $(DUPES) --dirty $(DIRTY) --topic $(TOPIC) --raw-dir $(RAW_DIR)

test: ## Run the pytest of the producer and the streaming job
	@uv run pytest -q

lint: ## ruff on producer, streaming and tests
	@uv run ruff check producer streaming scripts tests

venv: ## Create the Python 3.12 venv (uv reads .python-version; keeps the existing one)
	uv venv --python 3.12 --allow-existing

deps: venv ## Install requirements.txt into the venv (idempotent)
	uv pip install -r requirements.txt

check-java: ## Fail early if JDK 21 is not in place
	@test -x $(JAVA_HOME)/bin/java || { echo "JDK 21 missing at $(JAVA_HOME)"; echo "install it with: sudo pacman -S --needed jdk21-openjdk"; exit 1; }

smoke: check-java ## Spark local starts and counts 10, with the same JAVA_HOME as the jobs
	@uv run python -c "import pyspark; print('pyspark', pyspark.__version__)"
	@uv run python -c "from pyspark.sql import SparkSession as S; s=S.builder.master('local[2]').appName('smoke').getOrCreate(); print('count', s.range(10).count(), '| java', s.sparkContext._jvm.System.getProperty('java.version')); s.stop()"

jars: check-java ## Download the Kafka connector from Maven into jars/ (once, with network)
	@uv run python scripts/fetch_jars.py --dest $(JARS_DIR)

stream: check-java ## Consume trips.raw and write Bronze + Silver (Ctrl+C stops; STREAM_ARGS=--available-now)
	@uv run python -m streaming.job --jars-dir $(JARS_DIR) $(STREAM_ARGS)

bronze: ## Count the Bronze in Parquet (BRONZE_ARGS=--check counts nulls and repeated offsets)
	@uv run python scripts/count_bronze.py --dir data/bronze $(BRONZE_ARGS)

bronze-reset: ## Delete the Bronze and its checkpoint (lab only)
	@rm -rf data/bronze data/checkpoints/bronze
	@echo "bronze and checkpoint deleted"

silver: ## Apply the Silver DDL to Postgres (idempotent)
	@$(COMPOSE) ps --status running --services | grep -qx postgres || { echo "postgres is down — run 'make up'"; exit 1; }
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming < scripts/init_silver.sql

silver-reset: ## DROP + recreate the Silver (lab only: the schema is frozen after the restart test)
	@$(COMPOSE) ps --status running --services | grep -qx postgres || { echo "postgres is down — run 'make up'"; exit 1; }
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming -c 'DROP SCHEMA IF EXISTS silver CASCADE'
	@$(MAKE) --no-print-directory silver
	@rm -f data/state/counters.json
	@test ! -d data/checkpoints/bronze || echo "warning: Bronze checkpoint untouched — run 'make bronze-reset' to reprocess"

silver-stats: ## Silver counts (rows, distinct trip_ids and invalid)
	@$(COMPOSE) exec -T postgres psql -U stream -d streaming -c "SELECT count(*) AS rows, count(DISTINCT trip_id) AS distinct_ids, count(*) FILTER (WHERE NOT is_valid_trip) AS invalid FROM silver.trip_events"

windows: ## Zone x 5-min table and its reconciliation against the valid Silver rows
	@$(COMPOSE) exec -T postgres psql -U stream -d streaming -c "SELECT count(*) AS windows, sum(trips) AS trips, sum(revenue)::numeric(12,2) AS revenue FROM silver.zone_demand_5min"
	@$(COMPOSE) exec -T postgres psql -U stream -d streaming -c "SELECT (SELECT coalesce(sum(trips), 0) FROM silver.zone_demand_5min) AS window_trips, (SELECT count(*) FROM silver.trip_events WHERE is_valid_trip) AS valid_trips, (SELECT coalesce(sum(trips), 0) FROM silver.zone_demand_5min) = (SELECT count(*) FROM silver.trip_events WHERE is_valid_trip) AS reconciled"

windows-backfill: ## Rebuild zone_demand_5min from trip_events (repair; stop the job first)
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming < scripts/backfill_windows.sql
	@$(MAKE) --no-print-directory windows

restart-test: ## kill -9 mid-stream, restart from the checkpoint and compare (lab; destructive)
	@bash scripts/restart_test.sh

counters: ## Show the incremental Silver counters
	@test -f data/state/counters.json && uv run python -m json.tool data/state/counters.json || echo "no counters yet"

ps: ## Container status
	@$(COMPOSE) ps

logs: ## Redpanda logs
	@$(COMPOSE) logs -f redpanda

.PHONY: help up down clean topic produce test lint venv deps check-java smoke jars stream bronze bronze-reset silver silver-reset silver-stats windows windows-backfill counters ps logs restart-test
