.DEFAULT_GOAL := help
COMPOSE := docker compose
TOPIC ?= trips.raw

# Spark 4 não homologa o JDK 27 (default do repo): fixar o 21 instalado via pacman.
JAVA_HOME ?= /usr/lib/jvm/java-21-openjdk
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

up: ## Sobe Redpanda, Console e Postgres
	$(COMPOSE) up -d
	@$(COMPOSE) ps

down: ## Derruba os containers (mantém os volumes)
	$(COMPOSE) down

clean: ## Derruba e apaga os volumes (perde tópico e banco)
	$(COMPOSE) down -v

topic: ## Cria o tópico trips.raw (idempotente)
	@bash scripts/create_topic.sh

venv: ## Cria a venv com Python 3.12 (uv lê .python-version)
	uv venv --python 3.12

deps: venv ## Instala as dependências do requirements.txt na venv
	uv pip install -r requirements.txt

silver: ## Aplica o DDL do schema Silver no Postgres (idempotente)
	@$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U stream -d streaming < scripts/init_silver.sql

ps: ## Estado dos containers
	@$(COMPOSE) ps

logs: ## Logs do Redpanda
	@$(COMPOSE) logs -f redpanda

.PHONY: help up down clean topic venv deps silver ps logs
