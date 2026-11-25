.DEFAULT_GOAL := help
COMPOSE := docker compose
TOPIC ?= trips.raw

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

ps: ## Estado dos containers
	@$(COMPOSE) ps

logs: ## Logs do Redpanda
	@$(COMPOSE) logs -f redpanda

.PHONY: help up down clean topic ps logs
