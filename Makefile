.PHONY: up-core up-lakehouse up-spark up-streaming up-airflow up-bi up-all down ps logs init-db ingest

# Prefer hyphenated docker-compose (V1) if present; else use "docker compose" (V2)
DOCKER_COMPOSE := $(shell command -v docker-compose 2>/dev/null && echo docker-compose || echo "docker compose")
DB_URI := postgresql://de_user:de_pass@localhost:5432/de_db

# ==========================
# Docker profiles
# ==========================

up-core:
	$(DOCKER_COMPOSE) --profile core up -d

up-lakehouse:
	$(DOCKER_COMPOSE) --profile core --profile lakehouse up -d

up-spark:
	$(DOCKER_COMPOSE) --profile core --profile lakehouse --profile spark up -d

up-streaming:
	$(DOCKER_COMPOSE) --profile streaming up -d

up-airflow:
	$(DOCKER_COMPOSE) --profile airflow up -d

up-bi:
	$(DOCKER_COMPOSE) --profile bi up -d

up-all:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

ps:
	$(DOCKER_COMPOSE) ps

logs:
	$(DOCKER_COMPOSE) logs -f

# ==========================
# Database & ingestion
# ==========================

init-db:
	psql $(DB_URI) -f sql/01_create_oltp_schema.sql
	psql $(DB_URI) -f sql/02_seed_sample_data.sql

ingest:
	./scripts/automate_ingestion.sh
