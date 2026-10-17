.PHONY: help install up down reset ingest ingest-once ingest-logs ingest-stop api api-dev api-stop consume reconcile check-db-outage lint format test test-integration test-all smoke smoke-restart migrate coverage

help: ## List the available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv from uv.lock
	uv sync --locked

up: ## Start Kafka and PostgreSQL, wait until healthy, create topics
	docker compose up -d --wait kafka postgres
	docker compose run --rm kafka-init

down: ## Stop all services, including the poller (data volumes are kept)
	docker compose --profile ingest --profile api down

reset: ## Stop all services and delete their data volumes
	docker compose --profile ingest --profile api down --volumes

ingest: up ## Build and start the pipeline (migrate, poller, consumer) on the live USGS feed
	docker compose --profile ingest build migrate
	docker compose --profile ingest up -d --wait poller consumer

ingest-once: ## Poll the live feed once from this machine and exit
	uv run python -m earth_hub.ingestion --once

ingest-logs: ## Follow the poller's and consumer's logs
	docker compose --profile ingest logs -f poller consumer

ingest-stop: ## Stop the poller and consumer, leaving Kafka and PostgreSQL running
	docker compose --profile ingest stop poller consumer

api: up ## Build and start the API container at http://127.0.0.1:$${API_PORT:-8000}/docs
	docker compose --profile api build migrate
	docker compose --profile api up -d --wait api

api-dev: ## Run the API on this machine with auto-reload
	uv run uvicorn earth_hub.api.main:app --reload --host 127.0.0.1 --port $${API_PORT:-8000}

api-stop: ## Stop the API container
	docker compose --profile api stop api

consume: ## Run the consumer from this machine (Ctrl+C to stop)
	uv run python -m earth_hub.consumers

reconcile: ## Check every Kafka message has a database row, and the tables' invariants
	uv run python -m earth_hub.reconcile

check-db-outage: ## Stop PostgreSQL mid-stream and check nothing is lost
	./scripts/check_db_outage.sh

lint: ## Check style and formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Fix style and formatting
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run unit tests (no Docker needed)
	uv run pytest -m "not integration"

test-integration: ## Run integration tests (needs `make up`)
	uv run pytest -m integration

test-all: ## Run unit and integration tests together in one process (needs `make up`)
	uv run pytest

smoke: test-integration ## Smoke-test the running infrastructure

smoke-restart: ## Restart Kafka and PostgreSQL and check data survives
	./scripts/smoke_restart.sh

migrate: ## Apply database migrations
	uv run alembic upgrade head

coverage: ## Run unit tests with a coverage report
	uv run pytest -m "not integration" --cov=earth_hub --cov-report=term-missing
