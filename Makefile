.PHONY: help install up down reset ingest ingest-once ingest-logs ingest-stop api api-dev api-stop kafka-ui kafka-ui-stop consume reconcile check-db-outage lint format test test-integration test-e2e test-disruptive test-all regression flake-check traceability smoke smoke-restart migrate coverage

help: ## List the available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv from uv.lock
	uv sync --locked

up: ## Start Kafka and PostgreSQL, wait until healthy, create topics
	docker compose up -d --wait kafka postgres
	docker compose run --rm kafka-init

down: ## Stop all services, including the poller (data volumes are kept)
	docker compose --profile ingest --profile api --profile tools down

reset: ## Stop all services and delete their data volumes
	docker compose --profile ingest --profile api --profile tools down --volumes

ingest: up ## Build and start the pipeline (migrate, poller, consumer) on the live USGS feed
	docker compose --profile ingest build migrate
	docker compose --profile ingest up -d --wait poller consumer

ingest-once: ## Poll the live feed once from this machine and exit
	uv run python -m earth_hub.ingestion --once

ingest-logs: ## Follow the poller's and consumer's logs
	docker compose --profile ingest logs -f poller consumer

ingest-stop: ## Stop the poller and consumer, leaving Kafka and PostgreSQL running
	docker compose --profile ingest stop poller consumer

api: up ## Build and start the API container (http://127.0.0.1:8000/docs by default)
	docker compose --profile api build migrate
	docker compose --profile api up -d --wait api

api-dev: ## Run the API on this machine with auto-reload
	uv run uvicorn earth_hub.api.main:app --reload --host 127.0.0.1 --port $${API_PORT:-8000}

api-stop: ## Stop the API container
	docker compose --profile api stop api

kafka-ui: up ## Start the read-only Kafka web viewer (http://127.0.0.1:8080 by default)
	docker compose --profile tools up -d --wait kafka-ui

kafka-ui-stop: ## Stop the Kafka web viewer
	docker compose --profile tools stop kafka-ui

consume: ## Run the consumer from this machine (Ctrl+C to stop)
	uv run python -m earth_hub.consumers

reconcile: ## Check every Kafka message has a database row, and the tables' invariants
	uv run python -m earth_hub.reconcile

check-db-outage: ## Stop PostgreSQL mid-stream and check nothing is lost (disruptive)
	uv run pytest tests/disruptive --run-disruptive -k database_outage

lint: ## Check style and formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Fix style and formatting
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run unit tests (no Docker or network needed)
	uv run pytest -m "not integration and not e2e and not disruptive"

test-integration: ## Run integration tests (needs `make up`)
	uv run pytest -m integration

test-e2e: ## Run end-to-end tests: mock feed -> poller -> Kafka -> consumer -> PostgreSQL -> API
	uv run pytest -m e2e

test-disruptive: ## Run tests that stop and start the shared Kafka and PostgreSQL containers
	uv run pytest tests/disruptive --run-disruptive

test-all: ## Run every non-disruptive test in one process (needs `make up`)
	uv run pytest

regression: up ## Run every layer in order, stop at the first failure, write reports/
	uv run python scripts/regression.py

flake-check: up ## Run the end-to-end suite 5 times in a row; fail on any failure
	@for i in 1 2 3 4 5; do echo "== run $$i/5"; uv run pytest -m e2e -q -p no:randomly || exit 1; done

traceability: ## Rebuild docs/traceability-matrix.md from the last regression run
	uv run python scripts/traceability.py

smoke: test-integration ## Smoke-test the running infrastructure

smoke-restart: ## Restart Kafka and PostgreSQL and check data survives (disruptive)
	uv run pytest tests/disruptive --run-disruptive -k survives_restart

migrate: ## Apply database migrations
	uv run alembic upgrade head

coverage: ## Run unit tests with a coverage report
	uv run pytest -m "not integration" --cov=earth_hub --cov-report=term-missing
