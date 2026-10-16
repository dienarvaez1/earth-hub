.PHONY: help install up down reset ingest ingest-once ingest-logs ingest-stop lint format test test-integration smoke smoke-restart migrate coverage

help: ## List the available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv from uv.lock
	uv sync --locked

up: ## Start Kafka and PostgreSQL, wait until healthy, create topics
	docker compose up -d --wait kafka postgres
	docker compose run --rm kafka-init

down: ## Stop all services, including the poller (data volumes are kept)
	docker compose --profile ingest down

reset: ## Stop all services and delete their data volumes
	docker compose --profile ingest down --volumes

ingest: up ## Build and start the poller against the live USGS feed
	docker compose --profile ingest up -d --build --wait poller

ingest-once: ## Poll the live feed once from this machine and exit
	uv run python -m earth_hub.ingestion --once

ingest-logs: ## Follow the poller's logs
	docker compose --profile ingest logs -f poller

ingest-stop: ## Stop the poller, leaving Kafka and PostgreSQL running
	docker compose --profile ingest stop poller

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

smoke: test-integration ## Smoke-test the running infrastructure

smoke-restart: ## Restart Kafka and PostgreSQL and check data survives
	./scripts/smoke_restart.sh

migrate: ## Apply database migrations
	uv run alembic upgrade head

coverage: ## Run unit tests with a coverage report
	uv run pytest -m "not integration" --cov=earth_hub --cov-report=term-missing
