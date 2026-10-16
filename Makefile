.PHONY: help install up down reset lint format test test-integration smoke smoke-restart migrate coverage

help: ## List the available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv from uv.lock
	uv sync --locked

up: ## Start Kafka and PostgreSQL, wait until healthy, create topics
	docker compose up -d --wait kafka postgres
	docker compose run --rm kafka-init

down: ## Stop the services (data volumes are kept)
	docker compose down

reset: ## Stop the services and delete their data volumes
	docker compose down --volumes

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
