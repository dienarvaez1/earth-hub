.PHONY: help install up down reset ingest ingest-once ingest-logs ingest-stop api api-dev api-stop kafka-ui kafka-ui-stop backfill catchup-once catchup-now reconcile-usgs consume reconcile reconcile-direct repair-rows mode mode-direct mode-full to-full to-direct cutover-a cutover-b host-status host-release host-claim host-rebuild host-takeover backfill-full direct-once direct-run direct-runs export-r2 export-r2-runs r2-ls r2-count r2-get dashboard-install dashboard-build dashboard-dev dashboard-deploy trigger-token trigger-check test-dashboard check-db-outage lint format test test-integration test-e2e test-disruptive test-performance load load-ui test-all regression flake-check traceability release smoke smoke-restart migrate coverage

help: ## List the available commands
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv from uv.lock
	uv sync --locked

up: ## Start Kafka and PostgreSQL, wait until healthy, create topics
	docker compose up -d --wait kafka postgres
	docker compose run --rm kafka-init

down: ## Stop all services, including the poller (data volumes are kept)
	docker compose --profile ingest --profile api --profile tools down

reset: ## Stop all services and delete their data volumes
	docker compose --profile ingest --profile api --profile tools down --volumes

ingest: up ## Build and start the pipeline (migrate, poller, consumers, catch-up, the exporting service) on the live feed
	docker compose --profile ingest build migrate
	docker compose --profile ingest up -d --wait poller consumer consumer-backfill catchup
	docker compose --profile ingest --profile api up -d exporter

ingest-once: ## Poll the live feed once from this machine and exit
	uv run python -m earth_hub.ingestion --once

ingest-logs: ## Follow the pipeline services' logs
	docker compose --profile ingest logs -f poller consumer consumer-backfill catchup exporter

ingest-stop: ## Stop the pipeline services, leaving Kafka and PostgreSQL running
	docker compose --profile ingest stop poller consumer consumer-backfill catchup exporter

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

backfill: up ## Backfill from USGS FDSN, one week per request: make backfill START=2026-01-01 [END=...]
	@test -n "$(START)" || (echo "usage: make backfill START=2026-01-01 [END=2026-06-01]"; exit 1)
	uv run python -m earth_hub.ingestion.backfill --start $(START) $(if $(END),--end $(END))

catchup-once: up ## One catch-up pass (all_month feed) and one deleted-events check
	uv run python -m earth_hub.ingestion.catchup --once

consume: ## Run the consumer from this machine (Ctrl+C to stop)
	uv run python -m earth_hub.consumers

export-r2: ## Upload new and changed earthquakes rows to R2 through the API: make export-r2 [DRY_RUN=1] [VERIFY=1]
	uv run python -m earth_hub.export.r2 $(if $(DRY_RUN),--dry-run) $(if $(VERIFY),--verify)

export-r2-runs: ## List the latest R2 export runs and their counts
	uv run python -m earth_hub.export.r2 runs

r2-ls: ## List R2 keys (read-only): make r2-ls [PREFIX=runs/] [LIMIT=50]
	uv run python -m earth_hub.export.browse ls $(PREFIX) --limit $(or $(LIMIT),50)

r2-count: ## Count R2 objects and bytes (read-only): make r2-count [PREFIX=earthquakes/]
	uv run python -m earth_hub.export.browse count $(PREFIX)

r2-get: ## Print one R2 object (read-only): make r2-get KEY=analytics/meta.json [OUT=file]
	@test -n "$(KEY)" || (echo "usage: make r2-get KEY=analytics/meta.json [OUT=file]"; exit 1)
	uv run python -m earth_hub.export.browse get $(KEY) $(if $(OUT),--out $(OUT))

dashboard-install: ## Install the dashboard's Node dependencies (dashboard/package-lock.json) and the test browser
	cd dashboard && npm ci
	uv run playwright install chromium

dashboard-build: ## Build the dashboard page into dashboard/dist
	uv run python scripts/fetch_duckdb_extensions.py
	cd dashboard && npm run -s build

dashboard-dev: dashboard-build ## Serve the dashboard at http://127.0.0.1:8787, reading the real R2 bucket
	uv run python scripts/dashboard_config.py
	cd dashboard && npx --no-install wrangler dev --ip 127.0.0.1 --port 8787

dashboard-deploy: dashboard-build ## Publish the dashboard to Cloudflare Workers (*.workers.dev). Never part of the tests
	uv run python scripts/dashboard_config.py
	cd dashboard && npx --no-install wrangler deploy

reconcile: ## Check every Kafka message has a database row, and the tables' invariants
	uv run python -m earth_hub.reconcile

reconcile-direct: ## Check direct mode's state in R2 (read-only): make reconcile-direct [DEEP=1]
	uv run python -m earth_hub.direct.reconcile $(if $(DEEP),--deep)

repair-rows: ## Write row objects that differ from the snapshot again, in either mode: make repair-rows [DRY_RUN=1]
	uv run python -m earth_hub.direct.repair $(if $(DRY_RUN),--dry-run)

mode: ## Which mode writes the R2 bucket: full or direct
	uv run python -m earth_hub.direct.handover status

mode-direct: ## Hand over to direct mode: stop the poller and catch-up, export, write direct mode's state, catch up
	docker compose stop exporter poller catchup
	uv run python -m earth_hub.direct.handover direct || { docker compose start poller catchup exporter; echo "Handover failed: full mode resumed"; exit 1; }
	@echo "Direct mode: catching up with USGS, then checking against it"
	uv run python -m earth_hub.direct once --all
	uv run python -m earth_hub.usgs_reconcile || { echo "Direct mode is on; the check against USGS found differences it cannot explain (above)"; exit 1; }
	@echo "Direct mode: the 4-hourly schedule writes the bucket now (or make direct-run, not both)"

mode-full: ## Hand over to full mode: replay direct mode's log into Kafka (REBUILD=1: load an empty PostgreSQL from the bucket), check, then switch
	docker compose --profile ingest build migrate
	docker compose --profile ingest --profile api up -d --wait consumer consumer-backfill api
	uv run python -m earth_hub.direct.handover full $(if $(REBUILD),--rebuild)
	docker compose --profile ingest --profile api up -d poller catchup exporter
	@echo "Full mode: catching up with USGS, then checking against it"
	uv run python -m earth_hub.ingestion.catchup --once
	uv run python -m earth_hub.usgs_reconcile --wait 300 || { echo "Full mode is on; the check against USGS found differences it cannot explain (above)"; exit 1; }
	@echo "Full mode: the poller and catch-up are running again"

to-full: ## Direct mode to full mode in one command: checks, make mode-full, consumers caught up, make reconcile (DRY_RUN=1 lists the steps)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) to-full

to-direct: ## Full mode to direct mode in one command: consumers caught up, make reconcile, make mode-direct, make down (DRY_RUN=1 lists the steps)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) to-direct

cutover-a: ## On the machine giving up full mode: pre-checks, a dry run, then (confirmed) release the bucket if it owns it and stop its containers (DRY_RUN=1: checks and dry run only)
	@uv run python scripts/cutover.py a $(if $(DRY_RUN),--dry-run)

cutover-b: ## On the machine taking full mode: pre-checks (tools, .env, R2, USGS), the claim's dry run, then (confirmed) make host-claim and checks after (DRY_RUN=1: checks and dry run only)
	@uv run python scripts/cutover.py b $(if $(DRY_RUN),--dry-run)

host-status: ## Who owns the bucket and its lease, this host, whether its database holds anything the bucket lacks (read-only)
	@uv run python scripts/modes.py host-status

host-release: ## On the bucket's owner: hand it to direct mode (make to-direct) so another host can claim it (DRY_RUN=1 lists the steps)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) host-release

host-claim: ## Make this host the bucket's owner in full mode, rebuilding its database from the bucket if it differs (DRY_RUN=1 lists the steps)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) host-claim

host-rebuild: ## Back up, wipe and reload this host's database from the bucket, then claim it; asks for the machine ID (DRY_RUN=1 lists the steps)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) host-rebuild

host-takeover: ## Direct mode takes the bucket over from its host now, without waiting for the lease (for a host known gone)
	uv run python scripts/modes.py host-takeover

backfill-full: ## Backfill in full mode in one command: make ingest, make backfill, consumers caught up, make reconcile, make export-r2: START=2026-01-01 [END=...] [DRY_RUN=1]
	@test -n "$(START)" || (echo "usage: make backfill-full START=2026-01-01 [END=2026-06-01]"; exit 1)
	uv run python scripts/modes.py $(if $(DRY_RUN),--dry-run) backfill $(START) $(END)

catchup-now: ## Catch up with USGS now, in the mode that writes the bucket: day feed, month feed, deletions
	@if uv run python -m earth_hub.direct.handover status | grep -q '^direct (active)'; then \
		uv run python -m earth_hub.direct once --all; \
	else \
		uv run python -m earth_hub.ingestion.catchup --once; \
	fi

reconcile-usgs: ## Compare Earth-Hub with USGS for the last days, explaining every difference: [DAYS=3]
	uv run python -m earth_hub.usgs_reconcile $(if $(DAYS),--days $(DAYS))

direct-once: ## Direct mode: one poller round, and catch-up if due, straight to R2
	uv run python -m earth_hub.direct once

direct-run: ## Direct mode: the poller and catch-up straight to R2, until stopped
	uv run python -m earth_hub.direct run

direct-runs: ## Direct mode's latest executions from its audit trail (read-only): make direct-runs [LIMIT=20]
	uv run python -m earth_hub.direct.audit $(if $(LIMIT),--limit $(LIMIT))

check-db-outage: ## Stop PostgreSQL mid-stream and check nothing is lost (disruptive)
	uv run pytest tests/disruptive --run-disruptive -k database_outage

lint: ## Check style and formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Fix style and formatting
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run unit tests (no Docker or network needed)
	uv run pytest -m "not integration and not e2e and not disruptive and not performance and not dashboard"

test-dashboard: ## Run the dashboard in a browser against a simulated R2 (needs make dashboard-install)
	uv run pytest tests/dashboard -m dashboard

test-integration: ## Run integration tests (needs `make up`)
	uv run pytest -m integration

test-e2e: ## Run end-to-end tests: mock feed -> poller -> Kafka -> consumer -> PostgreSQL -> API
	uv run pytest -m e2e

test-disruptive: ## Run tests that stop and start the shared Kafka and PostgreSQL containers
	uv run pytest tests/disruptive --run-disruptive

test-performance: ## Backfill a full year of synthetic events through the pipeline, timed
	uv run pytest tests/performance --run-performance -s

load: ## Load-test the API with Locust over 140,000 synthetic events: reports/load-report.html
	uv run python scripts/load_test.py $(if $(USERS),--users $(USERS)) $(if $(RUN_TIME),--run-time $(RUN_TIME))

load-ui: ## The same load test in Locust's web UI, at http://127.0.0.1:8089
	uv run python scripts/load_test.py --ui

test-all: ## Run every non-disruptive test in one process (needs `make up`)
	uv run pytest

regression: up ## Run every layer in order, stop at the first failure, write reports/
	uv run python scripts/regression.py

flake-check: up ## Run the end-to-end suite 5 times in a row; fail on any failure
	@for i in 1 2 3 4 5; do echo "== run $$i/5"; uv run pytest -m e2e -q -p no:randomly || exit 1; done

traceability: ## Rebuild docs/progress/traceability-matrix.md from the last regression run
	uv run python scripts/traceability.py

trigger-token: ## The dashboard trigger's GitHub token: opens GitHub's form, checks the token, stores it on the Worker (run in a terminal)
	uv run python scripts/trigger_token.py

trigger-check: dashboard-build ## Whether the Worker's deployed GitHub token works, asked on Cloudflare (starts nothing)
	uv run python scripts/dashboard_config.py
	uv run python scripts/trigger_check.py

release: ## The public release, step by step, asking before each change (DRY_RUN=1 lists the steps)
	uv run python scripts/release.py $(if $(DRY_RUN),--dry-run)

smoke: test-integration ## Smoke-test the running infrastructure

smoke-restart: ## Restart Kafka and PostgreSQL and check data survives (disruptive)
	uv run pytest tests/disruptive --run-disruptive -k survives_restart

migrate: ## Apply database migrations
	uv run alembic upgrade head

coverage: ## Run unit tests with a coverage report (no Docker or network needed)
	uv run pytest -m "not integration and not e2e and not disruptive and not performance and not dashboard" --cov=earth_hub --cov-report=term-missing
