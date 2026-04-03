# PyCypher Makefile
# ------------------------------------------------------------------------------
# Configuration variables
PYTHON_VERSION = 3.14
SUPPORTED_PYTHON_VERSIONS = 3.14
PYTHON_TEST_THREADS = 8
BUMP = micro

# Cross-platform browser opener (macOS: open, Linux: xdg-open, WSL: wslview)
BROWSER := $(shell command -v xdg-open 2>/dev/null || command -v wslview 2>/dev/null || echo open)


# Project paths
export PROJECT_ROOT := .
export PACKAGES_DIR := ${PROJECT_ROOT}/packages
# Package-specific paths
export PYCYPHER_DIR := ${PACKAGES_DIR}/pycypher
export SHARED_DIR := ${PACKAGES_DIR}/shared
export FASTOPENDATA_DIR := ${PACKAGES_DIR}/fastopendata
export LC_ALL := C

# Documentation paths
export DOCS_DIR := ${PROJECT_ROOT}/docs

# Test and coverage paths
export TESTS_DIR := ${PROJECT_ROOT}/tests
export COVERAGE_DIR := ${PROJECT_ROOT}/coverage_report

# Main targets
.PHONY: help pycypher shared test docs lsp clean veryclean venv uv start format lint lint-changed audit typecheck coverage coverage-check check setup test-file test-find test-k test-mark watch reset lock-check dev-check bench bench-save bench-compare bench-memory metrics-snapshot metrics-prometheus test-telemetry dev-up dev-up-minimal dev-down dev-shell dev-rebuild dev-logs dev-test dev-typecheck dev-format spark-up spark-down spark-logs spark-ui spark-shell spark-scale neo4j-up neo4j-down neo4j-logs neo4j-browser neo4j-shell neo4j-reset infra-up infra-down test-spark test-neo4j test-integration fod-up fod-down fod-shell fod-logs fod-rebuild fod-api-up fod-api-down fod-api-shell fod-api-logs fod-api-rebuild nominatim-up nominatim-down nominatim-logs nominatim-search nominatim-status fod-data fod-data-plan fod-data-census fod-data-tiger fod-data-osm fod-data-wikidata fod-data-status fod-data-clean

# Default target - run the complete build process
all: clean venv format pycypher docs

## Show available targets with descriptions
help:
	@echo "PyCypher Development Targets"
	@echo "============================"
	@echo ""
	@echo "Setup & Build:"
	@echo "  make uv              Install/upgrade uv package manager"
	@echo "  make venv            Create virtual environment"
	@echo "  make build           Build wheel packages"
	@echo "  make pycypher        Build and install pycypher package"
	@echo "  make format          Run ruff import sorting + format"
	@echo "  make all             Full rebuild (veryclean + venv + format + pycypher)"
	@echo ""
	@echo "Testing:"
	@echo "  make test            Run all tests (parallel, $(PYTHON_TEST_THREADS) threads)"
	@echo "  make test-fast       Run tests (auto threads, stop on first failure)"
	@echo "  make test-serial     Run tests (single thread)"
	@echo "  make test-quick      Run tests (minimal output, no coverage)"
	@echo "  make test-failed     Re-run only previously failed tests"
	@echo "  make test-changed    Re-run failed tests first, then rest"
	@echo "  make test-verbose    Run tests with verbose output"
	@echo "  make test-unit       Run only unit-marked tests"
	@echo "  make test-no-slow    Run all tests except slow-marked"
	@echo "  make coverage        Run tests with HTML coverage report"
	@echo "  make coverage-check  Run tests with coverage floor (COVERAGE_FLOOR=50)"
	@echo ""
	@echo "Benchmarking:"
	@echo "  make bench           Run performance benchmarks (pytest-benchmark)"
	@echo "  make bench-save      Run benchmarks and save baseline"
	@echo "  make bench-compare   Run benchmarks and compare against baseline"
	@echo "  make bench-memory    Run memory profiling benchmark"
	@echo ""
	@echo "Telemetry & Monitoring:"
	@echo "  make metrics-snapshot     Show current metrics (human-readable)"
	@echo "  make metrics-prometheus   Export metrics in Prometheus text format"
	@echo "  make test-telemetry       Run telemetry/exporter tests"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint            Run ruff import check + format check + lint"
	@echo "  make lint-changed    Lint only files changed vs main (CI gate)"
	@echo "  make typecheck       Run ty type checker"
	@echo "  make audit           Scan dependencies for known vulnerabilities (pip-audit)"
	@echo "  make quality         Code quality dashboard (complexity, lint, types)"
	@echo "  make complexity      Show complexity hotspots only"
	@echo "  make quality-changed Quality check on changed files only"
	@echo ""
	@echo "Docker Development:"
	@echo "  make dev-up          Start dev container + Spark + Neo4j"
	@echo "  make dev-up-minimal  Start dev container only (no Spark/Neo4j)"
	@echo "  make dev-down        Stop all containers"
	@echo "  make dev-shell       Open shell in dev container"
	@echo "  make dev-rebuild     Rebuild and restart dev container"
	@echo "  make dev-logs        Tail dev container logs"
	@echo "  make dev-test        Run tests inside container"
	@echo "  make dev-typecheck   Run ty type checker inside container"
	@echo "  make dev-format      Run ruff format inside container"
	@echo "  make dev-jupyter     Start with Jupyter Lab (port 8888)"
	@echo "  make dev-vscode      Start with VS Code server (port 8080)"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra-up        Start Spark + Neo4j"
	@echo "  make infra-down      Stop Spark + Neo4j"
	@echo "  make spark-up        Start Spark cluster"
	@echo "  make spark-down      Stop Spark cluster"
	@echo "  make spark-ui        Open Spark UI (port 8090)"
	@echo "  make spark-shell     Open PySpark shell"
	@echo "  make spark-scale     Scale workers (WORKERS=N)"
	@echo "  make neo4j-up        Start Neo4j"
	@echo "  make neo4j-down      Stop Neo4j"
	@echo "  make neo4j-browser   Open Neo4j browser (port 7474)"
	@echo "  make neo4j-shell     Open Cypher shell"
	@echo "  make neo4j-reset     Delete all Neo4j data (with confirmation)"
	@echo ""
	@echo "Integration Tests:"
	@echo "  make test-spark      Run Spark tests (requires dev container)"
	@echo "  make test-neo4j      Run Neo4j tests (requires dev container)"
	@echo "  make test-integration Run all integration tests"
	@echo "  make test-large-dataset Run large-dataset tests (timeout=120s)"
	@echo "  make test-backends   Run backend equivalence tests (timeout=60s)"
	@echo ""
	@echo "Data Downloads (Snakemake):"
	@echo "  make fod-data          Download all 17 fastopendata datasets"
	@echo "  make fod-data-plan     Dry-run: show what would be downloaded"
	@echo "  make fod-data-census   Download Census surveys (ACS, SIPP, AHS, CJARS)"
	@echo "  make fod-data-tiger    Download TIGER/Line shapefiles"
	@echo "  make fod-data-osm      Download OpenStreetMap U.S. extract (~10 GB)"
	@echo "  make fod-data-wikidata Download Wikidata geopoints (~100 GB raw)"
	@echo "  make fod-data-status   Show pipeline status"
	@echo "  make fod-data-clean    Delete all downloaded raw data"
	@echo ""
	@echo "Documentation:"
	@echo "  make docs            Build Sphinx documentation"
	@echo ""
	@echo "Examples:"
	@echo "  uv run python examples/social_network/run_demo.py"
	@echo "  uv run python examples/functions_in_where.py"
	@echo "  uv run python examples/scalar_functions_in_with.py"
	@echo "  uv run python examples/ast_conversion_example.py"
	@echo "  uv run python examples/advanced_grammar_examples.py"
	@echo ""
	@echo "Developer Workflow:"
	@echo "  make setup           One-command onboarding (core deps, no Spark/Dask/Polars)"
	@echo "  make setup-full      Full onboarding (all deps including Spark/Dask/Polars/Neo4j)"
	@echo "  make check           Run lock-check + format + lint + typecheck + test-fast"
	@echo "  make lock-check      Verify uv.lock matches pyproject.toml (CI parity)"
	@echo "  make dev-check       Validate .env before Docker targets"
	@echo "  make test-file FILE=tests/test_foo.py  Run a single test file"
	@echo "  make test-find QUERY=binding           Search test names"
	@echo "  make test-k EXPR=\"binding AND frame\"   Run tests matching keyword expression"
	@echo "  make test-mark MARK=security           Run tests by marker"
	@echo "  make watch                             Re-run tests on file change (TDD)"
	@echo "  make watch WATCH_FILE=tests/test_foo.py  Watch a specific test file"
	@echo ""
	@echo "Getting Started:"
	@echo "  1. make setup            (core deps, or make setup-full for everything)"
	@echo "  2. cp .env.example .env  (set real credentials for Docker)"
	@echo "  3. uv sync               (install dependencies)"
	@echo "  4. make test             (run tests)"
	@echo ""
	@echo "Cleaning:"
	@echo "  make clean           Remove build artifacts"
	@echo "  make reset           Deep clean (clean + remove .venv + uv cache)"
	@echo "  make veryclean       Alias for reset"

uv:
	@command -v uv >/dev/null 2>&1 || { echo "Installing uv..."; curl -LsSf https://astral.sh/uv/install.sh | sh; }

venv: uv
	@echo "Setting up virtual environment..."
	uv venv .venv

start: veryclean install

# ------------------------------------------------------------------------------
# Cleaning targets

# Remove all generated files and virtual environment
reset: clean
	@echo "Deep cleaning project..."
	uv cache clean && rm -rfv ./.venv

# Keep veryclean as alias for backwards compatibility
veryclean: reset

# Clean up build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rfv ./venv
	rm -rfv ${DOCS_DIR}/build/html/*
	rm -rfv ${DOCS_DIR}/build/doctrees/*
	rm -fv ./requirements.txt
	rm -rfv ./dist/*
	rm -rfv ${COVERAGE_DIR}

test:
	uv run pytest -n ${PYTHON_TEST_THREADS} .

test-fast:
	uv run pytest -n auto -x -m "not slow" --ignore=tests/load_testing --ignore=tests/large_dataset .

test-serial:
	uv run pytest .

test-quick:
	uv run pytest -n auto --tb=line --no-cov -q -m "not slow" --ignore=tests/load_testing --ignore=tests/large_dataset --ignore=tests/property_based .

test-failed:
	uv run pytest --lf -n ${PYTHON_TEST_THREADS} .

test-changed:
	uv run pytest --lf --ff -n ${PYTHON_TEST_THREADS} .

test-verbose:
	uv run pytest -n ${PYTHON_TEST_THREADS} -v .

test-unit:
	uv run pytest -m unit -n ${PYTHON_TEST_THREADS} .

test-no-slow:
	uv run pytest -m "not slow" -n ${PYTHON_TEST_THREADS} .

test-large-dataset:
	@echo "Running large-dataset integration tests..."
	uv run pytest -m integration tests/test_distributed_scaffolding.py tests/test_large_dataset_dependency_compat.py -v --timeout=120

test-backends:
	@echo "Running backend equivalence and compatibility tests..."
	uv run pytest tests/test_large_dataset_dependency_compat.py tests/test_distributed_scaffolding.py -k "not Dask" -v --timeout=60

# ------------------------------------------------------------------------------
# Benchmarking targets (pytest-benchmark)

# Run all benchmarks (excludes slow/100K scale by default)
bench:
	@echo "Running performance benchmarks..."
	uv run pytest tests/benchmarks/bench_core_operations.py -v --benchmark-only -m "not slow" --timeout=120

# Run benchmarks and save as named baseline for regression comparison
BENCH_NAME ?= baseline
bench-save:
	@echo "Running benchmarks and saving as '$(BENCH_NAME)'..."
	uv run pytest tests/benchmarks/bench_core_operations.py -v --benchmark-only -m "not slow" --benchmark-save=$(BENCH_NAME) --timeout=120
	@echo "Saved to .benchmarks/ — compare later with: make bench-compare"

# Run benchmarks and compare against most recent saved baseline
bench-compare:
	@echo "Running benchmarks and comparing against saved baseline..."
	uv run pytest tests/benchmarks/bench_core_operations.py -v --benchmark-only -m "not slow" --benchmark-compare=0001_baseline --benchmark-compare-fail=mean:5.0 --timeout=120

# Run memory profiling benchmark (all scales including slow)
bench-memory:
	@echo "Running memory profiling benchmark..."
	uv run python tests/benchmarks/bench_memory_baseline.py

# ------------------------------------------------------------------------------
# Telemetry and monitoring targets

# Show current in-process metrics snapshot (human-readable)
metrics-snapshot:
	@uv run nmetl metrics

# Export current metrics in Prometheus text exposition format
metrics-prometheus:
	@uv run python -c "from shared.metrics import QUERY_METRICS; from shared.exporters import PrometheusExporter; print(PrometheusExporter().render(QUERY_METRICS.snapshot()))"

# Run telemetry and exporter test suites
test-telemetry:
	@echo "Running telemetry integration tests..."
	uv run pytest tests/test_otel_integration.py tests/test_metrics_exporters.py -v

# ------------------------------------------------------------------------------
# Code quality targets (local equivalents of CI checks)

lint:
	@echo "Running linters..."
	uv run ruff check --select I .
	uv run ruff format --check .
	uv run ruff check .

# Lint only files changed vs main (enforces quality on new code)
lint-changed:
	@./scripts/lint_changed.sh

# Scan dependencies for known vulnerabilities (CVEs)
# --skip-editable excludes workspace packages not on PyPI
audit:
	@echo "Auditing dependencies for known vulnerabilities..."
	uv run pip-audit --desc --skip-editable --cache-dir "$$(mktemp -d)"

# Static Application Security Testing (SAST) — scan source code for
# injection risks, hardcoded secrets, and other security anti-patterns.
# Configuration in pyproject.toml [tool.bandit].
sast:
	@echo "Running SAST scan (bandit)..."
	uv run bandit -r packages/pycypher/src/ packages/shared/src/ \
		-c pyproject.toml --severity-level medium -f txt

# Combined security scan: dependencies + code
security: audit sast

typecheck:
	@echo "Running type checker..."
	uv run ty check

# ------------------------------------------------------------------------------
# Developer workflow targets

# One-command onboarding: copy .env, install core deps, install pre-commit hooks.
# Uses dev-core group (no Spark/Dask/Polars). For full deps: make setup-full
setup:
	@echo "Setting up development environment (core)..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env from .env.example — edit it to set real credentials for Docker."; \
	else \
		echo ".env already exists, skipping copy."; \
	fi
	uv sync --group dev-core
	uv run pre-commit install
	@echo ""
	@echo "Setup complete! Next steps:"
	@echo "  make test        Run the test suite (use -m 'not spark and not neo4j' for core-only)"
	@echo "  make dev-up      Start Docker dev environment (edit .env first)"
	@echo ""
	@echo "Dependency groups available:"
	@echo "  dev-core  (installed) — testing, linting, docs"
	@echo "  dev                   — adds Spark, Dask, Polars, Neo4j  (make setup-full)"
	@echo "  dev-full              — adds Jupyter, profiling, visualization"

# Full onboarding: all dev dependencies including Spark, Dask, Polars, Neo4j
setup-full:
	@echo "Setting up full development environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env from .env.example — edit it to set real credentials for Docker."; \
	else \
		echo ".env already exists, skipping copy."; \
	fi
	uv sync --group dev
	uv run pre-commit install
	@echo ""
	@echo "Full setup complete! All dependency groups installed."
	@echo "  make test        Run all tests"
	@echo "  make dev-up      Start Docker dev environment (edit .env first)"

# Verify uv.lock is in sync with pyproject.toml (matches CI --frozen)
lock-check:
	@echo "Checking lockfile is in sync with pyproject.toml..."
	uv sync --frozen --dry-run 2>&1 || { echo "ERROR: uv.lock is out of date. Run 'uv sync' to update."; exit 1; }
	@echo "Lockfile is in sync."

# Validate .env before docker-compose (prevents cryptic startup errors)
dev-check:
	@echo "Checking Docker environment prerequisites..."
	@if [ ! -f .env ]; then \
		echo "ERROR: .env file not found. Run 'cp .env.example .env' and set real credentials."; \
		exit 1; \
	fi
	@missing=""; \
	for var in NEO4J_USER NEO4J_PASSWORD NEO4J_URI SPARK_MASTER_URL SPARK_RPC_SECRET; do \
		val=$$(grep "^$$var=" .env 2>/dev/null | cut -d= -f2-); \
		if [ -z "$$val" ] || echo "$$val" | grep -q '<.*>'; then \
			missing="$$missing $$var"; \
		fi; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "ERROR: The following .env variables are missing or still have placeholder values:"; \
		echo " $$missing"; \
		echo "Edit .env and set real values before running Docker targets."; \
		exit 1; \
	fi
	@echo "Environment OK — all required Docker variables are set."

# Run format + lint + typecheck + fast tests (local CI equivalent)
check: lock-check format lint typecheck test-fast

## Code quality dashboard — complexity hotspots, lint summary, type coverage
quality:
	uv run python scripts/code_quality.py

## Complexity analysis only
complexity:
	uv run python scripts/code_quality.py --complexity

## Quality check on changed files only
quality-changed:
	uv run python scripts/code_quality.py --changed

# Run a single test file: make test-file FILE=tests/test_foo.py
FILE ?= tests/
test-file:
	uv run pytest -n auto -x $(FILE)

# Search test names by keyword: make test-find QUERY=binding
QUERY ?= ""
test-find:
	@uv run pytest --collect-only -q 2>/dev/null | grep -i "$(QUERY)" || echo "No tests matching '$(QUERY)'"

# Run tests matching a keyword expression: make test-k EXPR="binding AND frame"
EXPR ?= ""
test-k:
	uv run pytest -n auto -x -k "$(EXPR)" .

# Run tests by marker: make test-mark MARK=security
MARK ?= ""
test-mark:
	uv run pytest -n ${PYTHON_TEST_THREADS} -m "$(MARK)" .

# Watch files and re-run tests on change (TDD workflow)
WATCH_FILE ?= tests/
watch:
	uv run ptw -- -x --tb=short $(WATCH_FILE)

# ------------------------------------------------------------------------------
# Docker development targets

# Start the development container (+ Spark + Neo4j)
dev-up: dev-check
	@echo "Starting pycypher development environment (dev + Spark + Neo4j)..."
	docker compose up -d
	@echo "  pycypher-dev : make dev-shell"
	@echo "  Spark UI     : http://localhost:8090"
	@echo "  Neo4j browser: http://localhost:7474  (neo4j / pycypher)"

# Start only the dev container (no Spark/Neo4j — faster for pure pycypher work)
dev-up-minimal:
	@echo "Starting pycypher dev container only (no Spark/Neo4j)..."
	docker compose up -d --no-deps pycypher-dev
	@echo "  pycypher-dev : make dev-shell"

# Stop all containers
dev-down:
	@echo "Stopping development container..."
	docker compose down

# Access shell in development container
dev-shell:
	@echo "Accessing pycypher development container shell..."
	docker compose exec pycypher-dev bash

# Rebuild and start development container
dev-rebuild:
	@echo "Rebuilding pycypher development container..."
	docker compose build pycypher-dev
	docker compose up -d pycypher-dev

# View logs from development container
dev-logs:
	@echo "Viewing pycypher development container logs..."
	docker compose logs -f pycypher-dev

# Start with Jupyter Lab for interactive development
dev-jupyter:
	@echo "Starting development environment with Jupyter Lab..."
	docker compose --profile jupyter up -d
	@echo "Jupyter Lab available at http://localhost:8888"

# Start with code-server (VS Code in browser)
dev-vscode:
	@echo "Starting development environment with VS Code server..."
	docker compose --profile code-server up -d
	@echo "VS Code available at http://localhost:8080"
	@echo "Password: ${CODE_SERVER_PASSWORD:-pycypher}"

# Run tests inside the container
dev-test:
	@echo "Running tests in development container..."
	docker compose exec pycypher-dev bash -c "cd /workspace && uv run pytest -n auto -x tests/"

# Run type checking inside the container
dev-typecheck:
	@echo "Running type checker in development container..."
	docker compose exec pycypher-dev bash -c "cd /workspace && uv run ty check packages/pycypher/"

# Format code inside the container
dev-format:
	@echo "Formatting code in development container..."
	docker compose exec pycypher-dev bash -c "cd /workspace && uv run ruff format packages/pycypher/"

# ------------------------------------------------------------------------------
# Nominatim geocoder targets
#
# IMPORTANT — first-start import time:
#   Importing the full US OSM extract takes several hours and ~32 GB of RAM.
#   The PBF must already exist at:
#     packages/fastopendata/raw_data/us-latest.osm.pbf
#   Run `make fod-shell` then download it per DATASETS.md #16 before starting
#   Nominatim for the first time.  Subsequent starts skip the import.

nominatim-up:
	@echo "Starting Nominatim (first start triggers OSM import — see DATASETS.md #16)..."
	docker compose up -d nominatim

nominatim-down:
	docker compose stop nominatim

nominatim-logs:
	docker compose logs -f nominatim

# Quick smoke-test: geocode "New York" against the running instance
nominatim-search:
	@echo "Searching for 'New York'..."
	curl -s "http://localhost:8092/search?q=New+York&format=json&limit=1" | python3 -m json.tool

# Check import/service status
nominatim-status:
	curl -s "http://localhost:8092/status.php" | python3 -m json.tool

# ------------------------------------------------------------------------------
# FastOpenData targets

fod-up:
	@echo "Starting fastopendata container..."
	docker compose up -d fastopendata
	@echo "  shell: make fod-shell"

fod-down:
	docker compose stop fastopendata

fod-rebuild:
	@echo "Rebuilding fastopendata image..."
	docker compose build fastopendata
	docker compose up -d fastopendata

fod-shell:
	@echo "Opening fastopendata shell..."
	docker compose exec fastopendata bash

fod-logs:
	docker compose logs -f fastopendata

# ------------------------------------------------------------------------------
# FastOpenData API targets
# The API container runs uvicorn with --reload against the bind-mounted source.
# Swagger UI is available at http://localhost:8093/docs once the container starts.

fod-api-up:
	@echo "Starting fastopendata API container..."
	docker compose up -d fastopendata-api
	@echo "  API:       http://localhost:8093"
	@echo "  Swagger:   http://localhost:8093/docs"
	@echo "  ReDoc:     http://localhost:8093/redoc"
	@echo "  shell:     make fod-api-shell"

fod-api-down:
	docker compose stop fastopendata-api

fod-api-rebuild:
	@echo "Rebuilding fastopendata API image..."
	docker compose build fastopendata-api
	docker compose up -d fastopendata-api

fod-api-shell:
	@echo "Opening fastopendata API container shell..."
	docker compose exec fastopendata-api bash

fod-api-logs:
	docker compose logs -f fastopendata-api

# ------------------------------------------------------------------------------
# Spark targets

spark-up:
	@echo "Starting Spark cluster..."
	docker compose up -d spark-master spark-worker

spark-down:
	docker compose stop spark-master spark-worker

spark-logs:
	docker compose logs -f spark-master spark-worker

spark-ui:
	$(BROWSER) http://localhost:8090

spark-shell:
	docker compose exec pycypher-dev bash -c \
	  "cd /workspace && PYSPARK_DRIVER_PYTHON=python3 \
	   uv run pyspark --master spark://spark-master:7077"

WORKERS ?= 2
spark-scale:
	docker compose up -d --scale spark-worker=$(WORKERS) spark-worker

# ------------------------------------------------------------------------------
# Neo4j targets

neo4j-up:
	@echo "Starting Neo4j..."
	docker compose up -d neo4j

neo4j-down:
	docker compose stop neo4j

neo4j-logs:
	docker compose logs -f neo4j

neo4j-browser:
	$(BROWSER) http://localhost:7474

neo4j-shell:
	@echo "Cypher shell (neo4j / pycypher)..."
	docker compose exec neo4j cypher-shell -u neo4j -p pycypher

neo4j-reset:
	@echo "WARNING: deleting all Neo4j data. Ctrl-C within 3s to abort."
	@sleep 3
	docker compose exec neo4j cypher-shell -u neo4j -p pycypher \
	  "MATCH (n) DETACH DELETE n"
	@echo "Neo4j graph cleared."

# ------------------------------------------------------------------------------
# Combined infrastructure targets

infra-up: spark-up neo4j-up
	@echo "Spark and Neo4j services running."

infra-down:
	docker compose stop spark-master spark-worker neo4j

# ------------------------------------------------------------------------------
# Integration test targets

test-spark:
	@echo "Running Spark integration tests..."
	docker compose exec pycypher-dev bash -c \
	  "cd /workspace && uv run pytest -m spark -v"

test-neo4j:
	@echo "Running Neo4j integration tests..."
	docker compose exec pycypher-dev bash -c \
	  "cd /workspace && uv run pytest -m neo4j -v"

test-integration:
	docker compose exec pycypher-dev bash -c \
	  "cd /workspace && uv run pytest -m 'spark or neo4j' -v"

# ------------------------------------------------------------------------------
# Development targets

# Format code
format:
	@echo "Formatting code..."
	uv run ruff check --select I --fix .
	uv run ruff format . --config ./pyproject.toml

# Build packages
build:
	@echo "Building packages..."
	@for v in ${SUPPORTED_PYTHON_VERSIONS}; do \
		echo "Building with Python version ${PYTHON_VERSION}..." && \
		uv run --python $v uv build -t wheel || exit 1; \
	done

# Install packages in development mode
install: build
	@echo "Installing packages in development mode..."
	uv pip install --upgrade -e ${PYCYPHER_DIR}
# ------------------------------------------------------------------------------
# Testing targets

# Run tests with coverage (parallel)
coverage:
	@echo "Running tests with coverage..."
	uv run pytest -n ${PYTHON_TEST_THREADS} --cov-report html:${COVERAGE_DIR} --cov

# Run tests with coverage (detailed, serial)
coverage-detailed:
	@echo "Running tests with detailed coverage..."
	uv run pytest --cov-report html:${COVERAGE_DIR} --cov --cov-report term-missing

# Run tests with coverage floor enforcement (CI gate)
COVERAGE_FLOOR ?= 80
coverage-check:
	@echo "Running tests with coverage floor ($(COVERAGE_FLOOR)%)..."
	uv run pytest -n ${PYTHON_TEST_THREADS} --cov --cov-fail-under=$(COVERAGE_FLOOR) -q

# ------------------------------------------------------------------------------
# Documentation targets

docs:
	@echo "Building documentation..."
	cd ${DOCS_DIR} && uv run make html
	@echo "Building PDF documentation..."
	cd ${DOCS_DIR} && uv run make latexpdf

lsp:
	@echo "Starting PyCypher LSP server (stdin/stdout)..."
	uv run python -m pycypher.cypher_lsp

# ------------------------------------------------------------------------------
# Release and publishing targets

# Publish package with version bump
publish: build
	@echo "Publishing package with version bump: $(BUMP)..."
	uv run python ./release.py --increment=$(BUMP)

# ------------------------------------------------------------------------------
# Snakemake dataset download targets
#
# These targets delegate to the Snakefile in packages/fastopendata/ which
# manages all 17 source datasets with retry logic, validation, and
# proper dependency ordering.
#
# Override data directory: make fod-data DATA_DIR=/mnt/data/fastopendata

SNAKEMAKE_CORES ?= all
FOD_SNAKEMAKE = cd ${FASTOPENDATA_DIR} && uv run snakemake --cores $(SNAKEMAKE_CORES)

## Download and process all 17 fastopendata datasets via Snakemake
fod-data:
	@echo "Downloading all fastopendata datasets ($(SNAKEMAKE_CORES) cores)..."
	$(FOD_SNAKEMAKE)

## Dry-run: show what Snakemake would download without executing
fod-data-plan:
	@echo "Snakemake dry-run (no downloads)..."
	$(FOD_SNAKEMAKE) --dry-run

## Download only Census survey datasets (ACS PUMS, SIPP, AHS, CJARS)
fod-data-census:
	@echo "Downloading Census survey datasets..."
	$(FOD_SNAKEMAKE) \
		raw_data/psam_pus.csv raw_data/psam_p.csv \
		raw_data/psam_hus.csv raw_data/psam_h.csv \
		raw_data/pu2023.csv raw_data/rw2023.csv raw_data/pu2023_schema.json \
		raw_data/.ahs_2023_extracted raw_data/cjars_joe_2022_co.csv \
		raw_data/state_county_tract_puma.csv

## Download only TIGER/Line geographic shapefiles
fod-data-tiger:
	@echo "Downloading TIGER/Line shapefiles..."
	$(FOD_SNAKEMAKE) \
		raw_data/puma_combined.shp raw_data/tl_2024_us_state.shp \
		raw_data/combined.shp

## Download and process OpenStreetMap U.S. extract (~10 GB)
fod-data-osm:
	@echo "Downloading OpenStreetMap U.S. extract..."
	$(FOD_SNAKEMAKE) raw_data/united_states_nodes.csv

## Download and filter Wikidata geopoint entities (~100 GB raw)
fod-data-wikidata:
	@echo "Downloading and filtering Wikidata dump..."
	$(FOD_SNAKEMAKE) raw_data/wikidata_us_points.json

## Show Snakemake DAG status for fastopendata pipeline
fod-data-status:
	@echo "Snakemake pipeline status:"
	$(FOD_SNAKEMAKE) --summary

## Clean downloaded raw data (requires confirmation)
fod-data-clean:
	@echo "This will delete all downloaded data in ${FASTOPENDATA_DIR}/raw_data/"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = y ] || exit 1
	rm -rf ${FASTOPENDATA_DIR}/raw_data/*
	@echo "Raw data cleaned."

# ------------------------------------------------------------------------------
# Data processing targets (legacy — prefer fod-data-* targets above)

# Process data with DVC
# data: install
# 	@echo "Running DVC pipeline..."
# 	uv run dvc repro

# Test environment variables
test_env:
	@echo "Testing environment variables..."
	@echo "DATA_DIR: ${DATA_DIR}"
	@echo "PROJECT_ROOT: ${PROJECT_ROOT}"
	./test_script.sh

# Run FastOpenData ingest
fod_ingest: data
	@echo "Running FastOpenData ingest..."
	uv run python ${FASTOPENDATA_DIR}/src/fastopendata/ingest.py

# Run FastOpenData ingest
ingest: data
	@echo "Running FastOpenData ingest..."
	uv run python ${FASTOPENDATA_DIR}/src/fastopendata/ingest.py

# ------------------------------------------------------------------------------
# Package-specific targets

# Build and install only pycypher
pycypher:
	@echo "Building and installing pycypher package..."
	cd ${PYCYPHER_DIR} && uv build
	uv pip install --upgrade -e ${PYCYPHER_DIR}

# Build and install only fastopendata (depends on pycypher)
fastopendata: pycypher
	@echo "Building and installing fastopendata package..."
	cd ${FASTOPENDATA_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade ${FASTOPENDATA_DIR}

#####################################
# vars:
#   - paths:
#       monorepo_path: /Users/zernst/git/pycypher-nmetl/
#       raw_data: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/
#       source_dir: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/
#
${DATA_DIR}/state_county_tract_puma.csv:
	wget --no-check-certificate https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt -O ${DATA_DIR}/state_county_unedited.txt
	cat ${DATA_DIR}/state_county_unedited.txt | sed 's/^\uFEFF//' > ${DATA_DIR}/state_county_tract_puma.csv
	dos2unix ${DATA_DIR}/state_county_tract_puma.csv

${DATA_DIR}/csv_pus_1_year.zip:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_1_year.zip

${DATA_DIR}/csv_pus_5_year.zip:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_5_year.zip

${DATA_DIR}/csv_hus_5_year.zip:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_5_year.zip

${DATA_DIR}/csv_hus_1_year.zip:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_1_year.zip

${DATA_DIR}/pus_1/psam_pusb.csv: ${DATA_DIR}/csv_pus_1_year.zip
	mkdir ${DATA_DIR}/pus_1 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_pus_1_year.zip -d ${DATA_DIR}/pus_1

${DATA_DIR}/hus_1/psam_husb.csv: ${DATA_DIR}/csv_hus_1_year.zip
	mkdir ${DATA_DIR}/hus_1 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_hus_1_year.zip -d ${DATA_DIR}/hus_1

${DATA_DIR}/pus_5/psam_pusd.csv: ${DATA_DIR}/csv_pus_5_year.zip
	mkdir ${DATA_DIR}/pus_5 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_pus_5_year.zip -d ${DATA_DIR}/pus_5

${DATA_DIR}/hus_5/psam_husd.csv: ${DATA_DIR}/csv_hus_5_year.zip
	mkdir ${DATA_DIR}/hus_5 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_hus_5_year.zip -d ${DATA_DIR}/hus_5

${DATA_DIR}/pus_1/psam_pus.csv: ${DATA_DIR}/pus_1/psam_pusb.csv
	cat ${DATA_DIR}/pus_1/psam_pusa.csv > ${DATA_DIR}/pus_1/psam_pus.csv
	cat ${DATA_DIR}/pus_1/psam_pusb.csv | tail -n +2 >> ${DATA_DIR}/pus_1/psam_pus.csv

${DATA_DIR}/hus_1/psam_hus.csv: ${DATA_DIR}/hus_1/psam_husb.csv
	cat ${DATA_DIR}/hus_1/psam_husa.csv > ${DATA_DIR}/hus_1/psam_hus.csv
	cat ${DATA_DIR}/hus_1/psam_husb.csv | tail -n +2 >> ${DATA_DIR}/hus_1/psam_hus.csv

${DATA_DIR}/pus_5/psam_pus.csv: ${DATA_DIR}/pus_5/psam_pusd.csv
	cat ${DATA_DIR}/pus_5/psam_pusa.csv > ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusb.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusc.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusd.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv

data: ${DATA_DIR}/hus_5/psam_hus.csv ${DATA_DIR}/pus_1/psam_pus.csv ${DATA_DIR}/hus_1/psam_hus.csv ${DATA_DIR}/pus_5/psam_pus.csv ${DATA_DIR}/wikidata_compressed.json.bz2 ${DATA_DIR}/united_states_nodes.csv ${DATA_DIR}/tl_2024_us_state.shp ${DATA_DIR}/ahs_2023_csv.zip ${DATA_DIR}/rw2023_csv.zip ${DATA_DIR}/pu2023_schema.json ${DATA_DIR}/united_states_nodes.csv ${DATA_DIR}/cjars_joe_2022_co.csv ${TRACT_FILES} ${ADDR_FILES} ${DATA_DIR}/state_county_tract_puma.csv
	echo "done"

${DATA_DIR}/hus_5/psam_hus.csv: ${DATA_DIR}/hus_5/psam_husd.csv
	cat ${DATA_DIR}/hus_5/psam_husa.csv > ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husb.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husc.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husd.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv

${DATA_DIR}/psam_2023_individual.csv: ${DATA_DIR}/psam_pus.csv
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_individual.csv
	grep 2023GQ ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_individual.csv

${DATA_DIR}/psam_2023_housing.csv: ${DATA_DIR}/psam_pus.csv
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_housing.csv
	grep 2023HU ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_housing.csv
#
#census_pus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${paths.raw_data}/csv_pus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_pus_5_year.zip
#   census_hus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${paths.raw_data}/csv_hus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_hus_5_year.zip

${DATA_DIR}/us-latest.osm.pbf:
	wget https://download.geofabrik.de/north-america/us-latest.osm.pbf -O ${DATA_DIR}/us-latest.osm.pbf

${DATA_DIR}/united_states_nodes.csv: ${DATA_DIR}/us-latest.osm.pbf
	DATA_DIR=${DATA_DIR} uv run python ${SOURCE_DIR}/extract_osm_nodes.py

census_block_shape_files:
	${SOURCE_DIR}/block_shape_files.sh

${DATA_DIR}/wikidata_compressed.json.bz2:
	wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 -O - | bunzip2 -c | uv run python ${SOURCE_DIR}/compress_wikidata.py | bzip2 -c > ${DATA_DIR}/wikidata_compressed.json.bz2

# ${DATA_DIR}/wikidata_compressed.json.bz2: ${DATA_DIR}/latest-all.json.bz2
# 	cat ${DATA_DIR}/latest-all.json.bz2 | bunzip2 -c | uv run python ${SOURCE_DIR}/compress_wikidata.py | bzip2 -c > ${DATA_DIR}/wikidata_compressed.json.bz2
#
#   download_pums_5_year:
#     cmd: wget -P '${paths.raw_data}' -nH --recursive -np https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/ && /bin/bash -c ${paths.source_dir}/pums_5_year.sh
#     outs:
#     - ${paths.raw_data}/psam_h.csv
#     - ${paths.raw_data}/psam_p.csv
#   compress_pums_5_year_h:
#     cmd: lbzip2 -k ${paths.raw_data}/psam_h.csv
#     deps:
#     - ${paths.raw_data}/psam_h.csv
#     outs:
#     - ${paths.raw_data}/psam_h.csv.bz2
#   compress_pums_5_year_p:
#     cmd: lbzip2 -k ${paths.raw_data}/psam_p.csv
#     deps:
#     - ${paths.raw_data}/psam_p.csv
#     outs:
#     - ${paths.raw_data}/psam_p.csv.bz2

${DATA_DIR}/combined.shp:
	wget --no-check-certificate -e robots=off -w 3 --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' -P ${DATA_DIR} -nH --recursive -np https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/
	${SOURCE_DIR}/puma_shape_files.sh
	uv run python ${SOURCE_DIR}/concatenate_puma_shape_files.py

${DATA_DIR}/us_state_boundaries.zip:
	wget --no-check-certificate -e robots=off --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip -O ${DATA_DIR}/us_state_boundaries.zip

${DATA_DIR}/tl_2024_us_state.shp: ${DATA_DIR}/us_state_boundaries.zip
	unzip ${DATA_DIR}/us_state_boundaries.zip -d ${DATA_DIR}/

# This one might be broken
${DATA_DIR}/pu2023_csv.zip:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip -O ${paths.raw_data}/pu2023_csv.zip
# && unzip -o ${paths.raw_data}/pu2023_csv.zip -d ${paths.raw_data}/

#     outs:
#     - ${paths.raw_data}/tl_2024_us_state.shp
#   filter_us_wikidata:
#     cmd: python ${paths.source_dir}/filter_us_nodes.py
#     deps:
#     - ${paths.raw_data}/location_entities.json.bz2
#     - ${paths.raw_data}/tl_2024_us_state.shp
#     outs:
#     - ${paths.raw_data}/wikidata_us_points.json
#   download_sipp_pu_data:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip -O ${paths.raw_data}/pu2023_csv.zip && unzip -o ${paths.raw_data}/pu2023_csv.zip -d ${paths.raw_data}/"
#     outs:
#     - ${paths.raw_data}/pu2023.csv

${DATA_DIR}/pu2023_schema.json:
	wget --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_schema.json -O ${DATA_DIR}/pu2023_schema.json

# This one should be broken into two steps
${DATA_DIR}/rw2023_csv.zip:
	wget --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/rw2023_csv.zip -O ${DATA_DIR}/rw2023_csv.zip
	unzip -o ${DATA_DIR}/rw2023_csv.zip -d ${DATA_DIR}/

# So should this
${DATA_DIR}/ahs_2023_csv.zip:
	wget --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20Value%20Labels%20Package.zip -O ${DATA_DIR}/ahs_2023.zip
	unzip -o ${DATA_DIR}/ahs_2023.zip -d ${DATA_DIR}/ahs_2023
	wget --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20National%20PUF%20v1.1%20Flat%20CSV.zip -O ${DATA_DIR}/ahs_2023_csv.zip
	unzip -o ${DATA_DIR}/ahs_2023_csv.zip -d ${DATA_DIR}/ahs_2023

${DATA_DIR}/cjars_joe_2022_co.csv.zip:
	wget --no-check-certificate https://www2.census.gov/programs-surveys/cjars/datasets/2022/cjars_joe_2022_co.csv.zip -O ${DATA_DIR}/cjars_joe_2022_co.csv.zip

${DATA_DIR}/cjars_joe_2022_co.csv: ${DATA_DIR}/cjars_joe_2022_co.csv.zip
	unzip -o ${DATA_DIR}/cjars_joe_2022_co.csv.zip -d ${DATA_DIR}
	mv ${DATA_DIR}/output/cjars_joe_2022_co.csv ${DATA_DIR}/cjars_joe_2022_co.csv

# https://www2.census.gov/geo/tiger/TIGER2025/TRACT/


${TRACT_FILES}:
	wget https://www2.census.gov/geo/tiger/TIGER2025/TRACT/$@ -O ${DATA_DIR}/$@tmp.zip
	unzip -o ${DATA_DIR}/$@tmp.zip -d ${DATA_DIR}
	rm ${DATA_DIR}/$@tmp.zip


${ADDR_FILES}:
	wget https://www2.census.gov/geo/tiger/TIGER2025/ADDR/$@ -O ${DATA_DIR}/$@tmp.zip
	unzip -o ${DATA_DIR}/$@tmp.zip -d ${DATA_DIR}
	rm ${DATA_DIR}/$@tmp.zip

#####################################################

