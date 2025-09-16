# pycypher-nmetl Makefile
# ------------------------------------------------------------------------------
# Configuration variables
PYTHON_VERSION = 3.12
SUPPORTED_PYTHON_VERSIONS = 3.12
PYTHON_TEST_THREADS = 4
BUMP = micro

# ------------------------------------------------------------------------------
# Project paths
export GIT_HOME := ${HOME}/git
export PROJECT_ROOT := ${GIT_HOME}/pycypher-nmetl
export PACKAGES_DIR := ${PROJECT_ROOT}/packages

# Package-specific paths
export PYCYPHER_DIR := ${PACKAGES_DIR}/pycypher
export NMETL_DIR := ${PACKAGES_DIR}/nmetl
export FASTOPENDATA_DIR := ${PACKAGES_DIR}/fastopendata
export DATA_DIR := ${FASTOPENDATA_DIR}/raw_data
export SOURCE_DIR := ${FASTOPENDATA_DIR}/src/fastopendata

# Documentation paths
export DOCS_DIR := ${PROJECT_ROOT}/docs

# Test and coverage paths
export TESTS_DIR := ${PROJECT_ROOT}/tests
export COVERAGE_DIR := ${PROJECT_ROOT}/coverage_report

export FOUNDATIONDB_VERSION := "7.1.31"

# ------------------------------------------------------------------------------
# Main targets
.PHONY: pycypher ingest nmetl fastopendata test tada docs

# Default target - run the complete build process
all: format veryclean build docs test

# ------------------------------------------------------------------------------
# Cleaning targets

# Remove all generated files and virtual environment
veryclean: clean
	@echo "Deep cleaning project..."
	uv cache clean && rm -rfv ./.venv

ingest: 
	@echo "Ingesting data..."
	(uv run python ${SOURCE_DIR}/ingest.py)

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
	(uv run pytest ./tests/test_eval.py)

# ------------------------------------------------------------------------------
# Development targets

# Format code
format:
	@echo "Formatting code..."
	uv run isort .
	uv run ruff format . --config ./pyproject.toml

# Build packages
build:
	@echo "Building packages..."
	@for version in ${SUPPORTED_PYTHON_VERSIONS}; do \
		echo "Building with Python version ${PYTHON_VERSION}..." && \
		uv run --python $version hatch build -t wheel || exit 1; \
	done	

# Install packages in development mode
install: build
	@echo "Installing packages in development mode..."
	# uv pip install --upgrade -e ${PYCYPHER_DIR}
	# uv pip install --upgrade -e ${NMETL_DIR}
	uv pip install --upgrade -e .

# ------------------------------------------------------------------------------
# Testing targets

# Run tests with coverage
coverage: install
	@echo "Running tests with coverage..."
	uv run pytest --cov-report html:${COVERAGE_DIR} --cov

# ------------------------------------------------------------------------------
# Documentation targets

docs:
	@echo "Building documentation..."
	cd ${DOCS_DIR} && uv run make html

# ------------------------------------------------------------------------------
# Release and publishing targets

# Publish package with version bump
publish: build
	@echo "Publishing package with version bump: $(BUMP)..."
	uv run python ./release.py --increment=$(BUMP)

# ------------------------------------------------------------------------------
# Data processing targets

# Process data with DVC
data: install
	@echo "Running DVC pipeline..."
	uv run dvc repro

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
ingest:
	@echo "Running FastOpenData ingest..."
	uv run python ${FASTOPENDATA_DIR}/src/fastopendata/ingest.py

# ------------------------------------------------------------------------------
# Package-specific targets

# Build and install only pycypher
pycypher:
	@echo "Building and installing pycypher package..."
	cd ${PYCYPHER_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade -e ${PYCYPHER_DIR}

# Build and install only nmetl (depends on pycypher)
nmetl: pycypher
	@echo "Building and installing nmetl package..."
	cd ${NMETL_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade -e ${NMETL_DIR}

# Build and install only fastopendata
fastopendata: nmetl
	@echo "Building and installing fastopendata package..."
	cd ${FASTOPENDATA_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade -e ${FASTOPENDATA_DIR}

fdbclear:
	@echo "Clearing FoundationDB data..."
	fdbcli --exec "writemode on; clearrange \"\" \"\\xFF\""