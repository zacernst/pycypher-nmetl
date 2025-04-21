# pycypher-nmetl Makefile
# ------------------------------------------------------------------------------
# Configuration variables
PYTHON_VERSION = 3.12
SUPPORTED_PYTHON_VERSIONS = 3.13 3.12 3.11 3.10
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

# ------------------------------------------------------------------------------
# Main targets
.PHONY: all clean clean_build veryclean format build install tests coverage docs publish data test_env fod_ingest

# Default target - run the complete build process
all: format veryclean build docs alltests

# ------------------------------------------------------------------------------
# Cleaning targets

# Remove all generated files and virtual environment
veryclean: clean
	@echo "Deep cleaning project..."
	uv cache clean && rm -rfv ./.venv

# Clean up build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rfv ./venv
	rm -rfv ${DOCS_DIR}/build/html/*
	rm -rfv ${DOCS_DIR}/build/doctrees/*
	rm -fv ./requirements.txt
	rm -rfv ./dist/*
	rm -rfv ${COVERAGE_DIR}

# ------------------------------------------------------------------------------
# Development targets

# Format code
format:
	@echo "Formatting code..."
	uv run isort .
	uv run ruff format .

# Generate requirements.txt from requirements.in
requirements.txt: requirements.in
	@echo "Compiling requirements.txt..."
	uv pip compile --output-file=requirements.txt requirements.in

# Build packages
build:
	@echo "Building packages..."
	@for version in ${SUPPORTED_PYTHON_VERSIONS}; do \
		echo "Building with Python $$version..." && \
		uv run --python $$version hatch build -t wheel || exit 1; \
	done	

# Install packages in development mode
install: build
	@echo "Installing packages in development mode..."
	# uv pip install --upgrade -e ${PYCYPHER_DIR}
	# uv pip install --upgrade -e ${NMETL_DIR}
	uv pip install --upgrade -e .

# ------------------------------------------------------------------------------
# Testing targets

# Run tests
tests: install 
	@echo "Running tests..."
	uv run --python ${PYTHON_VERSION} pytest -vv . && \
	echo "🎉\n"


test: tests

alltests: 
	@echo "Running tests..."
	@for version in ${SUPPORTED_PYTHON_VERSIONS}; do \
		echo "Builds and tests with Python $$version..." && \
		uv run --python $$version hatch build -t wheel || exit 1; \
		uv run --python $$version pip install --upgrade -e . || exit 1; \
		uv run --python $$version pytest -vv ${TESTS_DIR} -n ${PYTHON_TEST_THREADS} || exit 1; \
		echo "🎉\n"; \
	done	

# Run tests with coverage
coverage: install
	@echo "Running tests with coverage..."
	uv run pytest \
		--cov=${PACKAGES_DIR}/pycypher/src/pycypher \
		--cov=${PACKAGES_DIR}/nmetl/src/nmetl \
		--cov-report=html:${COVERAGE_DIR}

# ------------------------------------------------------------------------------
# Documentation targets

# Build documentation
docs: install
	@echo "Building documentation..."
	cd ${DOCS_DIR} && \
	uv run make clean && \
	uv run make html && \
	uv run make singlehtml

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

# ------------------------------------------------------------------------------
# Package-specific targets
.PHONY: pycypher nmetl fastopendata tada

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
