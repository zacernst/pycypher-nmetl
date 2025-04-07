PYTHON = 3.12
BUMP = micro

export GIT_HOME := ${HOME}/git
export PROJECT_ROOT := ${GIT_HOME}/pycypher-nmetl
export DATA_DIR := ${PROJECT_ROOT}/packages/fastopendata/raw_data
export SOURCE_DIR := ${PROJECT_ROOT}/packages/fastopendata/src/fastopendata

clean_build: veryclean all

all: format build docs tests

veryclean: clean
	uv cache clean && rm -rfv ./.venv

format:
	( \
		uv run isort . && \
		uv run ruff format . \
	)

requirements.txt: requirements.in
	( \
		echo "Compiling requirements.txt..." && \
		uv pip compile --output-file=requirements.txt requirements.in \
	)

install: build
	( \
		echo "Installing package as editable project..." && \
		uv pip install --upgrade -e ./packages/pycypher && \
		uv pip install --upgrade -e ./packages/nmetl && \
		uv pip install --upgrade -e . \
	)

tests: install
	( \
		echo "Running tests..." && \
		uv run pytest -vv tests/ \
	)

coverage: install
	( \
		echo "Running tests with coverage..." && \
		uv run pytest --cov=./src/pycypher --cov-report=html:coverage_report \
	)

clean:
	( \
		rm -rfv ./venv && \
		rm -fv ./requirements.txt \
		rm -rfv ./dist/* \
		rm -rfv ./coverage_report \
	)

build:
	( \
		echo "Formatting code and building package..." && \
		uv run hatch build -t wheel \
	)

docs: install
	( \
		echo "Building documentation..." && \
		cd docs && \
		uv run make html \
	)

publish: build
	( \
		uv run python ./release.py --increment=$(BUMP) \
	)

data: install
	( \
		echo "Running DVC pipeline..." && \
		uv run dvc repro \
	)

test_env:
	( \
		echo "${DATA_DIR}" && \
		./test_script.sh \
	)

fod_ingest: data
	( \
		echo "Running fod_ingest..." && \
		uv run python packages/fastopendata/src/fastopendata/ingest.py \
	)

.PHONY: clean clean_build tests deps install build docs grammar
